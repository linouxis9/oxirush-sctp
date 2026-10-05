#![cfg(target_os = "linux")]

use oxirush_sctp::*;
use std::os::unix::io::AsRawFd;
use std::time::Duration;

fn set_int(socket: &impl AsRawFd, name: i32, value: i32) {
    // Safety: `value` outlives the call, which only reads it.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            name,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of::<i32>() as libc::socklen_t,
        )
    };
    assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
}

#[tokio::test]
async fn refused_connect_completes_with_association_events() {
    // Reserve a port without listening, so its SCTP stack answers the INIT with an ABORT.
    let bound = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    bound.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let raw = bound.as_raw_fd();
    let mut address: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&address) as libc::socklen_t;
    // Safety: both output buffers outlive the call and have the stated lengths.
    assert_eq!(
        unsafe { libc::getsockname(raw, &mut address as *mut _ as *mut libc::sockaddr, &mut len) },
        0
    );
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], u16::from_be(address.sin_port)));

    for subscribed in [false, true] {
        let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        if subscribed {
            client
                .options()
                .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
                .unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(2), client.connect(address))
            .await
            .expect("connect waited after the association failed");
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECONNREFUSED));
    }
}

#[tokio::test]
async fn successful_connect_keeps_its_association_notification() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addrs(0).unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    let (client, _) = tokio::time::timeout(Duration::from_secs(2), client.connect(address))
        .await
        .unwrap()
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), client.recv())
            .await
            .unwrap()
            .unwrap(),
        NotificationOrData::Notification(Notification::AssociationChange(AssociationChange {
            state: AssocChangeState::CommUp,
            ..
        }))
    ));
}

#[tokio::test]
async fn peer_abort_without_notifications_completes_receive() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addrs(0).unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = client.connect(address).await.unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    client.options().set_linger(Some(Duration::ZERO)).unwrap();
    drop(client);
    let result = tokio::time::timeout(Duration::from_secs(2), peer.recv())
        .await
        .expect("receive waited after the peer aborted the association");
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECONNRESET));
}

#[tokio::test]
async fn positive_linger_is_rejected_without_changing_close_behavior() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addrs(0).unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = client.connect(address).await.unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    peer.options()
        .subscribe_events(
            &[Event::Shutdown, Event::Association],
            SubscribeEventAssocId::All,
        )
        .unwrap();
    for duration in [Duration::from_millis(500), Duration::from_secs(2)] {
        assert_eq!(
            client
                .options()
                .set_linger(Some(duration))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
    drop(client);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), peer.recv())
            .await
            .unwrap()
            .unwrap(),
        NotificationOrData::Notification(Notification::Shutdown(_))
    ));
}

#[tokio::test]
async fn aborted_partial_delivery_does_not_contaminate_another_sender() {
    for rcvinfo in [false, true] {
        let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        set_int(&socket, libc::SO_RCVBUF, 8192);
        socket.options().request_rcvinfo(rcvinfo).unwrap();
        let listener = socket.into_endpoint(2).unwrap();
        listener.options().request_rcvinfo(rcvinfo).unwrap();
        let address = listener.local_addrs(0).unwrap()[0];
        let a = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        set_int(&a, libc::SO_SNDBUF, 1 << 20);
        let (a, _) = a.connect(address).await.unwrap();
        a.send_data(SendData {
            payload: vec![0xaa; 300_000],
            snd_info: None,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), listener.recv())
                .await
                .is_err()
        );
        a.options().set_linger(Some(Duration::ZERO)).unwrap();
        drop(a);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let b = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        let (b, _) = b.connect(address).await.unwrap();
        let b_address = b.local_addrs(0).unwrap()[0];
        b.send_data(SendData {
            payload: b"hello from B".to_vec(),
            snd_info: None,
        })
        .await
        .unwrap();
        match tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .unwrap()
            .unwrap()
        {
            NotificationOrData::Data(data) => {
                assert_eq!(data.payload.len(), b"hello from B".len());
                assert!(data.payload == b"hello from B");
                assert_eq!(data.from, Some(b_address));
                assert_eq!(data.rcv_info.is_some(), rcvinfo);
            }
            other => panic!("not B's message: {:?}", other),
        }
    }
}
