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
    let address = listener.local_addrs().unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    let client = tokio::time::timeout(Duration::from_secs(2), client.connect(address))
        .await
        .unwrap()
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), client.recv())
            .await
            .unwrap()
            .unwrap(),
        Some(NotificationOrData::Notification(
            Notification::AssociationChange(AssociationChange {
                state: AssocChangeState::CommUp,
                ..
            })
        ))
    ));
}

#[tokio::test]
async fn peer_abort_without_notifications_completes_receive() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addrs().unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let client = client.connect(address).await.unwrap();
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
    let address = listener.local_addrs().unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let client = client.connect(address).await.unwrap();
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
        Some(NotificationOrData::Notification(Notification::Shutdown(_)))
    ));
}

#[tokio::test]
async fn aborted_partial_delivery_does_not_contaminate_another_sender() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    set_int(&socket, libc::SO_RCVBUF, 8192);
    let listener = socket.into_endpoint(2).unwrap();
    let address = listener.local_addrs(0).unwrap()[0];
    let a = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    set_int(&a, libc::SO_SNDBUF, 1 << 20);
    let a = a.connect(address).await.unwrap();
    a.send(&[0xaa; 300_000], SendOptions::new()).await.unwrap();
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
    let b = b.connect(address).await.unwrap();
    let b_address = b.local_addrs().unwrap()[0];
    b.send(b"hello from B", SendOptions::new()).await.unwrap();
    match tokio::time::timeout(Duration::from_secs(2), listener.recv())
        .await
        .unwrap()
        .unwrap()
    {
        NotificationOrData::Data(data) => {
            assert!(data.payload == b"hello from B");
            assert_eq!(data.from, Some(b_address));
        }
        other => panic!("not B's message: {other:?}"),
    }
}

/// A message longer than the send buffer: the error says what to make larger, where Linux
/// says "Message too long".
#[tokio::test]
async fn a_message_longer_than_the_send_buffer_says_which_buffer() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client.options().set_send_buffer_size(4096).unwrap();
    let buffer = client.options().send_buffer_size().unwrap();
    let client = client
        .connect(listener.local_addrs().unwrap()[0])
        .await
        .unwrap();
    let error = client
        .send(&vec![0; 4 * buffer], SendOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        format!(
            "a message of {} octets does not fit in the send buffer of {buffer} octets \
             (SO_SNDBUF): `options().set_send_buffer_size` makes it larger, up to \
             `net.core.wmem_max`",
            4 * buffer
        )
    );
}

/// At the limit of the descriptors of the process, a new socket is the error of the kernel.
/// Run alone, in a process of its own (see `a_socket_past_the_limit_of_descriptors_fails`):
/// the limit holds for every thread.
#[test]
#[ignore]
fn at_the_limit_of_descriptors_a_socket_is_an_error() {
    if std::env::var_os("SCTP_RS_TEST_FD_LIMIT").is_none() {
        return;
    }
    // The lowest free descriptor becomes the limit: the next one is past it.
    // Safety: `dup` and `close` take a descriptor of the process, and `setrlimit` a limit.
    unsafe {
        let lowest = libc::dup(0);
        assert!(lowest > 0, "{}", std::io::Error::last_os_error());
        libc::close(lowest);
        let mut limit: libc::rlimit = std::mem::zeroed();
        assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit), 0);
        limit.rlim_cur = lowest as libc::rlim_t;
        assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &limit), 0);
    }
    // It fails before it would be registered with a runtime
    let error = Socket::new_v4(SocketToAssociation::OneToOne).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EMFILE), "{error}");
}

#[test]
fn a_socket_past_the_limit_of_descriptors_fails() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "at_the_limit_of_descriptors_a_socket_is_an_error",
            "--ignored",
        ])
        .env("SCTP_RS_TEST_FD_LIMIT", "1")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "`at_the_limit_of_descriptors_a_socket_is_an_error` failed"
    );
}
