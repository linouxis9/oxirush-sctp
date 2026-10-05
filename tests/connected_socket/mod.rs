use oxirush_sctp::*;

use crate::{create_client_socket, create_socket_bind_and_listen};

#[tokio::test]
async fn bindx_not_supported() {
    // Safety: descriptor 100 is not open, so `from_rawfd` fails without taking it over.
    let connected = unsafe { ConnectedSocket::from_rawfd(100) };
    assert!(connected.is_err(), "{:?}", connected.ok().unwrap());

    // TODO: Write real test
}

#[tokio::test]
async fn connected_default_sendinfo_success() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let result = client_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:?}", result.err().unwrap());

    let result = client_socket.connectx(&[bindaddr]).await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let (connected, assoc_id) = result.unwrap();

    let accept = listener.accept().await;
    assert!(accept.is_ok(), "{:#?}", accept.err().unwrap());

    // Get Peer Address
    let (accepted, _client_addr) = accept.unwrap();

    let sid = 5;
    let ppid = 0x1234;
    let sendinfo = SendInfo {
        sid,
        ppid,
        flags: 0,
        assoc_id: 0,
        context: 0,
    };

    let result = accepted.options().set_default_sendinfo(sendinfo);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let senddata = SendData {
        payload: b"hello world!".to_vec(),
        snd_info: None,
    };
    let result = accepted.send_data(senddata.clone()).await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let result = connected.recv().await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let data = result.unwrap();
    assert!(
        matches!(data, NotificationOrData::Data(ReceivedData { .. })),
        "{:#?}",
        data
    );

    if let NotificationOrData::Data(ReceivedData {
        payload,
        rcv_info,
        nxt_info,
        ..
    }) = data
    {
        assert!(
            payload == b"hello world!".to_vec(),
            "received_payload: {:?}",
            payload,
        );
        assert!(rcv_info.is_some());
        let rcv_info = rcv_info.unwrap();
        assert_eq!(
            rcv_info.assoc_id, assoc_id,
            "rcv_info.assoc_id: {}, assoc_id: {}",
            rcv_info.assoc_id, assoc_id
        );
        assert_eq!(
            rcv_info.sid, sid,
            "rcv_info.sid: {}, sid: {}",
            rcv_info.sid, sid
        );
        assert_eq!(
            rcv_info.ppid, ppid,
            "rcv_info.ppid: {:x}, ppid: {:x}",
            rcv_info.ppid, ppid
        );
        assert!(nxt_info.is_none(), "{:#?}", nxt_info.unwrap());
    } else {
        panic!("Should never come here!: {:#?}", data);
    };
}

#[tokio::test]
async fn connected_send_some_sendinfo_success() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let result = client_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:?}", result.err().unwrap());

    let result = client_socket.connectx(&[bindaddr]).await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let (connected, assoc_id) = result.unwrap();

    let accept = listener.accept().await;
    assert!(accept.is_ok(), "{:#?}", accept.err().unwrap());

    // Get Peer Address
    let (accepted, _client_addr) = accept.unwrap();

    let sid = 5;
    let ppid = 0x1234;
    let snd_info = SendInfo {
        sid,
        ppid,
        flags: 1,
        assoc_id: 0,
        context: 0,
    };

    let senddata = SendData {
        payload: b"hello world!".to_vec(),
        snd_info: Some(snd_info),
    };
    let result = accepted.send_data(senddata).await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let result = connected.recv().await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let data = result.unwrap();
    assert!(
        matches!(data, NotificationOrData::Data(ReceivedData { .. })),
        "{:#?}",
        data
    );

    if let NotificationOrData::Data(ReceivedData {
        payload,
        rcv_info,
        nxt_info,
        ..
    }) = data
    {
        assert!(
            payload == b"hello world!".to_vec(),
            "received_payload: {:?}",
            payload,
        );
        assert!(rcv_info.is_some());
        let rcv_info = rcv_info.unwrap();
        assert_eq!(
            rcv_info.assoc_id, assoc_id,
            "rcv_info.assoc_id: {}, assoc_id: {}",
            rcv_info.assoc_id, assoc_id
        );
        assert_eq!(
            rcv_info.sid, sid,
            "rcv_info.sid: {}, sid: {}, {:#?}",
            rcv_info.sid, sid, rcv_info
        );
        assert_eq!(
            rcv_info.ppid, ppid,
            "rcv_info.ppid: {:x}, ppid: {:x}",
            rcv_info.ppid, ppid
        );
        assert!(nxt_info.is_none(), "{:#?}", nxt_info.unwrap());
    } else {
        panic!("Should never come here!: {:#?}", data);
    };
}
#[tokio::test]
async fn test_shutdown_event() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let result = client_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:?}", result.err().unwrap());

    let result = client_socket.connectx(&[bindaddr]).await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let (connected, client_assoc_id) = result.unwrap();
    let result = connected
        .options()
        .subscribe_events(&[Event::Shutdown], SubscribeEventAssocId::All);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let accept = listener.accept().await;
    assert!(accept.is_ok(), "{:#?}", accept.err().unwrap());
    let (accepted, _) = accept.unwrap();

    // drop the connected socket, so that should generate shutdown event.
    drop(accepted);

    let result = connected.recv().await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let data = result.unwrap();
    assert!(
        matches!(
            data,
            NotificationOrData::Notification(Notification::Shutdown(Shutdown { .. }))
        ),
        "{:#?}",
        data
    );

    if let NotificationOrData::Notification(Notification::Shutdown(Shutdown { assoc_id, .. })) =
        data
    {
        assert_eq!(
            client_assoc_id, assoc_id,
            "client_assoc_id: {}, Event assoc_id: {}",
            client_assoc_id, assoc_id
        );
    } else {
        panic!("Should never come here!: {:#?}", data);
    }
}

#[tokio::test]
async fn a_zero_linger_aborts_the_association_on_close() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let (connected, _) = client_socket.connectx(&[bindaddr]).await.unwrap();
    connected
        .options()
        .subscribe_events(
            &[Event::Association, Event::Shutdown],
            SubscribeEventAssocId::All,
        )
        .unwrap();
    let (accepted, _) = listener.accept().await.unwrap();

    accepted
        .options()
        .set_linger(Some(std::time::Duration::ZERO))
        .unwrap();
    drop(accepted);

    // An ABORT, not a SHUTDOWN: the association is lost at once.
    let data = connected.recv().await.unwrap();
    assert!(
        matches!(
            data,
            NotificationOrData::Notification(Notification::AssociationChange(AssociationChange {
                state: AssocChangeState::CommLost,
                ..
            }))
        ),
        "{:#?}",
        data
    );
}

#[tokio::test]
async fn test_get_status() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let result = client_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:?}", result.err().unwrap());

    let result = client_socket.connectx(&[bindaddr]).await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let (connected, client_assoc_id) = result.unwrap();

    let accept = listener.accept().await;
    assert!(accept.is_ok(), "{:#?}", accept.err().unwrap());
    let (accepted, client_addr) = accept.unwrap();

    let result = connected.status(0);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let status = result.unwrap();
    assert_eq!(status.state, ConnState::Established);
    assert_eq!(
        status.assoc_id, client_assoc_id,
        "client assoc ID: {}, status assoc ID: {}",
        client_assoc_id, status.assoc_id
    );

    let result = accepted.status(0);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
    let status = result.unwrap();
    assert!(matches!(status.state, ConnState::Established));
    let peer: PeerAddress = status.peer_primary;
    assert_eq!(
        client_addr, peer.address,
        "Client Addres: {}, Peer Primary Address: {}",
        client_addr, peer.address
    );
}

#[tokio::test]
async fn futures_are_send() {
    fn assert_send<T: Send>(_: &T) {}

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    assert_send(&listener.accept());
    let endpoint = create_client_socket(SocketToAssociation::OneToMany, true)
        .into_endpoint(2)
        .unwrap();
    assert_send(&endpoint.recv());
    assert_send(&endpoint.send_data(
        Some(bindaddr),
        SendData {
            payload: vec![],
            snd_info: None,
        },
    ));
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    let addrs = [bindaddr];
    let connect = client.connectx(&addrs);
    assert_send(&connect);
    let (connected, _) = connect.await.unwrap();
    assert_send(&connected.recv());
    assert_send(&connected.send_data(SendData {
        payload: vec![],
        snd_info: None,
    }));
}

#[tokio::test]
async fn send_waits_for_room_instead_of_failing_with_would_block() {
    use std::os::unix::io::AsRawFd;
    use std::time::Duration;

    fn set_send_buffer(fd: std::os::unix::io::RawFd) {
        let size: libc::c_int = 4096;
        // Safety: `size` outlives the call, which only reads it.
        let result = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
    }

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    set_send_buffer(client.as_raw_fd());
    let (connected, _) = client.connectx(&[bindaddr]).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();

    let data = SendData {
        payload: vec![0x5a; 1024],
        snd_info: None,
    };
    // The peer does not read: its window and then the send buffer fill up.
    let mut sent = 0;
    loop {
        let send = connected.send_data(data.clone());
        match tokio::time::timeout(Duration::from_millis(200), send).await {
            Ok(Ok(())) => sent += 1,
            Ok(Err(e)) => panic!("send {} failed: {}", sent + 1, e),
            Err(_) => break,
        }
        assert!(sent < 100_000, "the send buffer never filled");
    }

    // As the peer reads, the waiting send completes.
    let send = tokio::time::timeout(Duration::from_secs(5), connected.send_data(data.clone()));
    let read = async {
        for _ in 0..=sent {
            let received = accepted.recv().await.unwrap();
            assert!(matches!(received, NotificationOrData::Data(_)));
        }
    };
    let (sent_last, ()) = tokio::join!(send, read);
    sent_last.expect("the send never completed").unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn send_after_peer_closed_fails_without_sigpipe() {
    // Blocked on this thread, a `SIGPIPE` raised by `sendmsg` stays pending instead of being
    // ignored (Rust programs ignore it) or killing the process (the default).
    // Safety: the signal sets are initialized by `sigemptyset` before use, and the mask is restored
    // before returning.
    unsafe fn sigpipe_set() -> libc::sigset_t {
        let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigemptyset(set.as_mut_ptr());
        libc::sigaddset(set.as_mut_ptr(), libc::SIGPIPE);
        set.assume_init()
    }
    let sigpipe = unsafe { sigpipe_set() };
    let mut old_mask = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &sigpipe, old_mask.as_mut_ptr()) };

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    let (connected, _) = client.connectx(&[bindaddr]).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    drop(accepted);
    // The association has ended once the client reads the end of the stream.
    match connected.recv().await.unwrap() {
        NotificationOrData::Data(data) => assert!(data.payload.is_empty()),
        other => panic!("not the end of the stream: {:?}", other),
    }
    let result = connected
        .send_data(SendData {
            payload: b"too late".to_vec(),
            snd_info: None,
        })
        .await;

    let raised = unsafe {
        let mut pending = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigpending(pending.as_mut_ptr());
        let raised = libc::sigismember(pending.as_ptr(), libc::SIGPIPE) == 1;
        if raised {
            let no_wait = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            libc::sigtimedwait(&sigpipe, std::ptr::null_mut(), &no_wait);
        }
        libc::pthread_sigmask(libc::SIG_SETMASK, old_mask.as_ptr(), std::ptr::null_mut());
        raised
    };
    assert_eq!(
        result.map_err(|e| e.kind()),
        Err(std::io::ErrorKind::BrokenPipe)
    );
    assert!(!raised, "`sctp_send` raised SIGPIPE");
}

/// A connected pair whose receiving end requests `RcvInfo` and `NxtInfo`.
async fn pair_receiving_rcvinfo_and_nxtinfo() -> (ConnectedSocket, ConnectedSocket) {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    client.options().request_rcvinfo(true).unwrap();
    client.options().request_nxtinfo(true).unwrap();
    let (receiving, _) = client.connectx(&[bindaddr]).await.unwrap();
    let (sending, _) = listener.accept().await.unwrap();
    (sending, receiving)
}

async fn send_two_messages(sending: &ConnectedSocket) {
    for (sid, payload) in [(1, &b"first"[..]), (2, &b"second message"[..])] {
        let snd_info = SendInfo {
            sid,
            ppid: 60_u32.to_be(),
            ..Default::default()
        };
        sending
            .send_data(SendData {
                payload: payload.to_vec(),
                snd_info: Some(snd_info),
            })
            .await
            .unwrap();
    }
}

fn assert_first_with_infos(received: NotificationOrData) {
    match received {
        NotificationOrData::Data(data) => {
            assert_eq!(data.payload, b"first");
            let rcv_info = data.rcv_info.expect("no RcvInfo");
            assert_eq!((rcv_info.sid, rcv_info.ppid), (1, 60_u32.to_be()));
            let nxt_info = data.nxt_info.expect("no NxtInfo");
            assert_eq!((nxt_info.sid, nxt_info.length), (2, 14));
        }
        other => panic!("not data: {:?}", other),
    }
}

#[tokio::test]
async fn recv_returns_rcvinfo_and_nxtinfo_together() {
    let (sending, receiving) = pair_receiving_rcvinfo_and_nxtinfo().await;
    // The data I/O event adds an `SCTP_SNDRCV` control message.
    receiving
        .options()
        .subscribe_events(&[Event::DataIo], SubscribeEventAssocId::All)
        .unwrap();
    send_two_messages(&sending).await;
    assert_first_with_infos(receiving.recv().await.unwrap());
}

#[cfg(target_os = "linux")]
#[test]
fn recv_skips_control_messages_of_other_levels() {
    use std::os::unix::io::AsRawFd;

    // Run on another thread: a receive stuck in a loop never yields to a timeout.
    let (received_tx, received_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let received = runtime.block_on(async {
            let (sending, receiving) = pair_receiving_rcvinfo_and_nxtinfo().await;
            // Timestamps come as `SOL_SOCKET` control messages, before the SCTP ones.
            let on: libc::c_int = 1;
            // Safety: `on` outlives the call, which only reads it.
            let result = unsafe {
                libc::setsockopt(
                    receiving.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_TIMESTAMP,
                    &on as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
            send_two_messages(&sending).await;
            receiving.recv().await
        });
        let _ = received_tx.send(received);
    });
    let received = received_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("`sctp_recv` did not return");
    assert_first_with_infos(received.unwrap());
}

fn set_int_option(socket: &impl std::os::unix::io::AsRawFd, level: i32, name: i32, value: i32) {
    // Safety: `value` outlives the call, which only reads it.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of::<i32>() as libc::socklen_t,
        )
    };
    assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
}

fn get_int_option(socket: &impl std::os::unix::io::AsRawFd, level: i32, name: i32) -> i32 {
    let mut value: i32 = 0;
    let mut len = std::mem::size_of::<i32>() as libc::socklen_t;
    // Safety: `value` and `len` outlive the call and `len` is the size of `value`.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            level,
            name,
            &mut value as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
    value
}

/// A connected pair whose sending end can send messages of `max_size` octets. If `small_window`,
/// the receiving end has a small receive buffer, so that longer messages arrive in several parts
/// (partial delivery). `None` if the send buffer cannot be made large enough.
async fn pair_for_long_messages(
    max_size: usize,
    small_window: bool,
) -> Option<(ConnectedSocket, ConnectedSocket)> {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    // Otherwise the whole message fits in the receive window.
    let receive_buffer = if small_window { 8192 } else { max_size };
    set_int_option(
        &listener,
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        receive_buffer as i32,
    );
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    // Linux refuses to send messages longer than the send buffer.
    set_int_option(&client, libc::SOL_SOCKET, libc::SO_SNDBUF, max_size as i32);
    if (get_int_option(&client, libc::SOL_SOCKET, libc::SO_SNDBUF) as usize) < max_size
        || (get_int_option(&listener, libc::SOL_SOCKET, libc::SO_RCVBUF) as usize) < receive_buffer
    {
        eprintln!(
            "`net.core.[rw]mem_max` are too small for {} octets",
            max_size
        );
        return None;
    }
    // Retransmit quickly when the receive window reopens.
    client
        .options()
        .set_rto_info(RtoInfo {
            assoc_id: 0,
            initial: 10,
            max: 10,
            min: 10,
        })
        .unwrap();
    let (sending, _) = client.connectx(&[bindaddr]).await.unwrap();
    let (receiving, _) = listener.accept().await.unwrap();
    Some((sending, receiving))
}

fn long_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn data_payload(received: NotificationOrData) -> Vec<u8> {
    match received {
        NotificationOrData::Data(data) => data.payload,
        other => panic!("not data: {:?}", other),
    }
}

#[tokio::test]
async fn recv_returns_long_messages_whole() {
    let (sending, receiving) = pair_for_long_messages(65536, false).await.unwrap();
    for payload in [
        long_payload(10_000),
        b"short".to_vec(),
        long_payload(60_000),
    ] {
        sending
            .send_data(SendData {
                payload: payload.clone(),
                snd_info: None,
            })
            .await
            .unwrap();
        let received = data_payload(receiving.recv().await.unwrap());
        assert_eq!(received.len(), payload.len());
        assert!(received == payload);
    }
}

#[tokio::test]
async fn recv_returns_partially_delivered_messages_whole() {
    let len = 300_000;
    let Some((sending, receiving)) = pair_for_long_messages(len, true).await else {
        return;
    };
    let payload = long_payload(len);
    let send = sending.send_data(SendData {
        payload: payload.clone(),
        snd_info: None,
    });
    let (sent, received) = tokio::join!(send, receiving.recv());
    sent.unwrap();
    let received = data_payload(received.unwrap());
    assert_eq!(received.len(), payload.len());
    assert!(received == payload);
}

#[tokio::test]
async fn recv_cancelled_in_the_middle_of_a_message_loses_nothing() {
    let len = 300_000;
    let Some((sending, receiving)) = pair_for_long_messages(len, true).await else {
        return;
    };
    let payload = long_payload(len);
    let send = sending.send_data(SendData {
        payload: payload.clone(),
        snd_info: None,
    });
    let receive = async {
        let mut cancelled = 0;
        loop {
            let recv = receiving.recv();
            match tokio::time::timeout(std::time::Duration::from_millis(1), recv).await {
                Ok(received) => return (received, cancelled),
                Err(_) => cancelled += 1,
            }
        }
    };
    let (sent, (received, cancelled)) = tokio::join!(send, receive);
    sent.unwrap();
    assert!(cancelled > 0);
    let received = data_payload(received.unwrap());
    assert_eq!(received.len(), payload.len());
    assert!(received == payload);
}

#[tokio::test]
async fn recv_rejects_messages_longer_than_4_mib() {
    let max = 4 << 20;
    let Some((sending, receiving)) = pair_for_long_messages(max + 1, false).await else {
        return;
    };
    for len in [max + 1, max] {
        let payload = long_payload(len);
        let send = sending.send_data(SendData {
            payload: payload.clone(),
            snd_info: None,
        });
        let ((), received) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            // A failed send must stop the transfer: there will be no message to receive.
            // Keep receive errors as values, since an oversized message should be rejected.
            tokio::try_join!(send, async {
                Ok::<_, std::io::Error>(receiving.recv().await)
            })
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "send/receive of {} octets did not complete within 10 seconds; sender: {:?}; receiver: {:?}",
                len,
                sending.status(0),
                receiving.status(0),
            )
        })
        .unwrap();
        match received {
            Err(error) if len > max => {
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{}", error)
            }
            Ok(received) if len <= max => assert!(data_payload(received) == payload),
            Ok(NotificationOrData::Data(data)) => {
                panic!("{} octets received, of {}", data.payload.len(), len)
            }
            other => panic!("{:?}", other.map(|_| ())),
        }
    }
}

#[tokio::test]
async fn recv_limit_is_set_per_socket_and_inherited_on_accept() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    assert_eq!(listener.options().max_message_size(), 4 << 20);
    listener.options().set_max_message_size(1000);
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    let (sending, _) = client.connectx(&[bindaddr]).await.unwrap();
    let (receiving, _) = listener.accept().await.unwrap();
    assert_eq!(receiving.options().max_message_size(), 1000);
    for len in [1001, 1000] {
        sending
            .send_data(SendData {
                payload: long_payload(len),
                snd_info: None,
            })
            .await
            .unwrap();
        match receiving.recv().await {
            Err(error) if len > 1000 => {
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{}", error)
            }
            Ok(received) if len == 1000 => assert!(data_payload(received) == long_payload(len)),
            other => panic!("{} octets: {:?}", len, other.map(|_| ())),
        }
    }
}
