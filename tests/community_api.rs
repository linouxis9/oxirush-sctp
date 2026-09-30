#![cfg(target_os = "linux")]

use oxirush_sctp::*;
use std::convert::TryInto;
use std::os::fd::AsRawFd;

fn event_enabled(socket: &impl AsRawFd, event: Event) -> bool {
    // Linux struct sctp_event: association, event type, enabled byte, padding.
    let mut value = [0_u8; 8];
    value[..4].copy_from_slice(&0_i32.to_ne_bytes());
    value[4..6].copy_from_slice(&(event as u16).to_ne_bytes());
    let mut len = value.len() as libc::socklen_t;
    // Safety: the two output buffers are valid for the stated lengths.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_SCTP,
            127,
            value.as_mut_ptr().cast(),
            &mut len,
        )
    };
    assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
    value[6] != 0
}

#[tokio::test]
async fn subscription_failure_retains_kernel_source_and_attempts_later_events() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    assert!(!event_enabled(&socket, Event::Association));
    let error = socket
        .sctp_subscribe_events(
            &[Event::Unknown, Event::Association],
            SubscribeEventAssocId::All,
        )
        .unwrap_err();
    assert!(event_enabled(&socket, Event::Association));
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert!(
        error.get_ref().unwrap().source().is_some(),
        "kernel error was replaced by text"
    );
    let failures = error
        .get_ref()
        .unwrap()
        .downcast_ref::<EventSubscriptionError>()
        .unwrap()
        .failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, Event::Unknown);
    assert_eq!(failures[0].1.raw_os_error(), Some(libc::EINVAL));
    let error = socket
        .sctp_unsubscribe_events(
            &[Event::Unknown, Event::Association],
            SubscribeEventAssocId::All,
        )
        .unwrap_err();
    assert!(!event_enabled(&socket, Event::Association));
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<EventSubscriptionError>()
            .unwrap()
            .failures()[0]
            .1
            .raw_os_error(),
        Some(libc::EINVAL)
    );
}

fn server() -> Listener {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket.sctp_request_rcvinfo(true).unwrap();
    socket.listen(2).unwrap()
}

async fn expect_message(receiver: &ConnectedSocket, expected: &[u8], ppid: u32) {
    let NotificationOrData::Data(data) =
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver.sctp_recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("not data");
    };
    assert_eq!(data.payload, expected);
    let info = data.rcv_info.unwrap();
    assert_eq!(u32::from_be(info.ppid), ppid);
    assert_eq!(info.sid, 2);
    assert_eq!(info.flags & 1, 1);
}

#[tokio::test]
async fn borrowed_send_converts_host_ppid_without_changing_legacy_wire_order() {
    let server = server();
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = socket
        .connect(server.sctp_getladdrs(0).unwrap()[0])
        .await
        .unwrap();
    let (peer, _) = server.accept().await.unwrap();
    let payload = b"borrowed payload";
    client
        .send(
            payload,
            SendOptions {
                stream_id: 2,
                ppid: 60,
                unordered: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    expect_message(&peer, payload, 60).await;
    // Existing SendInfo continues to accept an already-converted wire-order PPID.
    client
        .sctp_send(SendData {
            payload: payload.to_vec(),
            snd_info: Some(SendInfo {
                sid: 2,
                ppid: 18_u32.to_be(),
                flags: 1,
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    expect_message(&peer, payload, 18).await;
}

#[tokio::test]
async fn one_to_many_connected_endpoint_can_connect_another_peer() {
    let a = server();
    let b = server();
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    let (client, assoc_a) = socket
        .connect(a.sctp_getladdrs(0).unwrap()[0])
        .await
        .unwrap();
    let (peer_a, _) = a.accept().await.unwrap();
    let assoc_b = client
        .sctp_connectx_association(&[b.sctp_getladdrs(0).unwrap()[0]])
        .unwrap();
    let (peer_b, _) = tokio::time::timeout(std::time::Duration::from_secs(2), b.accept())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(assoc_a, assoc_b);
    assert_eq!(
        client.sctp_connectx_association(&[]).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    for (assoc_id, payload, peer) in [
        (assoc_a, b"peer A".as_slice(), &peer_a),
        (assoc_b, b"peer B".as_slice(), &peer_b),
    ] {
        client
            .send(
                payload,
                SendOptions {
                    stream_id: 2,
                    ppid: 60,
                    unordered: true,
                    assoc_id,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        expect_message(peer, payload, 60).await;
    }
    drop(peer_a);
    client
        .send(
            b"still B",
            SendOptions {
                stream_id: 2,
                ppid: 60,
                unordered: true,
                assoc_id: assoc_b,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    expect_message(&peer_b, b"still B", 60).await;
    let c = server();
    let assoc_c = client
        .sctp_connectx_association(&[c.sctp_getladdrs(0).unwrap()[0]])
        .unwrap();
    let (peer_c, _) = tokio::time::timeout(std::time::Duration::from_secs(2), c.accept())
        .await
        .unwrap()
        .unwrap();
    client
        .send(
            b"new C",
            SendOptions {
                stream_id: 2,
                ppid: 60,
                unordered: true,
                assoc_id: assoc_c,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    expect_message(&peer_c, b"new C", 60).await;
}

#[tokio::test]
async fn one_to_many_listener_can_initiate_multiple_outgoing_associations() {
    let a = server();
    let b = server();
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let client = socket.listen(2).unwrap();
    let addr_a = a.sctp_getladdrs(0).unwrap()[0];
    let addr_b = b.sctp_getladdrs(0).unwrap()[0];
    let assoc_a = client.sctp_connectx_association(&[addr_a]).unwrap();
    let assoc_b = client.sctp_connectx_association(&[addr_b]).unwrap();
    assert_ne!(assoc_a, assoc_b);
    let (peer_a, _) = tokio::time::timeout(std::time::Duration::from_secs(2), a.accept())
        .await
        .unwrap()
        .unwrap();
    let (peer_b, _) = tokio::time::timeout(std::time::Duration::from_secs(2), b.accept())
        .await
        .unwrap()
        .unwrap();
    for (address, assoc_id, payload, peer) in [
        (addr_a, assoc_a, b"peer A".as_slice(), &peer_a),
        (addr_b, assoc_b, b"peer B".as_slice(), &peer_b),
    ] {
        client
            .send(
                address,
                payload,
                SendOptions {
                    stream_id: 2,
                    ppid: 18,
                    unordered: true,
                    assoc_id,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        expect_message(peer, payload, 18).await;
    }
    assert_eq!(
        a.sctp_connectx_association(&[addr_b])
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EOPNOTSUPP)
    );
}

#[tokio::test]
async fn heartbeat_and_path_settings_roundtrip_on_defaults_and_established_paths() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let defaults: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let mut params = socket.peer_address_params(0, defaults).unwrap();
    params.heartbeat_interval = std::time::Duration::from_millis(1500);
    params.heartbeat_enabled = true;
    params.path_max_retrans = 3;
    socket.set_peer_address_params(params.clone()).unwrap();
    assert_eq!(socket.peer_address_params(0, defaults).unwrap(), params);
    params.heartbeat_enabled = false;
    socket.set_peer_address_params(params.clone()).unwrap();
    assert_eq!(socket.peer_address_params(0, defaults).unwrap(), params);
    let server = server();
    let address = server.sctp_getladdrs(0).unwrap()[0];
    let (client, _) = socket.connect(address).await.unwrap();
    let (_peer, _) = server.accept().await.unwrap();
    let mut params = client.peer_address_params(0, address).unwrap();
    assert!(!params.heartbeat_enabled);
    assert_eq!(params.path_max_retrans, 3);
    params.heartbeat_enabled = true;
    params.heartbeat_interval = std::time::Duration::from_millis(1000);
    client.set_peer_address_params(params.clone()).unwrap();
    assert_eq!(client.peer_address_params(0, address).unwrap(), params);
    client.request_heartbeat(0, address).unwrap();
    params.heartbeat_interval = std::time::Duration::from_nanos(1);
    assert_eq!(
        client.set_peer_address_params(params).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn send_failure_notification_preserves_payload_and_metadata() {
    // A bound, non-listening SCTP port answers INIT with ABORT instead of silently timing out.
    let refused = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    refused.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let rawfd = refused.as_raw_fd();
    let mut address: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&address) as libc::socklen_t;
    // Safety: address and len are initialized output buffers valid during the call.
    assert_eq!(
        unsafe {
            libc::getsockname(
                rawfd,
                &mut address as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        },
        0
    );
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], u16::from_be(address.sin_port)));
    let client = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    client
        .sctp_subscribe_events(&[Event::SendFailureEvent], SubscribeEventAssocId::All)
        .unwrap();
    let client = client.listen(1).unwrap();
    client
        .send(
            address,
            b"undelivered",
            SendOptions {
                stream_id: 2,
                ppid: 60,
                context: 42,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let NotificationOrData::Notification(Notification::SendFailure(failure)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), client.sctp_recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("not a send failure");
    };
    assert_eq!(failure.payload, b"undelivered");
    assert_eq!(failure.snd_info.sid, 2);
    assert_eq!(u32::from_be(failure.snd_info.ppid), 60);
    assert_eq!(failure.snd_info.context, 42);
    assert_eq!(failure.flags, 0, "the message was never transmitted");
}

async fn read_fifty(socket: std::sync::Arc<ConnectedSocket>) -> Vec<u32> {
    let mut ids = Vec::new();
    for _ in 0..50 {
        let NotificationOrData::Data(data) = socket.sctp_recv().await.unwrap() else {
            panic!("unexpected notification");
        };
        let id = u32::from_be_bytes(data.payload[..4].try_into().unwrap());
        assert_eq!(data.payload.len(), if id % 2 == 0 { 9000 } else { 300 });
        assert!(data.payload[4..].iter().all(|&byte| byte == id as u8));
        assert_eq!(data.rcv_info.unwrap().sid, (id % 4) as u16);
        ids.push(id);
    }
    ids
}

#[tokio::test]
async fn concurrent_receivers_preserve_record_boundaries_and_streams() {
    let server = server();
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.set_nodelay(true).unwrap();
    let (sender, _) = socket
        .connect(server.sctp_getladdrs(0).unwrap()[0])
        .await
        .unwrap();
    let (receiver, _) = server.accept().await.unwrap();
    let receiver = std::sync::Arc::new(receiver);
    let check = async {
        let writer = tokio::spawn(async move {
            for id in 0_u32..100 {
                let mut bytes = vec![id as u8; if id % 2 == 0 { 9000 } else { 300 }];
                bytes[..4].copy_from_slice(&id.to_be_bytes());
                sender
                    .send(
                        &bytes,
                        SendOptions {
                            stream_id: (id % 4) as u16,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
        });
        let a = tokio::spawn(read_fifty(receiver.clone()));
        let b = tokio::spawn(read_fifty(receiver));
        writer.await.unwrap();
        let mut ids = a.await.unwrap();
        ids.extend(b.await.unwrap());
        ids.sort_unstable();
        assert_eq!(ids, (0_u32..100).collect::<Vec<_>>());
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), check)
        .await
        .unwrap();
}

#[tokio::test]
async fn ipv6_borrowed_send_and_path_controls_preserve_native_addresses() {
    let server = Socket::new_v6(SocketToAssociation::OneToOne).unwrap();
    server.bind("[::1]:0".parse().unwrap()).unwrap();
    server.sctp_request_rcvinfo(true).unwrap();
    let server = server.listen(1).unwrap();
    let address = server.sctp_getladdrs(0).unwrap()[0];
    assert!(address.is_ipv6());
    let socket = Socket::new_v6(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = socket.connect(address).await.unwrap();
    let (peer, accepted_address) = server.accept().await.unwrap();
    assert!(accepted_address.is_ipv6());
    let mut params = client.peer_address_params(0, address).unwrap();
    params.heartbeat_enabled = true;
    params.heartbeat_interval = std::time::Duration::from_millis(1000);
    params.path_max_retrans = 3;
    client.set_peer_address_params(params.clone()).unwrap();
    assert_eq!(client.peer_address_params(0, address).unwrap(), params);
    client.request_heartbeat(0, address).unwrap();
    client
        .send(
            b"ipv6",
            SendOptions {
                stream_id: 2,
                ppid: 60,
                unordered: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    expect_message(&peer, b"ipv6", 60).await;
}

#[tokio::test]
async fn connected_bindx_adds_and_removes_local_addresses() {
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let server = server.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client.bind("127.0.0.2:0".parse().unwrap()).unwrap();
    let (client, _) = client
        .connect(server.sctp_getladdrs(0).unwrap()[0])
        .await
        .unwrap();
    let (_peer, _) = server.accept().await.unwrap();
    let port = client.sctp_getladdrs(0).unwrap()[0].port();
    let added = std::net::SocketAddr::from(([127, 0, 0, 3], port));
    client.sctp_bindx(&[added], BindxFlags::Add).unwrap();
    assert!(client.sctp_getladdrs(0).unwrap().contains(&added));
    client.sctp_bindx(&[added], BindxFlags::Remove).unwrap();
    assert!(!client.sctp_getladdrs(0).unwrap().contains(&added));
}
