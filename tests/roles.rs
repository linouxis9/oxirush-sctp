//! Socket role transitions and the shared configuration view.
use oxirush_sctp::{
    BindxFlags, ConnectedSocket, NotificationOrData, SendOptions, Socket, SocketToAssociation,
};
use std::os::fd::AsRawFd;

#[tokio::test]
async fn one_to_many_socket_cannot_become_an_accepting_listener() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    match socket.listen(4) {
        Ok(_) => panic!("a one-to-many socket became an accepting listener"),
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(
                error.to_string(),
                "`listen` is for a one-to-one socket, and this one is one-to-many: \
                 `Socket::into_endpoint` makes it an endpoint, whose `connect` starts an \
                 association and which receives those of its peers"
            );
        }
    }
}

#[tokio::test]
async fn one_to_many_socket_cannot_become_a_single_association() {
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let server = server.listen(4).unwrap();
    let address = server.local_addrs().unwrap()[0];
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    match socket.connect(address).await {
        Ok(_) => panic!("a one-to-many socket became a single association"),
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                error.to_string().starts_with(
                    "`connect` is for a one-to-one socket, and this one is one-to-many: "
                ),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn one_to_one_socket_cannot_become_a_multi_association_endpoint() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let error = socket.into_endpoint(4).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "`into_endpoint` is for a one-to-many socket, and this one is one-to-one: \
         `Socket::listen` accepts associations on it, and `Socket::connect` starts one"
    );
}

/// Peeling off an association that the endpoint does not have says so, where Linux says
/// `EINVAL`.
#[tokio::test]
async fn peeling_off_an_association_that_the_endpoint_has_not_says_so() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(4).unwrap();
    let error = endpoint.peeloff(77).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(
        error.to_string(),
        "the endpoint has no association 77 to peel off"
    );
}

#[tokio::test]
async fn configuration_and_registration_survive_listen_connect_and_accept() {
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    server.options().set_max_message_size(2048);
    server.options().set_nodelay(true).unwrap();
    let original = server.as_raw_fd();
    let listener = server.listen(4).unwrap();
    assert_eq!(listener.as_raw_fd(), original);
    assert_eq!(listener.options().max_message_size(), 2048);
    assert!(listener.options().nodelay().unwrap());
    let address = listener.local_addrs().unwrap()[0];
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client.options().set_max_message_size(1024);
    client.options().set_nodelay(true).unwrap();
    let original = client.as_raw_fd();
    let (client, _) = client.connect(address).await.unwrap();
    assert_eq!(client.as_raw_fd(), original);
    assert_eq!(client.options().max_message_size(), 1024);
    assert!(client.options().nodelay().unwrap());
    let (peer, _) = listener.accept().await.unwrap();
    assert_eq!(peer.options().max_message_size(), 2048);
    assert!(peer.options().nodelay().unwrap());
    listener.options().set_max_message_size(512);
    assert_eq!(peer.options().max_message_size(), 2048);
}

#[tokio::test]
async fn local_addr_reports_the_bound_address_in_every_role() {
    let unbound = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    assert_eq!(unbound.local_addr().unwrap(), "0.0.0.0:0".parse().unwrap());
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let addresses = [
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.2:0".parse().unwrap(),
    ];
    server.bindx(&addresses, BindxFlags::Add).unwrap();
    let address = server.local_addr().unwrap();
    assert!(addresses.iter().any(|bound| bound.ip() == address.ip()));
    assert_ne!(address.port(), 0);
    let listener = server.listen(4).unwrap();
    assert_eq!(listener.local_addr().unwrap(), address);
    let (client, _) = unbound.connect(address).await.unwrap();
    let (peer, client_address) = listener.accept().await.unwrap();
    assert_eq!(peer.local_addr().unwrap().port(), address.port());
    assert_eq!(client.local_addr().unwrap(), client_address);

    let socket = Socket::new_v6(SocketToAssociation::OneToMany).unwrap();
    socket.bind("[::1]:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(4).unwrap();
    assert_eq!(
        endpoint.local_addr().unwrap(),
        endpoint.local_addrs(0).unwrap()[0]
    );
}

#[tokio::test]
async fn connected_and_accepted_sockets_report_stream_and_ppid_by_default() {
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = server.listen(4).unwrap();
    let (client, assoc_id) = Socket::new_v4(SocketToAssociation::OneToOne)
        .unwrap()
        .connect(listener.local_addrs().unwrap()[0])
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let options = SendOptions::new().stream(3).ppid(60);
    client.send(b"request", options).await.unwrap();
    peer.send(b"answer", options).await.unwrap();
    let NotificationOrData::Data(data) = peer.recv().await.unwrap().unwrap() else {
        panic!("not data")
    };
    assert_eq!((data.stream_id(), data.ppid()), (Some(3), Some(60)));
    let NotificationOrData::Data(data) = client.recv().await.unwrap().unwrap() else {
        panic!("not data")
    };
    assert_eq!((data.stream_id(), data.ppid()), (Some(3), Some(60)));
    assert_eq!(data.assoc_id(), Some(assoc_id));
}

#[tokio::test]
async fn endpoint_defaults_identify_associations_and_peeloff_preserves_configuration() {
    let socket = Socket::new_v6(SocketToAssociation::OneToMany).unwrap();
    socket.bind("[::1]:0".parse().unwrap()).unwrap();
    socket.options().set_max_message_size(100);
    socket.options().set_nodelay(true).unwrap();
    let original = socket.as_raw_fd();
    let endpoint = socket.into_endpoint(4).unwrap();
    assert_eq!(endpoint.as_raw_fd(), original);
    assert_eq!(endpoint.options().max_message_size(), 100);
    let address = endpoint.local_addrs(0).unwrap()[0];
    let (client, _) = Socket::new_v6(SocketToAssociation::OneToOne)
        .unwrap()
        .connect(address)
        .await
        .unwrap();
    client.send(b"identify", SendOptions::new()).await.unwrap();
    let NotificationOrData::Data(data) = endpoint.recv().await.unwrap() else {
        panic!("not data")
    };
    let assoc_id = data.rcv_info.unwrap().assoc_id;
    assert_ne!(assoc_id, 0);
    let peer = endpoint.peeloff(assoc_id).unwrap();
    assert_eq!(peer.options().max_message_size(), 100);
    assert!(peer.options().nodelay().unwrap());
    client.send(&[0xaa; 101], SendOptions::new()).await.unwrap();
    client.send(b"next", SendOptions::new()).await.unwrap();
    assert_eq!(
        peer.recv().await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    let NotificationOrData::Data(data) = peer.recv().await.unwrap().unwrap() else {
        panic!("not data")
    };
    assert_eq!(data.payload, b"next");
}

#[tokio::test]
async fn raw_stream_import_transfers_ownership_of_an_established_association() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(2).unwrap();
    let (client, _) = Socket::new_v4(SocketToAssociation::OneToOne)
        .unwrap()
        .connect(listener.local_addrs().unwrap()[0])
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    // Safety: dup returns a new, independently owned descriptor with inherited O_NONBLOCK.
    let owned = unsafe { libc::dup(client.as_raw_fd()) };
    assert!(owned >= 0);
    drop(client);
    // Safety: this is the sole owned established SCTP descriptor, and remains nonblocking.
    let imported = unsafe { ConnectedSocket::from_rawfd(owned) }.unwrap();
    imported
        .send(b"imported", SendOptions::new())
        .await
        .unwrap();
    let NotificationOrData::Data(data) = peer.recv().await.unwrap().unwrap() else {
        panic!("not data")
    };
    assert_eq!(data.payload, b"imported");
    drop(imported);
    let closed = tokio::time::timeout(std::time::Duration::from_secs(2), peer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed, None, "imported descriptor remains open");
}

/// An endpoint with a receive buffer of `buffer` octets, and two clients: `slow`, whose send
/// buffer holds `long` octets, and `quick`. `None` where `net.core.wmem_max` is too low.
async fn endpoint_with_two_clients(
    buffer: usize,
    long: usize,
) -> Option<(
    oxirush_sctp::OneToManyEndpoint,
    ConnectedSocket,
    ConnectedSocket,
)> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket.options().set_recv_buffer_size(buffer).unwrap();
    let endpoint = socket.into_endpoint(4).unwrap();
    let address = endpoint.local_addr().unwrap();

    let slow = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    slow.options().set_send_buffer_size(2 * long).unwrap();
    if slow.options().send_buffer_size().unwrap() < 2 * long {
        eprintln!("`net.core.wmem_max` is below {} octets; skipping", 2 * long);
        return None;
    }
    let (slow, _) = slow.connect(address).await.unwrap();
    let quick = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (quick, _) = quick.connect(address).await.unwrap();
    Some((endpoint, slow, quick))
}

/// The endpoint reads from the start; `slow` sends a long message, and `quick`, 300 ms later,
/// a short one. The lengths of the two messages in the order they came, with the time that
/// the short one took.
async fn long_and_short(
    endpoint: oxirush_sctp::OneToManyEndpoint,
    slow: ConnectedSocket,
    quick: ConnectedSocket,
    long: usize,
) -> (Vec<usize>, std::time::Duration) {
    let sent = std::sync::Arc::new(std::sync::Mutex::new(None::<std::time::Instant>));
    let reader = tokio::spawn({
        let sent = sent.clone();
        async move {
            let mut lengths = Vec::new();
            let mut short = std::time::Duration::ZERO;
            while lengths.len() < 2 {
                let next = endpoint.recv();
                let received = tokio::time::timeout(std::time::Duration::from_secs(20), next)
                    .await
                    .expect("every message arrives")
                    .unwrap();
                if let NotificationOrData::Data(message) = received {
                    if message.payload.len() == 15
                        && let Some(sent) = *sent.lock().unwrap()
                    {
                        short = sent.elapsed();
                    }
                    lengths.push(message.payload.len());
                }
            }
            (lengths, short)
        }
    });
    let sending = tokio::spawn(async move {
        slow.send(&vec![0x5a; long], SendOptions::new())
            .await
            .unwrap();
        slow
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    *sent.lock().unwrap() = Some(std::time::Instant::now());
    quick
        .send(b"a short message", SendOptions::new())
        .await
        .unwrap();
    let result = reader.await.unwrap();
    drop(sending.await.unwrap());
    result
}

/// On a one-to-many endpoint, the parts of the messages of several associations come in
/// turns, and each is assembled apart: a message that arrives in many parts holds back none
/// of the other associations. With Linux's default (`SCTP_FRAGMENT_INTERLEAVE` 0) and one
/// assembler, the short message waited for the long one.
#[tokio::test]
async fn a_long_message_of_one_association_holds_back_none_of_the_others_on_an_endpoint() {
    const LONG: usize = 512 << 10;
    let Some((endpoint, slow, quick)) = endpoint_with_two_clients(BUFFER, LONG).await else {
        return;
    };
    let (lengths, short) = long_and_short(endpoint, slow, quick, LONG).await;
    eprintln!("{lengths:?}, the short message in {short:?}");
    assert_eq!(
        lengths,
        [15, LONG],
        "the short message waited for the long one"
    );
    assert!(
        short < std::time::Duration::from_millis(500),
        "the short message took {short:?}"
    );
}

const BUFFER: usize = 64 << 10;

/// Without the receive information, an endpoint cannot tell the parts of messages of two
/// associations apart: they come one message after the other, as Linux has it by default,
/// and a long one holds back the others. A `Listener` gives each association a socket.
#[tokio::test]
async fn without_the_receive_information_a_long_message_holds_back_the_others() {
    const LONG: usize = 512 << 10;
    let Some((endpoint, slow, quick)) = endpoint_with_two_clients(BUFFER, LONG).await else {
        return;
    };
    endpoint.options().request_rcvinfo(false).unwrap();
    let (lengths, short) = long_and_short(endpoint, slow, quick, LONG).await;
    eprintln!("{lengths:?}, the short message in {short:?}");
    assert_eq!(lengths, [LONG, 15]);
}

/// A peer that aborts in the middle of a long message leaves nothing of it to the endpoint:
/// the parts go with the notification that the kernel gave up their delivery, which the
/// endpoint subscribes to for itself and does not pass on, and the other associations go on.
#[tokio::test]
async fn a_peer_that_aborts_in_the_middle_of_a_message_leaves_the_others_alone() {
    const LONG: usize = 1 << 20;
    let Some((endpoint, slow, quick)) = endpoint_with_two_clients(8 << 10, LONG).await else {
        return;
    };
    endpoint
        .options()
        .subscribe_events(
            &[oxirush_sctp::Event::Association],
            oxirush_sctp::SubscribeEventAssocId::All,
        )
        .unwrap();
    let slow = std::sync::Arc::new(slow);
    let sending = tokio::spawn({
        let slow = slow.clone();
        async move { slow.send(&vec![0x5a; LONG], SendOptions::new()).await }
    });
    // The endpoint reads, and has the first parts of the long message, when its peer aborts.
    let receiving = tokio::spawn(async move {
        let mut received = Vec::new();
        loop {
            let next = tokio::time::timeout(std::time::Duration::from_secs(5), endpoint.recv())
                .await
                .expect("the endpoint hears of the abort and of the next message")
                .unwrap();
            let done =
                matches!(&next, NotificationOrData::Data(message) if message.payload == b"after");
            received.push(next);
            if done {
                return received;
            }
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    slow.options()
        .set_linger(Some(std::time::Duration::ZERO))
        .unwrap();
    sending.abort();
    let _ = sending.await;
    drop(std::sync::Arc::into_inner(slow).expect("the only owner"));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    quick.send(b"after", SendOptions::new()).await.unwrap();
    let received = receiving.await.unwrap();
    use oxirush_sctp::{AssocChangeState, Notification};
    let lost = received.iter().filter(|received| {
        matches!(
            received,
            NotificationOrData::Notification(Notification::AssociationChange(change))
                if change.state == AssocChangeState::CommLost
        )
    });
    assert_eq!(lost.count(), 1, "{received:?}");
    // Nothing of the long message, and no notification that was not subscribed.
    for received in &received {
        match received {
            NotificationOrData::Data(message) => assert_eq!(message.payload, b"after"),
            NotificationOrData::Notification(Notification::AssociationChange(_)) => {}
            other => panic!("{other:?}"),
        }
    }
}

/// Peers together cannot make an endpoint hold more than its limit of messages in assembly:
/// past it, the endpoint aborts the association whose message is the longest, says so once,
/// and drops what comes of that message. The other associations go on.
#[tokio::test]
async fn past_its_limit_of_messages_in_assembly_an_endpoint_aborts_the_longest() {
    const LONG: usize = 1 << 20;
    let Some((endpoint, slow, quick)) = endpoint_with_two_clients(8 << 10, LONG).await else {
        return;
    };
    endpoint.options().set_max_assembly(64 << 10);
    assert_eq!(endpoint.options().max_assembly(), 64 << 10);
    let receiving = tokio::spawn(async move {
        let mut received = Vec::new();
        loop {
            let next = tokio::time::timeout(std::time::Duration::from_secs(5), endpoint.recv())
                .await
                .expect("the endpoint says that it aborted, and gets the next message");
            let done = matches!(
                &next,
                Ok(NotificationOrData::Data(message)) if message.payload == b"after"
            );
            received.push(next.map_err(|error| (error.kind(), error.to_string())));
            if done {
                return received;
            }
        }
    });
    slow.send(&vec![0x5a; LONG], SendOptions::new())
        .await
        .unwrap();
    // The peer whose association was aborted learns of it.
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), slow.recv())
        .await
        .expect("the abort reaches the peer");
    assert_eq!(
        ended.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionReset
    );
    quick.send(b"after", SendOptions::new()).await.unwrap();
    let received = receiving.await.unwrap();
    let [Err((kind, text)), Ok(NotificationOrData::Data(after))] = &received[..] else {
        panic!("{received:?}");
    };
    assert_eq!(*kind, std::io::ErrorKind::InvalidData);
    assert!(text.starts_with("aborted association "), "{text}");
    assert_eq!(after.payload, b"after");
}
