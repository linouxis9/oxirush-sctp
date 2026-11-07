#![cfg(target_os = "linux")]
//! What a core network team sets on its associations: the retransmission limit, the DSCP,
//! the primary path, the buffers and the delay of acknowledgements; an abort, the typed
//! notifications, and a receive into the buffer of the caller.

use oxirush_sctp::*;
use std::time::Duration;

async fn pair() -> (ConnectedSocket, ConnectedSocket) {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = client
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (client, peer)
}

#[tokio::test]
async fn the_options_of_an_association_are_set_and_read_back() {
    for v4 in [true, false] {
        let socket = if v4 {
            Socket::new_v4(SocketToAssociation::OneToOne).unwrap()
        } else {
            Socket::new_v6(SocketToAssociation::OneToOne).unwrap()
        };
        let options = socket.options();

        // Expedited forwarding
        options.set_dscp(46).unwrap();
        assert_eq!(options.dscp().unwrap(), 46);
        let error = options.set_dscp(64).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        // Linux gives twice what is asked, for its own bookkeeping
        options.set_send_buffer_size(64 * 1024).unwrap();
        assert_eq!(options.send_buffer_size().unwrap(), 128 * 1024);
        options.set_recv_buffer_size(64 * 1024).unwrap();
        assert_eq!(options.recv_buffer_size().unwrap(), 128 * 1024);

        let mut sack = SackInfo::default();
        (sack.delay, sack.frequency) = (50, 2);
        options.set_sack_info(sack).unwrap();
        let sack = options.sack_info(0).unwrap();
        assert_eq!((sack.delay, sack.frequency), (50, 2));

        let before = options.association_params(0).unwrap();
        assert!(before.max_retransmissions > 0 && before.cookie_life > 0);
        let params = AssociationParams {
            max_retransmissions: 4,
            ..Default::default()
        };
        options.set_association_params(params).unwrap();
        let after = options.association_params(0).unwrap();
        assert_eq!(after.max_retransmissions, 4);
        // A zero left the life of the cookie as it was
        assert_eq!(after.cookie_life, before.cookie_life);
    }
}

#[tokio::test]
async fn the_primary_path_is_one_of_the_addresses_of_the_peer() {
    let (client, _peer) = pair().await;
    let address = client.peer_addrs().unwrap()[0];
    client.options().set_primary_address(0, address).unwrap();
    assert_eq!(client.status().unwrap().peer_primary.address, address);
    // An address that the peer does not have
    let error = client
        .options()
        .set_primary_address(0, "192.0.2.1:9".parse().unwrap())
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
}

#[tokio::test]
async fn an_abort_ends_the_association_for_the_peer_at_once() {
    let (client, peer) = pair().await;
    peer.options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    client.abort(b"going away").unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), peer.recv())
        .await
        .unwrap();
    assert!(
        matches!(
            &received,
            Ok(Some(NotificationOrData::Notification(Notification::AssociationChange(change))))
                if change.state == AssocChangeState::CommLost
        ),
        "{received:?}"
    );
    let error = peer.recv().await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
}

/// On an endpoint, and so on a socket that was peeled off one, the reason of an abort
/// reaches the peer in the notification of the loss.
#[tokio::test]
async fn an_abort_of_an_endpoint_carries_its_reason() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    let (client, _) = client
        .connect(endpoint.local_addr().unwrap())
        .await
        .unwrap();
    client.send(b"hello", SendOptions::new()).await.unwrap();
    let NotificationOrData::Data(message) = endpoint.recv().await.unwrap() else {
        panic!("not a message");
    };
    endpoint
        .abort(message.assoc_id().unwrap(), b"going away")
        .unwrap();
    let lost = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match client.recv().await {
                Ok(Some(NotificationOrData::Notification(Notification::AssociationChange(
                    change,
                )))) if change.state == AssocChangeState::CommLost => return change,
                Ok(Some(_)) => {}
                other => panic!("{other:?}"),
            }
        }
    })
    .await
    .unwrap();
    // A user-initiated abort, with its reason after the four octets of the cause
    assert_eq!(lost.cause, 12);
    assert!(lost.info.ends_with(b"going away"), "{:?}", lost.info);
}

#[tokio::test]
async fn a_sender_with_nothing_left_to_send_is_a_typed_notification() {
    let (client, peer) = pair().await;
    client
        .options()
        .subscribe_events(&[Event::SenderDry], SubscribeEventAssocId::All)
        .unwrap();
    client.send(b"data", SendOptions::new()).await.unwrap();
    let dry = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(NotificationOrData::Notification(Notification::SenderDry { assoc_id })) =
                client.recv().await.unwrap()
            {
                return assoc_id;
            }
        }
    })
    .await
    .unwrap();
    assert_ne!(dry, 0);
    drop(peer);
}

#[tokio::test]
async fn a_socket_prints_its_addresses() {
    let (client, peer) = pair().await;
    let printed = format!("{client:?}");
    let local = client.local_addr().unwrap();
    let remote = peer.local_addr().unwrap();
    assert!(
        printed.contains(&format!("local: Some({local})")),
        "{printed}"
    );
    assert!(
        printed.contains(&format!("peer: Some({remote})")),
        "{printed}"
    );
}

/// The per-association statistics count the packets an association
/// exchanged, and are read while it is up.
#[tokio::test]
async fn association_statistics_count_the_packets_exchanged() {
    let (client, peer) = pair().await;
    client.send(b"one", SendOptions::new()).await.unwrap();
    client.send(b"two", SendOptions::new()).await.unwrap();
    let _ = peer.recv().await.unwrap();
    let _ = peer.recv().await.unwrap();
    // The association of a One-to-one socket: the ID is ignored.
    let stats = client.options().association_stats(0).unwrap();
    assert!(stats.out_packets > 0, "sent packets: {stats:?}");
    let peer_stats = peer.options().association_stats(0).unwrap();
    assert!(
        peer_stats.in_packets > 0,
        "received packets: {peer_stats:?}"
    );
    // A healthy loopback association retransmits nothing.
    assert_eq!(stats.rtx_chunks, 0, "{stats:?}");
}

/// An endpoint, and a peer that has an association with it, whose ID at the endpoint the
/// first message gives.
async fn endpoint_and_peer() -> (OneToManyEndpoint, ConnectedSocket, AssociationId) {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(4).unwrap();
    let peer = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (peer, _) = peer.connect(endpoint.local_addr().unwrap()).await.unwrap();
    peer.send(b"hello", SendOptions::new()).await.unwrap();
    let NotificationOrData::Data(message) = endpoint.recv().await.unwrap() else {
        panic!("not the message of the peer")
    };
    (endpoint, peer, message.assoc_id().unwrap())
}

/// What the associations of a socket start with is read back as it was set.
#[tokio::test]
async fn the_init_parameters_are_read_back() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let params = InitParams {
        outbound_streams: 7,
        inbound_streams: 9,
        max_attempts: 3,
        max_init_timeout: 1500,
    };
    socket.options().set_init_params(params).unwrap();
    assert_eq!(socket.options().init_params().unwrap(), params);
}

/// A heartbeat interval of zero is set as zero, and the longest, `u32::MAX` milliseconds, is
/// set; one longer is refused.
#[tokio::test]
async fn a_heartbeat_interval_from_zero_to_the_longest() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let options = socket.options();
    let defaults = "0.0.0.0:0".parse().unwrap();
    let mut params = options.peer_address_params(0, defaults).unwrap();
    assert_ne!(params.heartbeat_interval, Duration::ZERO);
    params.heartbeat_enabled = true;
    for millis in [0, u64::from(u32::MAX)] {
        params.heartbeat_interval = Duration::from_millis(millis);
        options.set_peer_address_params(params.clone()).unwrap();
        let read = options.peer_address_params(0, defaults).unwrap();
        assert_eq!(read.heartbeat_interval, Duration::from_millis(millis));
    }
    params.heartbeat_interval = Duration::from_millis(u64::from(u32::MAX) + 1);
    let error = options.set_peer_address_params(params).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// On an endpoint, the options of an association are its own: read and set by its ID, apart
/// from those of the endpoint.
#[tokio::test]
async fn the_options_of_an_association_of_an_endpoint_are_its_own() {
    let (endpoint, _peer, assoc_id) = endpoint_and_peer().await;
    let options = endpoint.options();
    assert_eq!(options.rto_info(assoc_id).unwrap().assoc_id, assoc_id);
    assert_eq!(options.sack_info(assoc_id).unwrap().assoc_id, assoc_id);
    assert_eq!(
        options.association_stats(assoc_id).unwrap().assoc_id,
        assoc_id
    );
    assert_eq!(
        options.association_params(assoc_id).unwrap().assoc_id,
        assoc_id
    );
    let before = options.association_params(0).unwrap();
    let wanted = (before.max_retransmissions + 1, before.cookie_life / 2);
    let params = AssociationParams {
        assoc_id,
        max_retransmissions: wanted.0,
        cookie_life: wanted.1,
        ..Default::default()
    };
    options.set_association_params(params).unwrap();
    let association = options.association_params(assoc_id).unwrap();
    assert_eq!(
        (association.max_retransmissions, association.cookie_life),
        wanted
    );
    let own = options.association_params(0).unwrap();
    assert_eq!(
        (own.max_retransmissions, own.cookie_life),
        (before.max_retransmissions, before.cookie_life)
    );
}

/// A DSCP is 0 to 63: 63 is taken.
#[tokio::test]
async fn the_highest_dscp_is_taken() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.options().set_dscp(63).unwrap();
    assert_eq!(socket.options().dscp().unwrap(), 63);
}

/// A subscription that the kernel refuses for each event says which events and why: on an
/// endpoint, an association that it does not have.
#[tokio::test]
async fn a_refused_subscription_says_each_event_and_why() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(1).unwrap();
    let events = [Event::Association, Event::Shutdown];
    let error = endpoint
        .options()
        .subscribe_events(&events, SubscribeEventAssocId::Value(12345))
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    let refused = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<EventSubscriptionError>())
        .unwrap();
    let failures: Vec<(Event, Option<i32>)> = refused
        .failures()
        .iter()
        .map(|(event, error)| (*event, error.raw_os_error()))
        .collect();
    let einval = Some(libc::EINVAL);
    assert_eq!(failures, [(events[0], einval), (events[1], einval)]);
    let invalid = std::io::Error::from_raw_os_error(libc::EINVAL);
    assert_eq!(
        refused.to_string(),
        format!("Association: {invalid}; Shutdown: {invalid}")
    );
    let source = std::error::Error::source(refused).unwrap();
    let source = source.downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(source.raw_os_error(), einval);
}

/// An endpoint holds 4 MiB of a message and 64 MiB of the messages that it assembles, by
/// default.
#[tokio::test]
async fn the_limits_of_an_endpoint_by_default() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(1).unwrap();
    assert_eq!(endpoint.options().max_message_size(), 4 << 20);
    assert_eq!(endpoint.options().max_assembly(), 64 << 20);
}

/// Each role prints its local address and its descriptor.
#[tokio::test]
async fn each_role_prints_its_address_and_descriptor() {
    let printed = |printed: String, role: &str, local: std::net::SocketAddr, fd: i32| {
        let expected = format!("{role} {{ local: Some({local}), fd: {fd} }}");
        assert_eq!(printed, expected);
    };
    use std::os::fd::AsRawFd;
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let local = socket.local_addr().unwrap();
    printed(format!("{socket:?}"), "Socket", local, socket.as_raw_fd());
    let endpoint = socket.into_endpoint(1).unwrap();
    let fd = endpoint.as_raw_fd();
    printed(format!("{endpoint:?}"), "OneToManyEndpoint", local, fd);
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let local = listener.local_addr().unwrap();
    printed(
        format!("{listener:?}"),
        "Listener",
        local,
        listener.as_raw_fd(),
    );
}

/// A socket bound to no address has none; one bound to many has each, past the room of the
/// first buffer of the query too.
#[tokio::test]
async fn the_local_addresses_from_none_to_many() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    assert_eq!(socket.local_addrs().unwrap(), []);
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = socket.local_addr().unwrap().port();
    // 300 addresses of the loopback interface, 16 octets each: 4,800 octets
    let more: Vec<std::net::SocketAddr> = (2..302_u16)
        .map(|host| std::net::SocketAddr::from(([127, 0, (host >> 8) as u8, host as u8], port)))
        .collect();
    socket.bindx(&more, BindxFlags::Add).unwrap();
    let local = socket.local_addrs().unwrap();
    assert_eq!(local.len(), 301);
    assert!(more.iter().all(|address| local.contains(address)));
}

/// An endpoint adds addresses to those that it is bound to, and has no peer addresses for
/// an association that it does not have.
#[tokio::test]
async fn an_endpoint_adds_addresses_and_has_none_of_an_association_that_it_lacks() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(1).unwrap();
    let mut other = endpoint.local_addr().unwrap();
    other.set_ip("127.0.0.2".parse().unwrap());
    endpoint.bindx(&[other], BindxFlags::Add).unwrap();
    assert!(endpoint.local_addrs(0).unwrap().contains(&other));
    let error = endpoint.peer_addrs(12345).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
}

/// An endpoint that has an association with a peer refuses to start another with it.
#[tokio::test]
async fn an_endpoint_refuses_a_second_association_with_a_peer() {
    let (endpoint, peer, _) = endpoint_and_peer().await;
    let error = endpoint.connect(&[peer.local_addr().unwrap()]).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EISCONN));
}

/// Two sockets may share a port with `SO_REUSEADDR` while neither listens: the second to
/// listen is the error of the kernel.
#[tokio::test]
async fn a_second_listener_on_a_shared_port_is_refused() {
    let reusing = || {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        socket.options().set_reuseaddr(true).unwrap();
        socket
    };
    let first = reusing();
    first.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let second = reusing();
    second.bind(first.local_addr().unwrap()).unwrap();
    let _listener = first.listen(1).unwrap();
    let error = second.listen(1).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EADDRINUSE));
}

/// `recv_many` of an endpoint with a limit of 0 returns 0 at once.
#[tokio::test]
async fn recv_many_of_an_endpoint_with_a_limit_of_0_returns_0() {
    let (endpoint, _peer, _) = endpoint_and_peer().await;
    let mut out = Vec::new();
    assert_eq!(endpoint.recv_many(&mut out, 0).await.unwrap(), 0);
    assert!(out.is_empty());
}

/// The options of a batch of a connected socket name no association.
#[tokio::test]
async fn a_batch_of_a_connected_socket_names_no_association() {
    let (client, _peer) = pair().await;
    let messages = [(&b"x"[..], SendOptions::new().association(5))];
    let error = client.send_many(&messages).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// A send of an endpoint to an association that it does not have fails, and leaves its
/// other associations as they were.
#[tokio::test]
async fn a_send_to_an_association_that_an_endpoint_lacks_fails_alone() {
    let (endpoint, peer, assoc_id) = endpoint_and_peer().await;
    let lacking = SendOptions::new().association(assoc_id + 100);
    let error = endpoint.send(b"lost", lacking).await.unwrap_err();
    assert!(error.raw_os_error().is_some(), "{error}");
    let options = SendOptions::new().association(assoc_id);
    endpoint.send(b"kept", options).await.unwrap();
    match tokio::time::timeout(Duration::from_secs(2), peer.recv()).await {
        Ok(Ok(Some(NotificationOrData::Data(message)))) => assert_eq!(message.payload, b"kept"),
        other => panic!("{other:?}"),
    }
}

/// An endpoint shuts an association down with a send of no message and the flag `EOF`.
#[tokio::test]
async fn an_endpoint_shuts_an_association_down_with_an_empty_send() {
    let (endpoint, peer, assoc_id) = endpoint_and_peer().await;
    let info = SendInfo {
        flags: SendInfo::EOF,
        assoc_id,
        ..Default::default()
    };
    let data = SendData::new(Vec::new()).with_info(info);
    endpoint.send_data(None, data).await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(2), peer.recv()).await;
    assert!(matches!(end, Ok(Ok(None))), "{end:?}");
}
