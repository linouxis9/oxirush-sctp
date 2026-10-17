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
async fn a_message_is_received_into_the_buffer_of_the_caller() {
    let (client, peer) = pair().await;
    peer.options()
        .subscribe_events(&[Event::Shutdown], SubscribeEventAssocId::All)
        .unwrap();
    let mut buffer = [0_u8; 2048];

    client
        .send(b"into the buffer", SendOptions::new().stream(1).ppid(60))
        .await
        .unwrap();
    let Some(ReceivedInto::Data { len, rcv_info, .. }) = peer.recv_into(&mut buffer).await.unwrap()
    else {
        panic!("not a message");
    };
    assert_eq!(&buffer[..len], b"into the buffer");
    let info = rcv_info.unwrap();
    assert_eq!((info.sid, info.ppid), (1, 60));

    // A message longer than the buffer is discarded, and the one after it arrives
    client.send(&[7; 3000], SendOptions::new()).await.unwrap();
    client.send(b"next", SendOptions::new()).await.unwrap();
    let error = peer.recv_into(&mut buffer).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{error}");
    let Some(ReceivedInto::Data { len, .. }) = peer.recv_into(&mut buffer).await.unwrap() else {
        panic!("not a message");
    };
    assert_eq!(&buffer[..len], b"next");

    // A notification, then the end, as `recv` has them
    drop(client);
    let received = peer.recv_into(&mut buffer).await.unwrap();
    assert!(
        matches!(
            received,
            Some(ReceivedInto::Notification(Notification::Shutdown(_)))
        ),
        "{received:?}"
    );
    for _ in 0..2 {
        assert_eq!(peer.recv_into(&mut buffer).await.unwrap(), None);
    }
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
