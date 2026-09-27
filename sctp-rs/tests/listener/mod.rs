use crate::{create_client_socket, create_socket_bind_and_listen};
use sctp_rs::*;
use std::net::SocketAddr;

// Tests for `accept` API for Listening Socket.
#[tokio::test]
async fn listening_one_2_one_listen_accept_success() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);

    let assoc_id = client_socket.sctp_connectx(&[bindaddr]).await;
    assert!(assoc_id.is_ok(), "{:#?}", assoc_id.err().unwrap());

    let accept = listener.accept().await;
    assert!(accept.is_ok(), "{:#?}", accept.err().unwrap());

    // Get Peer Address
    let (accepted, _address) = accept.unwrap();
    let result = accepted.sctp_getpaddrs(0);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
}

#[tokio::test]
async fn listening_one_2_many_listen_accept_failure() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToMany, true);

    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);

    let assoc_id = client_socket.sctp_connectx(&[bindaddr]).await;
    assert!(assoc_id.is_ok(), "{:#?}", assoc_id.err().unwrap());

    let accept = listener.accept().await;
    assert!(accept.is_err(), "{:#?}", accept.ok().unwrap());
}

// Tests for `shutdown` API for Listening Socket.
// TODO:

// Test for `sctp_bindx` API for Listening Socket.
#[tokio::test]
async fn listening_sctp_bindx_add_success() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let bindx_bindaddr: SocketAddr = format!("127.0.0.53:{}", bindaddr.port()).parse().unwrap();
    let result = listener.sctp_bindx(&[bindx_bindaddr], BindxFlags::Add);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
}

// Tests for `sctp_peeloff` API for Listening Socket.
#[tokio::test]
async fn listening_socket_no_connect_peeloff_failure() {
    let (listener, _) = create_socket_bind_and_listen(SocketToAssociation::OneToMany, true);

    let result = listener.sctp_peeloff(42);
    assert!(result.is_err(), "{:#?}", result.ok().unwrap());
}

#[tokio::test]
async fn listening_socket_one2one_connected_peeloff_failure() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);

    let result =
        listener.sctp_subscribe_events(&[Event::Association], SubscribeEventAssocId::Future);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);

    let assoc_id = client_socket.sctp_connectx(&[bindaddr]).await;
    assert!(assoc_id.is_ok(), "{:#?}", assoc_id.err().unwrap());

    let received = listener.sctp_peeloff(0);
    assert!(received.is_err(), "{:#?}", received.ok().unwrap());
}

#[tokio::test]
async fn listening_socket_one2many_connected_peeloff_success() {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToMany, true);

    let result =
        listener.sctp_subscribe_events(&[Event::Association], SubscribeEventAssocId::Future);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);

    let assoc_id = client_socket.sctp_connectx(&[bindaddr]).await;
    assert!(assoc_id.is_ok(), "{:#?}", assoc_id.err().unwrap());

    let result = listener.sctp_recv().await;
    assert!(result.is_ok(), "{:#}", result.err().unwrap());

    let notification = result.unwrap();
    assert!(
        matches!(
            notification,
            NotificationOrData::Notification(Notification::AssociationChange(
                AssociationChange { .. }
            ))
        ),
        "{:#?}",
        notification
    );

    if let NotificationOrData::Notification(Notification::AssociationChange(AssociationChange {
        assoc_id,
        state,
        ..
    })) = notification
    {
        let received = listener.sctp_peeloff(assoc_id);
        assert!(received.is_ok(), "{:#?}", received.err().unwrap());
        assert!(state == AssocChangeState::CommUp, "{:#?}", state);
    } else {
        assert!(false, "Should never come here!: {:#?}", notification);
    };
}

// Tests for `sctp_getpaddrs` and `sctp_getladdrs` for Listening Socket.
#[tokio::test]
async fn listening_getladdrs_and_getpaddrs_of_many_addresses() {
    for v4 in [true, false] {
        let socket = if v4 {
            Socket::new_v4(SocketToAssociation::OneToOne).unwrap()
        } else {
            Socket::new_v6(SocketToAssociation::OneToOne).unwrap()
        };
        // Loopback addresses: 127.0.0.0/8 is local.
        let first: SocketAddr = "127.0.0.1:0".parse().unwrap();
        socket.bind(first).unwrap();
        let port = {
            let listener = socket.listen(10).unwrap();
            let port = listener.sctp_getladdrs(0).unwrap()[0].port();
            let more: Vec<SocketAddr> = (2..=20)
                .map(|i| SocketAddr::from(([127, 0, 0, i], port)))
                .collect();
            listener.sctp_bindx(&more, BindxFlags::Add).unwrap();

            let mut laddrs = listener.sctp_getladdrs(0).unwrap();
            assert_eq!(laddrs.len(), 20, "{:?}", laddrs);
            laddrs.sort();
            // IPv6 sockets report IPv4 addresses as IPv4-mapped IPv6 addresses.
            let expected: Vec<SocketAddr> = (1..=20)
                .map(|i| {
                    let ip = std::net::Ipv4Addr::new(127, 0, 0, i);
                    if v4 {
                        SocketAddr::from((ip, port))
                    } else {
                        SocketAddr::from((ip.to_ipv6_mapped(), port))
                    }
                })
                .collect();
            assert_eq!(laddrs, expected);

            let client = create_client_socket(SocketToAssociation::OneToOne, true);
            let (connected, _) = client
                .sctp_connectx(&[SocketAddr::from(([127, 0, 0, 1], port))])
                .await
                .unwrap();
            let (accepted, _) = listener.accept().await.unwrap();
            let paddrs = connected.sctp_getpaddrs(0).unwrap();
            assert_eq!(paddrs.len(), 20, "{:?}", paddrs);
            assert_eq!(accepted.sctp_getladdrs(0).unwrap().len(), 20);
            port
        };
        assert_ne!(port, 0);
    }
}

// Tests for `sctp_recv` for Listening Socket.
// TODO:

// Tests for `sctp_send for Listening Socket.
// TODO:

// Tests for `sctp_subscribe_event`/`sctp_unsubscribe_event` for Listening Socket.
// TODO:
