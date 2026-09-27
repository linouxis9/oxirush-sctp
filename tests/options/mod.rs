//! Access to the descriptors and socket options.

use super::{create_client_socket, create_socket_bind_and_listen};

use std::os::unix::io::{AsFd, AsRawFd, RawFd};

use oxirush_sctp::*;

fn getsockopt_int(fd: RawFd, level: libc::c_int, name: libc::c_int) -> libc::c_int {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // Safety: `value` and `len` outlive the call and `len` is the size of `value`.
    let result = unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            &mut value as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
    value
}

fn assert_sctp_socket<T: AsFd + AsRawFd>(fd: &T, sock_type: libc::c_int) {
    assert_eq!(fd.as_fd().as_raw_fd(), fd.as_raw_fd());
    let raw = fd.as_raw_fd();
    assert_eq!(
        getsockopt_int(raw, libc::SOL_SOCKET, libc::SO_PROTOCOL),
        libc::IPPROTO_SCTP
    );
    assert_eq!(
        getsockopt_int(raw, libc::SOL_SOCKET, libc::SO_TYPE),
        sock_type
    );
}

#[tokio::test]
async fn descriptors_are_the_sctp_sockets() {
    assert_sctp_socket(
        &create_client_socket(SocketToAssociation::OneToOne, true),
        libc::SOCK_STREAM,
    );
    assert_sctp_socket(
        &create_client_socket(SocketToAssociation::OneToMany, false),
        libc::SOCK_SEQPACKET,
    );

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    assert_sctp_socket(&listener, libc::SOCK_STREAM);

    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    let (connected, _) = client.sctp_connectx(&[bindaddr]).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    assert_sctp_socket(&connected, libc::SOCK_STREAM);
    assert_sctp_socket(&accepted, libc::SOCK_STREAM);
}

const SOL_SCTP: libc::c_int = 132;
const SCTP_NODELAY: libc::c_int = 3;

#[tokio::test]
async fn nodelay_round_trips_on_every_socket_type() {
    let socket = create_client_socket(SocketToAssociation::OneToMany, true);
    assert!(!socket.nodelay().unwrap());
    socket.set_nodelay(true).unwrap();
    assert!(socket.nodelay().unwrap());
    assert_eq!(
        getsockopt_int(socket.as_raw_fd(), SOL_SCTP, SCTP_NODELAY),
        1
    );
    socket.set_nodelay(false).unwrap();
    assert!(!socket.nodelay().unwrap());
    assert_eq!(
        getsockopt_int(socket.as_raw_fd(), SOL_SCTP, SCTP_NODELAY),
        0
    );

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    listener.set_nodelay(true).unwrap();
    assert!(listener.nodelay().unwrap());
    assert_eq!(
        getsockopt_int(listener.as_raw_fd(), SOL_SCTP, SCTP_NODELAY),
        1
    );

    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    client.set_nodelay(true).unwrap();
    let (connected, _) = client.sctp_connectx(&[bindaddr]).await.unwrap();
    assert!(connected.nodelay().unwrap());
    connected.set_nodelay(false).unwrap();
    assert!(!connected.nodelay().unwrap());
    assert_eq!(
        getsockopt_int(connected.as_raw_fd(), SOL_SCTP, SCTP_NODELAY),
        0
    );
}

#[tokio::test]
async fn accepted_and_peeled_off_sockets_inherit_nodelay() {
    // Set on the socket before `listen`.
    let socket = create_client_socket(SocketToAssociation::OneToOne, true);
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket.set_nodelay(true).unwrap();
    let listener = socket.listen(10).unwrap();
    let bindaddr = listener.sctp_getladdrs(0).unwrap()[0];
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    let _connected = client.sctp_connectx(&[bindaddr]).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    assert!(accepted.nodelay().unwrap());

    // Set on the listener, and the other way round.
    for nodelay in [true, false] {
        let (listener, bindaddr) =
            create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
        listener.set_nodelay(nodelay).unwrap();
        let client = create_client_socket(SocketToAssociation::OneToOne, true);
        client.set_nodelay(!nodelay).unwrap();
        let _connected = client.sctp_connectx(&[bindaddr]).await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        assert_eq!(accepted.nodelay().unwrap(), nodelay);
    }

    // Peeled off a one-to-many listener.
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToMany, true);
    listener.set_nodelay(true).unwrap();
    listener
        .sctp_subscribe_events(&[Event::Association], SubscribeEventAssocId::Future)
        .unwrap();
    let client = create_client_socket(SocketToAssociation::OneToMany, true);
    let _connected = client.sctp_connectx(&[bindaddr]).await.unwrap();
    let assoc_id = match listener.sctp_recv().await.unwrap() {
        NotificationOrData::Notification(Notification::AssociationChange(change)) => {
            change.assoc_id
        }
        other => panic!("not an association change: {:?}", other),
    };
    let peeled = listener.sctp_peeloff(assoc_id).unwrap();
    assert!(peeled.nodelay().unwrap());
}

async fn recv_data(socket: &ConnectedSocket) -> Vec<u8> {
    match socket.sctp_recv().await.unwrap() {
        NotificationOrData::Data(data) => data.payload,
        other => panic!("not data: {:?}", other),
    }
}

/// How much later than the second of two small messages, sent back to back while the first is
/// unacknowledged, the third one arrives.
async fn third_message_lag(nodelay: bool) -> std::time::Duration {
    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    client.set_nodelay(nodelay).unwrap();
    let (connected, _) = client.sctp_connectx(&[bindaddr]).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    let send = |payload: &[u8]| {
        connected.sctp_send(SendData {
            payload: payload.to_vec(),
            snd_info: None,
        })
    };

    // Linux acknowledges the first DATA chunk of an association at once, then every second
    // packet or when its delayed SACK timer (200 ms by default) expires.
    send(b"first").await.unwrap();
    assert_eq!(recv_data(&accepted).await, b"first");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Nothing is in flight, so "second" leaves at once, and its SACK is delayed. Nagle's
    // algorithm holds "third" back until that SACK arrives, unless `SCTP_NODELAY` is set.
    send(b"second").await.unwrap();
    send(b"third").await.unwrap();
    assert_eq!(recv_data(&accepted).await, b"second");
    let second = std::time::Instant::now();
    assert_eq!(recv_data(&accepted).await, b"third");
    second.elapsed()
}

#[tokio::test]
async fn nodelay_does_not_hold_small_messages_while_data_is_unacknowledged() {
    // Without `SCTP_NODELAY` the scenario shows the stall, so the test does test something.
    let lag = third_message_lag(false).await;
    assert!(lag >= std::time::Duration::from_millis(100), "{:?}", lag);

    let lag = third_message_lag(true).await;
    assert!(lag < std::time::Duration::from_millis(50), "{:?}", lag);
}

fn assert_close_on_exec_and_non_blocking<T: AsRawFd>(socket: &T) {
    // Safety: `fcntl` only queries the descriptor.
    let (fd_flags, fl_flags) = unsafe {
        (
            libc::fcntl(socket.as_raw_fd(), libc::F_GETFD),
            libc::fcntl(socket.as_raw_fd(), libc::F_GETFL),
        )
    };
    assert!(fd_flags & libc::FD_CLOEXEC != 0, "not close-on-exec");
    assert!(fl_flags & libc::O_NONBLOCK != 0, "blocking");
}

#[tokio::test]
async fn descriptors_are_close_on_exec() {
    for association in [
        SocketToAssociation::OneToOne,
        SocketToAssociation::OneToMany,
    ] {
        for v4 in [true, false] {
            assert_close_on_exec_and_non_blocking(&create_client_socket(association.clone(), v4));
        }
    }

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    assert_close_on_exec_and_non_blocking(&listener);
    let client = create_client_socket(SocketToAssociation::OneToOne, true);
    let (connected, _) = client.sctp_connectx(&[bindaddr]).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    assert_close_on_exec_and_non_blocking(&connected);
    assert_close_on_exec_and_non_blocking(&accepted);

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToMany, true);
    listener
        .sctp_subscribe_events(&[Event::Association], SubscribeEventAssocId::Future)
        .unwrap();
    let client = create_client_socket(SocketToAssociation::OneToMany, true);
    let _connected = client.sctp_connectx(&[bindaddr]).await.unwrap();
    let assoc_id = match listener.sctp_recv().await.unwrap() {
        NotificationOrData::Notification(Notification::AssociationChange(change)) => {
            change.assoc_id
        }
        other => panic!("not an association change: {:?}", other),
    };
    assert_close_on_exec_and_non_blocking(&listener.sctp_peeloff(assoc_id).unwrap());
}

#[tokio::test]
async fn rto_info_round_trips() {
    let rto_info = RtoInfo {
        assoc_id: 0,
        initial: 500,
        max: 4000,
        min: 100,
    };
    let socket = create_client_socket(SocketToAssociation::OneToOne, true);
    socket.sctp_set_rto_info(rto_info.clone()).unwrap();
    assert_eq!(socket.sctp_get_rto_info(0).unwrap(), rto_info);
    // Zero leaves a value unchanged.
    socket
        .sctp_set_rto_info(RtoInfo {
            max: 6000,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        socket.sctp_get_rto_info(0).unwrap(),
        RtoInfo {
            max: 6000,
            ..rto_info.clone()
        }
    );
    // The kernel checks the values.
    let invalid = RtoInfo {
        min: 7000,
        ..rto_info.clone()
    };
    assert!(socket.sctp_set_rto_info(invalid).is_err());

    let (listener, bindaddr) = create_socket_bind_and_listen(SocketToAssociation::OneToOne, true);
    listener.sctp_set_rto_info(rto_info.clone()).unwrap();
    assert_eq!(listener.sctp_get_rto_info(0).unwrap(), rto_info);

    // Associations start with the values of their socket.
    let (connected, assoc_id) = socket.sctp_connectx(&[bindaddr]).await.unwrap();
    let association = connected.sctp_get_rto_info(assoc_id).unwrap();
    assert_eq!((association.max, association.min), (6000, 100));
    connected
        .sctp_set_rto_info(RtoInfo {
            assoc_id,
            max: 2000,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(connected.sctp_get_rto_info(assoc_id).unwrap().max, 2000);
}

#[tokio::test]
async fn reuseaddr_allows_binding_a_bound_address() {
    let bound = create_client_socket(SocketToAssociation::OneToOne, true);
    assert!(!bound.reuseaddr().unwrap());
    bound.set_reuseaddr(true).unwrap();
    assert!(bound.reuseaddr().unwrap());
    assert_eq!(
        getsockopt_int(bound.as_raw_fd(), libc::SOL_SOCKET, libc::SO_REUSEADDR),
        1
    );
    bound.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    // Bound, not listening.
    let address = {
        // Safety: `address` and `len` outlive the call and `len` is the size of `address`.
        let mut address: libc::sockaddr_in = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockname(
                bound.as_raw_fd(),
                &mut address as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
        std::net::SocketAddr::from(([127, 0, 0, 1], u16::from_be(address.sin_port)))
    };

    let without = create_client_socket(SocketToAssociation::OneToOne, true);
    let error = without.bind(address).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EADDRINUSE), "{}", error);

    let with = create_client_socket(SocketToAssociation::OneToOne, true);
    with.set_reuseaddr(true).unwrap();
    with.bind(address).unwrap();
}
