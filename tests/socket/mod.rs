use super::{connect_endpoint, create_client_socket, create_endpoint_bind_and_listen};

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

#[allow(unused)]
use oxirush_sctp::*;

#[tokio::test]
async fn socket_connect_basic_send_recv_req_info_on_and_off() {
    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);

    let result = client_socket
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::Current);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    // Request Receive Info on client socket
    let result = client_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let (listener, bindaddr) = create_endpoint_bind_and_listen(true);

    let sock_and_assoc_id = connect_endpoint(client_socket, &[bindaddr]).await;
    assert!(
        sock_and_assoc_id.is_ok(),
        "{:#?}",
        sock_and_assoc_id.err().unwrap()
    );
    let (connected, assoc_id) = sock_and_assoc_id.unwrap();
    eprintln!("assoc_id: {}", assoc_id);

    let laddrs = connected.local_addrs(assoc_id);
    assert!(laddrs.is_ok(), "{:#?}", laddrs.err().unwrap());

    let client_addr = laddrs.unwrap()[0];

    let senddata = SendData {
        payload: b"hello world!".to_vec(),
        snd_info: None,
    };
    let result = listener
        .send_data(Some(client_addr), senddata.clone())
        .await;
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
        assert!(nxt_info.is_none(), "{:#?}", nxt_info.unwrap());
    } else {
        panic!("Should never come here!: {:#?}", data);
    };

    // Now turn off Request Receive Info on client socket
    let result = connected.options().request_rcvinfo(false);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    // Again send the data to client
    let result = listener.send_data(Some(client_addr), senddata).await;
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
        assert!(rcv_info.is_none(), "{:#?}", rcv_info.unwrap());
        assert!(nxt_info.is_none(), "{:#?}", nxt_info.unwrap());
    } else {
        panic!("Should never come here!: {:#?}", data);
    };
}

#[tokio::test]
async fn socket_send_recv_nxtinfo_test() {
    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);
    let result = client_socket
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::Current);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    // Request Receive Info on client socket
    let result = client_socket.options().request_nxtinfo(true);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let (listener, bindaddr) = create_endpoint_bind_and_listen(true);

    let sock_and_assoc_id = connect_endpoint(client_socket, &[bindaddr]).await;
    assert!(
        sock_and_assoc_id.is_ok(),
        "{:#?}",
        sock_and_assoc_id.err().unwrap()
    );
    let (connected, assoc_id) = sock_and_assoc_id.unwrap();
    connected.options().request_rcvinfo(false).unwrap();

    let laddrs = connected.local_addrs(assoc_id);
    assert!(laddrs.is_ok(), "{:#?}", laddrs.err().unwrap());

    let client_addr = laddrs.unwrap()[0];

    let senddata = SendData {
        payload: b"hello world!".to_vec(),
        snd_info: None,
    };
    let result = listener
        .send_data(Some(client_addr), senddata.clone())
        .await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    // Send again
    let result = listener
        .send_data(Some(client_addr), senddata.clone())
        .await;
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    // First Receive nxtinfo should not be none.
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
        assert!(rcv_info.is_none(), "{:#?}", rcv_info.unwrap());
        assert!(nxt_info.is_some());
    } else {
        panic!("Should never come here!: {:#?}", data);
    };

    // First Receive nxtinfo should not be none.
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
        assert!(rcv_info.is_none(), "{:#?}", rcv_info.unwrap());
        assert!(nxt_info.is_none(), "{:#?}", nxt_info.unwrap());
    } else {
        panic!("Should never come here!: {:#?}", data);
    };
}

#[tokio::test]
async fn socket_init_params_set_ostreams_success() {
    let (listener, bindaddr) = create_endpoint_bind_and_listen(true);

    let result = listener
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::Future);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let client_ostreams = 100;
    let client_istreams = 5;
    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);
    let result = client_socket
        .options()
        .set_init_params(client_ostreams, client_istreams, 0, 0);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let assoc_id = connect_endpoint(client_socket, &[bindaddr]).await;
    assert!(assoc_id.is_ok(), "{:#?}", assoc_id.err().unwrap());

    let result = listener.recv().await;
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
        ib_streams,
        ob_streams,
        ..
    })) = notification
    {
        assert!(
            ib_streams == client_ostreams,
            "client_ostreams: {}, ib_streams: {}",
            client_ostreams,
            ib_streams
        );
        assert!(
            ob_streams == client_istreams,
            "client_istreams: {}, ob_streams: {}",
            client_istreams,
            ob_streams
        );
    } else {
        panic!("Should never come here!: {:#?}", notification);
    };
}

#[tokio::test]
async fn socket_sctp_req_recv_info_success() {
    let one2one_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let result = one2one_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:?}", result.err().unwrap());

    let one2many_socket = create_client_socket(SocketToAssociation::OneToMany, true);
    let result = one2many_socket.options().request_rcvinfo(true);
    assert!(result.is_ok(), "{:?}", result.err().unwrap());
}

#[tokio::test]
async fn test_bind_success() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let bindaddr = Ipv4Addr::UNSPECIFIED;

    let result = sctp_socket.bind(SocketAddr::new(IpAddr::V4(bindaddr), 0));
    assert!(result.is_ok(), "{:?}", result.err().unwrap());
}

#[tokio::test]
async fn test_bindx_inaddr_any_add_success() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let bindaddr = Ipv4Addr::UNSPECIFIED;

    let result = sctp_socket.bindx(&[SocketAddr::new(IpAddr::V4(bindaddr), 0)], BindxFlags::Add);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
}

#[tokio::test]
async fn test_bindx_inaddr6_any_add_success() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, false);
    let bindaddr = Ipv6Addr::UNSPECIFIED;

    let result = sctp_socket.bindx(&[SocketAddr::new(IpAddr::V6(bindaddr), 0)], BindxFlags::Add);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());
}

#[tokio::test]
async fn test_bindx_inaddr_any_add_and_remove_failure() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, false);
    let bindaddr6_localhost = Ipv6Addr::LOCALHOST;

    let result = sctp_socket.bindx(
        &[SocketAddr::new(IpAddr::V6(bindaddr6_localhost), 0)],
        BindxFlags::Add,
    );
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let mut address: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&address) as libc::socklen_t;
    // Safety: both output buffers are valid for the stated sizes.
    assert_eq!(
        unsafe {
            libc::getsockname(
                std::os::fd::AsRawFd::as_raw_fd(&sctp_socket),
                &mut address as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        },
        0
    );
    let port = u16::from_be(address.sin6_port);

    let result = sctp_socket.bindx(
        &[SocketAddr::new(IpAddr::V6(bindaddr6_localhost), port)],
        BindxFlags::Remove,
    );
    assert!(result.is_err(), "{:#?}", result.ok().unwrap());
}

#[tokio::test]
async fn test_connect_no_listen_failure() {
    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let connect_addr: SocketAddr = "127.0.0.53:8080".parse().unwrap();

    let result = client_socket.connect(connect_addr).await;
    assert!(result.is_err(), "{:?}", result.ok().unwrap());
    let err = result.err().unwrap();
    assert_eq!(err.raw_os_error(), Some(libc::ECONNREFUSED));
}

/// In a new network namespace (see `connect_reports_why_the_association_did_not_start`), where
/// nothing answers the INIT.
#[tokio::test]
#[ignore]
async fn connect_in_network_namespace_times_out() {
    if std::env::var_os("SCTP_RS_TEST_IN_NETNS").is_none() {
        return;
    }
    let client_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    // One INIT, given up after 100 ms.
    client_socket
        .options()
        .set_init_params(1, 1, 1, 100)
        .unwrap();
    client_socket
        .options()
        .set_rto_info(RtoInfo {
            assoc_id: 0,
            initial: 100,
            max: 100,
            min: 100,
        })
        .unwrap();
    // Routed to the loopback interface, which drops the INIT: it is not a local address.
    let connect_addr: SocketAddr = "10.99.0.2:8080".parse().unwrap();

    let result = client_socket.connect(connect_addr).await;
    let err = result.err().unwrap();
    assert_eq!(err.raw_os_error(), Some(libc::ETIMEDOUT), "{}", err);
}

#[test]
fn connect_reports_why_the_association_did_not_start() {
    let status = std::process::Command::new("unshare")
        .args(["--user", "--map-root-user", "--net", "--", "sh", "-c"])
        .arg(
            "ip link set lo up && ip address add 10.98.0.1/32 dev lo \
             && ip route add 10.99.0.0/16 dev lo src 10.98.0.1 && exec \"$0\" \"$@\"",
        )
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "socket::connect_in_network_namespace_times_out",
            "--ignored",
        ])
        .env("SCTP_RS_TEST_IN_NETNS", "1")
        .status();
    match status.map(|status| status.code()) {
        Ok(Some(0)) => {}
        // The test harness exits with 101 when a test fails.
        Ok(Some(101)) => panic!("`connect_in_network_namespace_times_out` failed"),
        other => eprintln!("no network namespace with `ip` ({:?}); skipping", other),
    }
}
