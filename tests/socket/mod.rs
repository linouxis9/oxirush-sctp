use super::{connect_endpoint, create_client_socket, create_endpoint_bind_and_listen};

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

#[allow(unused)]
use oxirush_sctp::*;

/// A message that an endpoint receives has the association, the stream and the payload
/// protocol identifier that it came with.
#[tokio::test]
async fn an_endpoint_receives_the_association_of_each_message() {
    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);
    let (listener, bindaddr) = create_endpoint_bind_and_listen(true);
    let (connected, assoc_id) = connect_endpoint(client_socket, &[bindaddr]).await.unwrap();
    let client_addr = connected.local_addrs(assoc_id).unwrap()[0];
    let options = SendOptions::new().stream(1).ppid(60);
    listener
        .send_to(client_addr, b"hello world!", options)
        .await
        .unwrap();
    let NotificationOrData::Data(data) = connected.recv().await.unwrap() else {
        panic!("not data");
    };
    assert_eq!(data.payload, b"hello world!");
    assert_eq!((data.assoc_id, data.stream, data.ppid), (assoc_id, 1, 60));
}

#[tokio::test]
async fn socket_init_params_set_ostreams_success() {
    let (listener, bindaddr) = create_endpoint_bind_and_listen(true);

    listener
        .options()
        .subscribe_events(&[Event::Association])
        .unwrap();

    let client_ostreams = 100;
    let client_istreams = 5;
    let client_socket = create_client_socket(SocketToAssociation::OneToMany, true);
    client_socket
        .options()
        .set_init_params(InitParams::streams(client_ostreams, client_istreams))
        .unwrap();

    let _client = connect_endpoint(client_socket, &[bindaddr]).await.unwrap();

    let notification = listener.recv().await.unwrap();
    assert!(
        matches!(
            notification,
            NotificationOrData::Notification(Notification::AssociationChange(
                AssociationChange { .. }
            ))
        ),
        "{notification:#?}"
    );

    if let NotificationOrData::Notification(Notification::AssociationChange(AssociationChange {
        inbound_streams: ib_streams,
        outbound_streams: ob_streams,
        ..
    })) = notification
    {
        assert!(
            ib_streams == client_ostreams,
            "client_ostreams: {client_ostreams}, ib_streams: {ib_streams}"
        );
        assert!(
            ob_streams == client_istreams,
            "client_istreams: {client_istreams}, ob_streams: {ob_streams}"
        );
    } else {
        panic!("Should never come here!: {notification:#?}");
    };
}

#[tokio::test]
async fn test_bind_success() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let bindaddr = Ipv4Addr::UNSPECIFIED;

    sctp_socket
        .bind(SocketAddr::new(IpAddr::V4(bindaddr), 0))
        .unwrap();
}

#[tokio::test]
async fn test_bindx_inaddr_any_add_success() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, true);
    let bindaddr = Ipv4Addr::UNSPECIFIED;

    sctp_socket
        .bindx(&[SocketAddr::new(IpAddr::V4(bindaddr), 0)], BindxFlags::Add)
        .unwrap();
}

#[tokio::test]
async fn test_bindx_inaddr6_any_add_success() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, false);
    let bindaddr = Ipv6Addr::UNSPECIFIED;

    sctp_socket
        .bindx(&[SocketAddr::new(IpAddr::V6(bindaddr), 0)], BindxFlags::Add)
        .unwrap();
}

#[tokio::test]
async fn test_bindx_inaddr_any_add_and_remove_failure() {
    let sctp_socket = create_client_socket(SocketToAssociation::OneToOne, false);
    let bindaddr6_localhost = Ipv6Addr::LOCALHOST;

    sctp_socket
        .bindx(
            &[SocketAddr::new(IpAddr::V6(bindaddr6_localhost), 0)],
            BindxFlags::Add,
        )
        .unwrap();

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
        .set_init_params(InitParams {
            outbound_streams: 1,
            inbound_streams: 1,
            max_attempts: 1,
            max_init_timeout: 100,
        })
        .unwrap();
    assert_eq!(
        client_socket.options().init_params().unwrap(),
        InitParams {
            outbound_streams: 1,
            inbound_streams: 1,
            max_attempts: 1,
            max_init_timeout: 100,
        }
    );
    client_socket
        .options()
        .set_rto_info(RtoInfo::new(100, 100, 100))
        .unwrap();
    // Routed to the loopback interface, which drops the INIT: it is not a local address.
    let connect_addr: SocketAddr = "10.99.0.2:8080".parse().unwrap();

    let result = client_socket.connect(connect_addr).await;
    let err = result.err().unwrap();
    assert_eq!(err.raw_os_error(), Some(libc::ETIMEDOUT), "{err}");
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
        other => eprintln!("no network namespace with `ip` ({other:?}); skipping"),
    }
}
