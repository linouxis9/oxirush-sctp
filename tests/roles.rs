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
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput),
    }
}

#[tokio::test]
async fn one_to_many_socket_cannot_become_a_single_association() {
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let server = server.listen(4).unwrap();
    let address = server.sctp_getladdrs(0).unwrap()[0];
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    match socket.connect(address).await {
        Ok(_) => panic!("a one-to-many socket became a single association"),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput),
    }
}

#[tokio::test]
async fn one_to_one_socket_cannot_become_a_multi_association_endpoint() {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let error = socket.into_endpoint(4).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
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
    let address = listener.sctp_getladdrs(0).unwrap()[0];
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
    server.sctp_bindx(&addresses, BindxFlags::Add).unwrap();
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
        endpoint.sctp_getladdrs(0).unwrap()[0]
    );
}

#[tokio::test]
async fn connected_and_accepted_sockets_report_stream_and_ppid_by_default() {
    let server = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = server.listen(4).unwrap();
    let (client, assoc_id) = Socket::new_v4(SocketToAssociation::OneToOne)
        .unwrap()
        .connect(listener.sctp_getladdrs(0).unwrap()[0])
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let options = SendOptions {
        stream_id: 3,
        ppid: 60,
        ..Default::default()
    };
    client.send(b"request", options).await.unwrap();
    peer.send(b"answer", options).await.unwrap();
    let NotificationOrData::Data(data) = peer.recv().await.unwrap() else {
        panic!("not data")
    };
    assert_eq!((data.stream_id(), data.ppid()), (Some(3), Some(60)));
    let NotificationOrData::Data(data) = client.recv().await.unwrap() else {
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
    let address = endpoint.sctp_getladdrs(0).unwrap()[0];
    let (client, _) = Socket::new_v6(SocketToAssociation::OneToOne)
        .unwrap()
        .connect(address)
        .await
        .unwrap();
    client
        .send(b"identify", SendOptions::default())
        .await
        .unwrap();
    let NotificationOrData::Data(data) = endpoint.recv().await.unwrap() else {
        panic!("not data")
    };
    let assoc_id = data.rcv_info.unwrap().assoc_id;
    assert_ne!(assoc_id, 0);
    let peer = endpoint.peeloff(assoc_id).unwrap();
    assert_eq!(peer.options().max_message_size(), 100);
    assert!(peer.options().nodelay().unwrap());
    client
        .send(&[0xaa; 101], SendOptions::default())
        .await
        .unwrap();
    client.send(b"next", SendOptions::default()).await.unwrap();
    assert_eq!(
        peer.recv().await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    let NotificationOrData::Data(data) = peer.recv().await.unwrap() else {
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
        .connect(listener.sctp_getladdrs(0).unwrap()[0])
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
        .send(b"imported", SendOptions::default())
        .await
        .unwrap();
    let NotificationOrData::Data(data) = peer.recv().await.unwrap() else {
        panic!("not data")
    };
    assert_eq!(data.payload, b"imported");
    drop(imported);
    let NotificationOrData::Data(closed) =
        tokio::time::timeout(std::time::Duration::from_secs(2), peer.recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("not end of association")
    };
    assert!(
        closed.payload.is_empty(),
        "imported descriptor remains open"
    );
}
