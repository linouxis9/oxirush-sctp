#![cfg(test)]

use oxirush_sctp::{
    AssociationId, ConnState, Listener, OneToManyEndpoint, Socket, SocketToAssociation,
};
use std::net::SocketAddr;

fn create_socket_bind_and_listen(
    association: SocketToAssociation,
    v4: bool,
) -> (Listener, SocketAddr) {
    let sctp_socket = if v4 {
        Socket::new_v4(association)
    } else {
        Socket::new_v6(association)
    };
    assert!(sctp_socket.is_ok(), "{:#?}", sctp_socket.err().unwrap());
    let sctp_socket = sctp_socket.unwrap();

    let mut bindaddr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let result = sctp_socket.bind(bindaddr);
    assert!(result.is_ok(), "{:#?}", result.err().unwrap());

    let listener = sctp_socket.listen(10);
    assert!(listener.is_ok(), "{:#?}", listener.err().unwrap());

    let listener = listener.unwrap();
    bindaddr.set_port(listener.local_addrs(0).unwrap()[0].port());
    (listener, bindaddr)
}

fn create_client_socket(association: SocketToAssociation, v4: bool) -> Socket {
    let client_socket = if v4 {
        Socket::new_v4(association)
    } else {
        Socket::new_v6(association)
    };
    assert!(client_socket.is_ok(), "{:#?}", client_socket.err().unwrap());

    client_socket.unwrap()
}

fn create_endpoint_bind_and_listen(v4: bool) -> (OneToManyEndpoint, SocketAddr) {
    let socket = create_client_socket(SocketToAssociation::OneToMany, v4);
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(10).unwrap();
    let address = endpoint.local_addrs(0).unwrap()[0];
    (endpoint, address)
}

async fn connect_endpoint(
    socket: Socket,
    addresses: &[SocketAddr],
) -> std::io::Result<(OneToManyEndpoint, AssociationId)> {
    let endpoint = socket.into_endpoint(10)?;
    let assoc_id = endpoint.connect(addresses)?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if endpoint.status(assoc_id)?.state == ConnState::Established {
                return Ok::<_, std::io::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::TimedOut, error))??;
    Ok((endpoint, assoc_id))
}

mod connected_socket;
mod listener;
#[cfg(target_os = "linux")]
mod options;
mod socket;
