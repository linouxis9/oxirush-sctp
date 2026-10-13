//! An association between two addresses on each side: `bindx` and `connectx`.
//!
//! ```text
//! cargo run --example multihoming
//! ```

use oxirush_sctp::{BindxFlags, Socket, SocketToAssociation};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

fn addresses(first: &str, second: &str) -> [SocketAddr; 2] {
    [first.parse().unwrap(), second.parse().unwrap()]
}

fn ips(addresses: &[SocketAddr]) -> Vec<IpAddr> {
    addresses.iter().map(SocketAddr::ip).collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // With port 0 the kernel chooses one port for both addresses
    let server = Socket::new_v4(SocketToAssociation::OneToOne)?;
    server.bindx(&addresses("127.0.0.1:0", "127.0.0.2:0"), BindxFlags::Add)?;
    let listener = server.listen(5)?;
    let server_addresses = listener.local_addrs()?;

    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    client.bindx(&addresses("127.0.0.1:0", "127.0.0.3:0"), BindxFlags::Add)?;
    let (client, id) = client.connectx(&server_addresses).await?;
    let (accepted, _) = listener.accept().await?;

    println!("client: {:?}", ips(&client.local_addrs()?));
    println!("server: {:?}", ips(&client.peer_addrs()?));
    println!("the server sees: {:?}", ips(&accepted.peer_addrs()?));
    println!(
        "primary path: {}",
        client.status()?.peer_primary.address.ip()
    );

    // Probe every path each second instead of each 30 seconds
    for address in client.peer_addrs()? {
        let mut path = client.options().peer_address_params(id, address)?;
        path.heartbeat_interval = Duration::from_secs(1);
        client.options().set_peer_address_params(path)?;
        let path = client.options().peer_address_params(id, address)?;
        println!(
            "{}: heartbeat every {:?}, lost after {} retransmissions",
            address.ip(),
            path.heartbeat_interval,
            path.path_max_retrans
        );
    }
    Ok(())
}
