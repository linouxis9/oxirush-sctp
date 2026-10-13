//! A one-to-many socket: every association on one `recv`, and no accept.
//!
//! ```text
//! cargo run --example one_to_many
//! ```

use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let endpoint = socket.into_endpoint(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (peer, _) = peer.connect(endpoint.local_addr()?).await?;
    peer.send(b"ping", SendOptions::new()).await?;

    // A message comes with its association, which the answer names
    if let NotificationOrData::Data(message) = endpoint.recv().await? {
        println!("endpoint: {:?}", String::from_utf8_lossy(&message.payload));
        let options = SendOptions::new().association(message.assoc_id().unwrap());
        endpoint.send(b"pong", options).await?;
    }
    if let Some(NotificationOrData::Data(message)) = peer.recv().await? {
        println!("peer: {:?}", String::from_utf8_lossy(&message.payload));
    }
    Ok(())
}
