//! How the end of an association reaches `recv`: its peer closed it, or
//! aborted it.
//!
//! ```text
//! cargo run --example association_end
//! ```

use oxirush_sctp::{ConnectedSocket, Listener, Socket, SocketToAssociation};
use std::time::Duration;

/// The two ends of a new association of `listener`.
async fn connected(listener: &Listener) -> std::io::Result<(ConnectedSocket, ConnectedSocket)> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (peer, _) = socket.connect(listener.local_addr()?).await?;
    let (accepted, _) = listener.accept().await?;
    Ok((accepted, peer))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(5)?;

    // The peer closes its socket: `None`, at every call from then on
    let (association, peer) = connected(&listener).await?;
    drop(peer);
    for _ in 0..2 {
        println!("closed: {:?}", association.recv().await?);
    }

    // The peer aborts, as a socket does that is dropped with a linger of
    // zero: an error
    let (association, peer) = connected(&listener).await?;
    peer.options().set_linger(Some(Duration::ZERO))?;
    drop(peer);
    let end = association.recv().await.map(|_| ());
    println!("aborted: {:?}", end.map_err(|error| error.kind()));
    Ok(())
}
