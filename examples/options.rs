//! Options that are set before `listen`, and the status of an association.
//!
//! ```text
//! cargo run --example options
//! ```

use oxirush_sctp::{InitParams, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // What is set before `listen` or `connect` stays, and the accepted
    // associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let options = socket.options();
    options.set_nodelay(true)?; // send small messages at once
    options.set_init_params(InitParams::streams(4, 4))?; // four streams each way
    options.set_max_message_size(64 << 10); // refuse longer received messages
    let listener = socket.listen(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (_peer, _) = peer.connect(listener.local_addr()?).await?;
    let (association, _) = listener.accept().await?;

    let status = association.status()?;
    println!(
        "{:?}, {} outgoing and {} incoming streams",
        status.state, status.outstreams, status.instreams
    );
    let options = association.options();
    println!(
        "nodelay {}, messages of {} octets at most",
        options.nodelay()?,
        options.max_message_size()
    );
    Ok(())
}
