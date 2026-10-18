//! Several messages a system call: `send_many` and `recv_many`.
//!
//! ```text
//! cargo run --example batches
//! ```

use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(5)?;
    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (client, _) = client.connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;

    // Three messages, each with its options, in one system call
    let messages = [
        (&b"one"[..], SendOptions::new().stream(0).ppid(60)),
        (b"two", SendOptions::new().stream(1).ppid(60)),
        (b"three", SendOptions::new().stream(1).ppid(60)),
    ];
    let mut sent = 0;
    while sent < messages.len() {
        sent += client.send_many(&messages[sent..]).await?;
    }

    // What has come, 64 messages a call at most
    let mut received = Vec::new();
    while received.len() < messages.len() {
        server.recv_many(&mut received, 64).await?;
    }
    for item in &received {
        if let NotificationOrData::Data(message) = item {
            let text = String::from_utf8_lossy(&message.payload);
            println!("{text:?} on stream {:?}", message.stream_id());
        }
    }
    Ok(())
}
