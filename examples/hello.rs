//! A client and a server exchange one message each on stream 1, with a payload protocol
//! identifier.
//!
//! ```text
//! cargo run --example hello
//! ```

use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Stream 1, and the payload protocol identifier of NGAP.
    let options = SendOptions {
        stream_id: 1,
        ppid: 60,
        ..Default::default()
    };

    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;
    let address = listener.local_addr()?;

    let server = async {
        let (association, peer) = listener.accept().await?;
        if let NotificationOrData::Data(message) = association.recv().await? {
            println!(
                "{peer} sent {:?} on stream {:?} with PPID {:?}",
                String::from_utf8_lossy(&message.payload),
                message.stream_id(),
                message.ppid()
            );
            association.send(b"pong", options).await?;
        }
        std::io::Result::Ok(())
    };
    let client = async {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let (association, _) = socket.connect(address).await?;
        association.send(b"ping", options).await?;
        if let NotificationOrData::Data(message) = association.recv().await? {
            let answer = String::from_utf8_lossy(&message.payload);
            println!("the server answered {answer:?}");
        }
        std::io::Result::Ok(())
    };
    let (server, client) = tokio::join!(server, client);
    server.and(client)
}
