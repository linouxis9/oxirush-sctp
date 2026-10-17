//! A writer and a reader for a socket, each a task, with the overload of the writer's queue.
//!
//! ```text
//! cargo run --example driver
//! ```

use oxirush_sctp::driver::{self, DriverConfig};
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;
    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (client, _) = client.connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;

    // A writer, which every task that sends can clone, and a reader
    let (writer, _) = driver::spawn(client, DriverConfig::default())?;
    let (_, mut reader) = driver::spawn(server, DriverConfig::default())?;
    for index in 0..1000_u32 {
        let options = SendOptions::new().ppid(60);
        writer.send(index.to_be_bytes().to_vec(), options).await?;
    }
    let mut last = 0;
    for _ in 0..1000 {
        if let Some(Ok(NotificationOrData::Data(message))) = reader.recv().await {
            last = u32::from_be_bytes(message.payload[..4].try_into().unwrap());
        }
    }
    let overload = writer.overload();
    println!(
        "last {last}, refused {}, in overload {}",
        overload.refused, overload.overloaded
    );
    Ok(())
}
