//! What `recv` returns beside messages, once events are subscribed: an
//! association comes up, its peer shuts it down.
//!
//! ```text
//! cargo run --example notifications
//! ```

use oxirush_sctp::{
    Event, Notification, NotificationOrData, SendOptions, Socket, SocketToAssociation,
    SubscribeEventAssocId,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Subscribe before listening: the accepted associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    socket.options().subscribe_events(
        &[Event::Association, Event::Shutdown],
        SubscribeEventAssocId::All,
    )?;
    let listener = socket.listen(5)?;

    // A peer that sends one message and closes its socket
    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (peer, _) = peer.connect(listener.local_addr()?).await?;
    peer.send(b"hello", SendOptions::default()).await?;
    drop(peer);

    let (association, _) = listener.accept().await?;
    loop {
        match association.recv().await? {
            NotificationOrData::Notification(Notification::AssociationChange(change)) => {
                println!("association change: {:?}", change.state)
            }
            NotificationOrData::Notification(Notification::Shutdown(_)) => {
                println!("the peer shuts the association down")
            }
            NotificationOrData::Notification(other) => println!("{other:?}"),
            // An empty message is the end of the association
            NotificationOrData::Data(message) if message.payload.is_empty() => break,
            NotificationOrData::Data(message) => println!("{} octets", message.payload.len()),
        }
    }
    Ok(())
}
