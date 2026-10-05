//! A server sets its options, accepts several associations, prints the status of each and
//! tells from notifications how it ends: its peer shut it down, or aborted it.
//!
//! ```text
//! cargo run --example server
//! ```

use oxirush_sctp::{
    AssocChangeState, ConnectedSocket, Event, Listener, Notification, NotificationOrData,
    SendOptions, Socket, SocketToAssociation, SubscribeEventAssocId,
};
use std::net::SocketAddr;
use std::time::Duration;

async fn serve(association: ConnectedSocket, peer: SocketAddr) -> std::io::Result<()> {
    let status = association.status(0)?;
    println!(
        "{peer}: {:?}, {} outgoing and {} incoming streams, nodelay {}",
        status.state,
        status.outstreams,
        status.instreams,
        association.options().nodelay()?
    );
    loop {
        match association.recv().await {
            // An empty message is the end of an association that was shut down.
            Ok(NotificationOrData::Data(message)) if message.payload.is_empty() => break,
            Ok(NotificationOrData::Data(message)) => {
                println!("{peer}: {:?}", String::from_utf8_lossy(&message.payload));
                association.send(b"welcome", SendOptions::default()).await?;
            }
            Ok(NotificationOrData::Notification(Notification::Shutdown(_))) => {
                println!("{peer}: the peer shuts the association down")
            }
            Ok(NotificationOrData::Notification(Notification::AssociationChange(change))) => {
                match change.state {
                    AssocChangeState::CommLost => println!(
                        "{peer}: association lost, error cause {}",
                        u16::from_be(change.error)
                    ),
                    state => println!("{peer}: {state:?}"),
                }
            }
            Ok(NotificationOrData::Notification(other)) => println!("{peer}: {other:?}"),
            // An aborted association ends with an error.
            Err(error) => {
                println!("{peer}: {error}");
                break;
            }
        }
    }
    Ok(())
}

async fn server(listener: Listener) -> std::io::Result<()> {
    let mut associations = Vec::new();
    for _ in 0..2 {
        let (association, peer) = listener.accept().await?;
        associations.push(tokio::spawn(serve(association, peer)));
    }
    for association in associations {
        association.await??;
    }
    Ok(())
}

async fn peers(server: SocketAddr) -> std::io::Result<()> {
    for abort in [false, true] {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let (association, _) = socket.connect(server).await?;
        association.send(b"hello", SendOptions::default()).await?;
        association.recv().await?;
        if abort {
            // Dropped with a zero linger, the socket aborts its association.
            association.options().set_linger(Some(Duration::ZERO))?;
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    // The associations accepted later inherit these options.
    let options = socket.options();
    options.set_nodelay(true)?; // send small messages at once
    options.set_init_params(4, 4, 0, 0)?; // four streams each way
    options.subscribe_events(
        &[Event::Association, Event::Shutdown],
        SubscribeEventAssocId::All,
    )?;
    let listener = socket.listen(5)?;
    let address = listener.local_addr()?;
    let (server, peers) = tokio::join!(server(listener), peers(address));
    server.and(peers)
}
