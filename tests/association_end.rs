#![cfg(target_os = "linux")]
//! How the end of an association reaches `recv` on a `ConnectedSocket`, and that it stays
//! there: no call waits for an association that is gone.

use oxirush_sctp::*;
use std::net::Shutdown;
use std::sync::Arc;
use std::time::Duration;

async fn pair() -> (ConnectedSocket, ConnectedSocket) {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = client
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (client, peer)
}

async fn recv(socket: &ConnectedSocket) -> std::io::Result<NotificationOrData> {
    tokio::time::timeout(Duration::from_secs(2), socket.recv())
        .await
        .expect("receive waited for an association that has ended")
}

fn is_end(received: &std::io::Result<NotificationOrData>) -> bool {
    matches!(received, Ok(NotificationOrData::Data(message)) if message.payload.is_empty())
}

#[tokio::test]
async fn a_write_shutdown_ends_receive_when_it_is_complete() {
    let (client, peer) = pair().await;
    client.shutdown(Shutdown::Write).unwrap();
    // The peer learns of it and its side ends too.
    assert!(is_end(&recv(&peer).await));
    for _ in 0..3 {
        assert!(is_end(&recv(&client).await));
    }
}

#[tokio::test]
async fn what_the_peer_sent_before_the_shutdown_is_received_first() {
    let (client, peer) = pair().await;
    peer.send(b"last words", SendOptions::default())
        .await
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let received = recv(&client).await.unwrap();
    assert!(
        matches!(&received, NotificationOrData::Data(message) if message.payload == b"last words"),
        "{received:?}"
    );
    assert!(is_end(&recv(&client).await));
}

#[tokio::test]
async fn a_write_shutdown_with_association_events_ends_after_its_notification() {
    let (client, _peer) = pair().await;
    client
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let received = recv(&client).await.unwrap();
    assert!(
        matches!(
            &received,
            NotificationOrData::Notification(Notification::AssociationChange(change))
                if change.state == AssocChangeState::ShutdownComplete
        ),
        "{received:?}"
    );
    for _ in 0..2 {
        assert!(is_end(&recv(&client).await));
    }
}

#[tokio::test]
async fn association_events_unsubscribed_after_a_write_shutdown_are_not_passed_on() {
    let (client, _peer) = pair().await;
    let events = [Event::Association];
    client
        .options()
        .subscribe_events(&events, SubscribeEventAssocId::All)
        .unwrap();
    // Hold the shutdown back: the peer has to answer it before it completes.
    client.shutdown(Shutdown::Write).unwrap();
    client
        .options()
        .unsubscribe_events(&events, SubscribeEventAssocId::All)
        .unwrap();
    // Either the notification was already queued when the events were unsubscribed, or
    // `recv` reads it without passing it on: in both cases the end follows.
    let mut received = recv(&client).await;
    if matches!(received, Ok(NotificationOrData::Notification(_))) {
        received = recv(&client).await;
    }
    assert!(is_end(&received), "{received:?}");
}

#[tokio::test]
async fn an_aborted_association_is_an_error_on_every_receive() {
    let (client, peer) = pair().await;
    client.options().set_linger(Some(Duration::ZERO)).unwrap();
    drop(client);
    for _ in 0..3 {
        let error = recv(&peer).await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ECONNRESET));
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }
    // Sending says so too.
    let error = peer
        .send(b"late", SendOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EPIPE));
}

#[tokio::test]
async fn an_aborted_association_with_events_is_a_notification_then_an_error() {
    let (client, peer) = pair().await;
    peer.options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    client.options().set_linger(Some(Duration::ZERO)).unwrap();
    drop(client);
    let received = recv(&peer).await.unwrap();
    assert!(
        matches!(
            &received,
            NotificationOrData::Notification(Notification::AssociationChange(change))
                if change.state == AssocChangeState::CommLost
        ),
        "{received:?}"
    );
    for _ in 0..2 {
        let error = recv(&peer).await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ECONNRESET));
    }
}

#[tokio::test]
async fn every_waiting_receiver_learns_of_the_end() {
    // After a shutdown of this side, and after an abort by the peer.
    for abort in [false, true] {
        let (client, peer) = pair().await;
        let client = Arc::new(client);
        let waiting: Vec<_> = (0..3)
            .map(|_| {
                let client = client.clone();
                tokio::spawn(async move { recv(&client).await })
            })
            .collect();
        // Let the three tasks wait before the association ends.
        tokio::time::sleep(Duration::from_millis(50)).await;
        if abort {
            peer.options().set_linger(Some(Duration::ZERO)).unwrap();
            drop(peer);
        } else {
            client.shutdown(Shutdown::Write).unwrap();
        }
        for task in waiting {
            let received = task.await.unwrap();
            if abort {
                assert_eq!(received.unwrap_err().raw_os_error(), Some(libc::ECONNRESET));
            } else {
                assert!(is_end(&received), "{received:?}");
            }
        }
    }
}

#[tokio::test]
async fn a_shutdown_by_the_peer_ends_receive_on_every_call() {
    let (client, peer) = pair().await;
    drop(client);
    for _ in 0..3 {
        assert!(is_end(&recv(&peer).await));
    }
}
