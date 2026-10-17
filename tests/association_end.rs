#![cfg(target_os = "linux")]
//! How an association ends on a `ConnectedSocket`, whichever call learns of it first, and
//! what every later call returns: one test for each row of the table of the documentation.
//! No call waits for an association that is gone.

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

type Received = std::io::Result<Option<NotificationOrData>>;

async fn recv(socket: &ConnectedSocket) -> Received {
    tokio::time::timeout(Duration::from_secs(2), socket.recv())
        .await
        .expect("receive waited for an association that has ended")
}

async fn send(socket: &ConnectedSocket) -> std::io::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(2),
        socket.send(b"late", SendOptions::new()),
    )
    .await
    .expect("send waited for an association that has ended")
}

fn is_end(received: &Received) -> bool {
    matches!(received, Ok(None))
}

fn error_number(received: Received) -> Option<i32> {
    received.unwrap_err().raw_os_error()
}

fn is_change(received: &Received, state: AssocChangeState) -> bool {
    matches!(
        received,
        Ok(Some(NotificationOrData::Notification(Notification::AssociationChange(change))))
            if change.state == state
    )
}

/// Send until a send fails: the error that Linux gives the call that is there.
async fn send_until_it_fails(socket: &ConnectedSocket) -> std::io::Error {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Err(error) = socket.send(b"downlink", SendOptions::new().ppid(60)).await {
                return error;
            }
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    })
    .await
    .expect("a send to an association that has ended fails")
}

/// The peer shut the association down, and `recv` is the first to learn of it: `None` at
/// each call, and a broken pipe for a send.
#[tokio::test]
async fn a_shutdown_by_the_peer_is_the_end_for_receive_and_a_broken_pipe_for_send() {
    let (client, peer) = pair().await;
    drop(client);
    for _ in 0..3 {
        assert!(is_end(&recv(&peer).await));
    }
    for _ in 0..2 {
        assert_eq!(
            send(&peer).await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }
    assert!(is_end(&recv(&peer).await));
}

/// The peer shut the association down, and a send is the first to learn of it: `recv`
/// still gives what the peer had sent, then `None`, and does not wait.
#[tokio::test]
async fn a_shutdown_by_the_peer_that_a_send_sees_first_is_the_end_for_receive() {
    let (client, peer) = pair().await;
    client
        .send(b"last words", SendOptions::new())
        .await
        .unwrap();
    drop(client);
    let error = send_until_it_fails(&peer).await;
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe, "{error}");
    let received = recv(&peer).await;
    assert!(
        matches!(&received, Ok(Some(NotificationOrData::Data(message))) if message.payload == b"last words"),
        "{received:?}"
    );
    for _ in 0..3 {
        assert!(is_end(&recv(&peer).await));
    }
    assert_eq!(
        send(&peer).await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
}

/// The peer aborted, and `recv` is the first to learn of it: the same error at each call
/// of `recv` and of `send`.
#[tokio::test]
async fn an_abort_by_the_peer_is_the_same_error_on_every_call() {
    let (client, peer) = pair().await;
    client.options().set_linger(Some(Duration::ZERO)).unwrap();
    drop(client);
    for _ in 0..3 {
        let error = recv(&peer).await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ECONNRESET));
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }
    for _ in 0..2 {
        assert_eq!(
            send(&peer).await.unwrap_err().raw_os_error(),
            Some(libc::ECONNRESET)
        );
    }
}

/// The peer aborted, and a send is the first to learn of it, which takes the error that
/// Linux has for the socket: `recv` returns that error, at once and at each call. It
/// waited forever.
#[tokio::test]
async fn an_abort_that_a_send_sees_first_is_the_same_error_for_receive() {
    let (client, peer) = pair().await;
    client.options().set_linger(Some(Duration::ZERO)).unwrap();
    drop(client);
    let error = send_until_it_fails(&peer).await;
    assert_eq!(error.raw_os_error(), Some(libc::ECONNRESET), "{error}");
    for _ in 0..3 {
        assert_eq!(error_number(recv(&peer).await), Some(libc::ECONNRESET));
    }
    assert_eq!(
        send(&peer).await.unwrap_err().raw_os_error(),
        Some(libc::ECONNRESET)
    );
}

/// The same with association events: the notification of the loss, which `recv` still
/// returns after a send has learned of the end, and then the error.
#[tokio::test]
async fn an_abort_with_association_events_is_a_notification_then_the_error() {
    for send_first in [false, true] {
        let (client, peer) = pair().await;
        peer.options()
            .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
            .unwrap();
        client.options().set_linger(Some(Duration::ZERO)).unwrap();
        drop(client);
        if send_first {
            send_until_it_fails(&peer).await;
        }
        let received = recv(&peer).await;
        assert!(
            is_change(&received, AssocChangeState::CommLost),
            "{received:?}"
        );
        if let Ok(Some(NotificationOrData::Notification(Notification::AssociationChange(change)))) =
            received
        {
            // A user-initiated abort
            assert_eq!(change.cause, 12);
        }
        for _ in 0..2 {
            assert_eq!(error_number(recv(&peer).await), Some(libc::ECONNRESET));
        }
    }
}

/// The receive loop of a server and a task that sends, as the review ran them: the peer
/// aborts while the receiving task is busy, or while it waits in `recv`, with association
/// events or without. The receive loop ends each time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_receive_loop_ends_after_an_abort_that_the_sending_task_saw() {
    for (subscribed, busy) in [(false, false), (true, false), (false, true), (true, true)] {
        for trial in 0..20_u64 {
            let (app, peer) = pair().await;
            if subscribed {
                app.options()
                    .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
                    .unwrap();
            }
            let app = Arc::new(app);
            let receiver = {
                let app = app.clone();
                tokio::spawn(async move {
                    if busy {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                    }
                    loop {
                        match app.recv().await {
                            Err(error) => return Some(error.kind()),
                            Ok(None) => return None,
                            Ok(Some(_)) => {}
                        }
                    }
                })
            };
            let sender = {
                let app = app.clone();
                tokio::spawn(async move {
                    loop {
                        let options = SendOptions::new().ppid(60);
                        if let Err(error) = app.send(b"downlink NAS transport", options).await {
                            return error.raw_os_error();
                        }
                        tokio::time::sleep(Duration::from_micros(20)).await;
                    }
                })
            };
            tokio::time::sleep(Duration::from_micros(500 + (trial * 37) % 1500)).await;
            // The peer never read what it was sent: a zero linger aborts.
            peer.options().set_linger(Some(Duration::ZERO)).unwrap();
            drop(peer);
            let sent = tokio::time::timeout(Duration::from_secs(2), sender)
                .await
                .expect("the sending task learns of the abort")
                .unwrap();
            assert_eq!(sent, Some(libc::ECONNRESET));
            let ended = tokio::time::timeout(Duration::from_secs(2), receiver)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "the receive loop still waits 2 s after the abort (events: \
                         {subscribed}, busy: {busy}, trial {trial})"
                    )
                })
                .unwrap();
            assert_eq!(ended, Some(std::io::ErrorKind::ConnectionReset));
        }
    }
}

/// The peer was dropped with a message that it had not read: Linux aborts the association
/// then, and this side sees an abort.
#[tokio::test]
async fn a_peer_dropped_with_an_unread_message_is_an_abort() {
    let (client, peer) = pair().await;
    peer.send(b"never read", SendOptions::new()).await.unwrap();
    // Let the message arrive before its reader goes.
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(client);
    for _ in 0..2 {
        assert_eq!(error_number(recv(&peer).await), Some(libc::ECONNRESET));
    }
}

/// This side shut the association down for writing: `recv` returns what the peer had
/// still sent, then `None` once the peer has answered, and a send is a broken pipe.
#[tokio::test]
async fn a_write_shutdown_ends_receive_when_it_is_complete() {
    let (client, peer) = pair().await;
    peer.send(b"last words", SendOptions::new()).await.unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    assert_eq!(
        send(&client).await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    let received = recv(&client).await;
    assert!(
        matches!(&received, Ok(Some(NotificationOrData::Data(message))) if message.payload == b"last words"),
        "{received:?}"
    );
    // The peer learns of it and its side ends too.
    assert!(is_end(&recv(&peer).await));
    for _ in 0..3 {
        assert!(is_end(&recv(&client).await));
    }
}

#[tokio::test]
async fn a_write_shutdown_with_association_events_ends_after_its_notification() {
    let (client, _peer) = pair().await;
    client
        .options()
        .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let received = recv(&client).await;
    assert!(
        is_change(&received, AssocChangeState::ShutdownComplete),
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
    if matches!(received, Ok(Some(NotificationOrData::Notification(_)))) {
        received = recv(&client).await;
    }
    assert!(is_end(&received), "{received:?}");
}

/// This side shut the socket down for reading alone: `recv` returns `None` at once and at
/// each call, the association lives, and a send still reaches the peer.
#[tokio::test]
async fn a_read_shutdown_ends_receive_and_leaves_send() {
    let (client, peer) = pair().await;
    client.shutdown(Shutdown::Read).unwrap();
    for _ in 0..3 {
        assert!(is_end(&recv(&client).await));
    }
    client
        .send(b"still here", SendOptions::new())
        .await
        .unwrap();
    let received = recv(&peer).await;
    assert!(
        matches!(&received, Ok(Some(NotificationOrData::Data(message))) if message.payload == b"still here"),
        "{received:?}"
    );
}

/// This side shut both directions down: `None` for `recv`, a broken pipe for `send`, and
/// the end for the peer.
#[tokio::test]
async fn a_shutdown_of_both_directions_ends_both() {
    let (client, peer) = pair().await;
    client.shutdown(Shutdown::Both).unwrap();
    for _ in 0..3 {
        assert!(is_end(&recv(&client).await));
    }
    assert_eq!(
        send(&client).await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert!(is_end(&recv(&peer).await));
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
                assert_eq!(error_number(received), Some(libc::ECONNRESET));
            } else {
                assert!(is_end(&received), "{received:?}");
            }
        }
    }
}

/// A connection that the peer refuses is the error of `connect`, which has consumed the
/// socket: nothing is left to call.
#[tokio::test]
async fn a_refused_connect_is_its_error() {
    // A port that nothing listens on: bound, then closed.
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let error = client.connect(address).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ECONNREFUSED), "{error}");
}

/// The loop of the documentation of `Listener`, on an association whose peer closes: it
/// ends, where a loop on `Ok` span once the end was a message without octets.
#[tokio::test]
async fn the_receive_loop_of_the_documentation_ends_when_the_peer_closes() {
    let (client, association) = pair().await;
    client.send(b"hello", SendOptions::new()).await.unwrap();
    drop(client);
    let mut calls = 0;
    let looped = tokio::time::timeout(Duration::from_millis(500), async {
        while let Some(_received) = association.recv().await? {
            calls += 1;
        }
        std::io::Result::Ok(())
    })
    .await;
    assert!(matches!(looped, Ok(Ok(()))), "{looped:?}");
    assert_eq!(calls, 1);
}

/// A path that fails: every packet to the peer is lost, and the association is given up
/// after its retransmission limit. `recv` returns the error at each call.
#[tokio::test]
#[ignore]
async fn a_lost_peer_in_network_namespace_is_a_time_out() {
    if std::env::var_os("SCTP_RS_TEST_IN_NETNS").is_none() {
        return;
    }
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let options = client.options();
    // Retransmissions after 100 ms, and an association that is lost after two of them.
    let mut rto = RtoInfo::default();
    (rto.initial, rto.max, rto.min) = (100, 100, 100);
    options.set_rto_info(rto).unwrap();
    let mut path = PeerAddressParams::new(0, "0.0.0.0:0".parse().unwrap());
    path.path_max_retrans = 2;
    options.set_peer_address_params(path).unwrap();
    let association = AssociationParams {
        max_retransmissions: 2,
        ..Default::default()
    };
    options.set_association_params(association).unwrap();
    assert_eq!(
        options.association_params(0).unwrap().max_retransmissions,
        2
    );
    let (client, _) = client
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (_peer, _) = listener.accept().await.unwrap();

    // Nothing goes through the loopback interface any more.
    let down = std::process::Command::new("ip")
        .args(["link", "set", "lo", "down"])
        .status()
        .unwrap();
    assert!(down.success());
    client
        .send(b"into the void", SendOptions::new())
        .await
        .unwrap();
    let lost = tokio::time::timeout(Duration::from_secs(10), client.recv())
        .await
        .expect("the association is given up after its retransmission limit");
    assert_eq!(error_number(lost), Some(libc::ETIMEDOUT));
    for _ in 0..2 {
        assert_eq!(error_number(recv(&client).await), Some(libc::ETIMEDOUT));
    }
    assert_eq!(
        send(&client).await.unwrap_err().raw_os_error(),
        Some(libc::ETIMEDOUT)
    );
}

#[test]
fn a_lost_peer_is_a_time_out_on_every_call() {
    let status = std::process::Command::new("unshare")
        .args(["--user", "--map-root-user", "--net", "--", "sh", "-c"])
        .arg("ip link set lo up && exec \"$0\" \"$@\"")
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_lost_peer_in_network_namespace_is_a_time_out",
            "--ignored",
        ])
        .env("SCTP_RS_TEST_IN_NETNS", "1")
        .status();
    match status.map(|status| status.code()) {
        Ok(Some(0)) => {}
        // The test harness exits with 101 when a test fails.
        Ok(Some(101)) => panic!("`a_lost_peer_in_network_namespace_is_a_time_out` failed"),
        other => eprintln!("no network namespace with `ip` ({other:?}); skipping"),
    }
}

/// `close` loses nothing of what was sent, where a drop with an unread message aborts
/// and loses it: the arrangement of the review, forty messages and one that was not read.
#[tokio::test]
async fn close_delivers_what_a_drop_with_an_unread_message_loses() {
    async fn received_by_the_peer(close: bool) -> (usize, Received) {
        let (server, peer) = pair().await;
        peer.send(
            b"a request that the server does not read",
            SendOptions::new(),
        )
        .await
        .unwrap();
        for _ in 0..40 {
            server
                .send(&[0x5a; 1000], SendOptions::new())
                .await
                .unwrap();
        }
        if close {
            let unread = server.close(Duration::from_secs(2));
            // The peer reads to the end, which lets the shutdown complete
            let (unread, received) = tokio::join!(unread, count(&peer));
            assert_eq!(unread.unwrap().len(), 1);
            received
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(server);
            count(&peer).await
        }
    }
    async fn count(peer: &ConnectedSocket) -> (usize, Received) {
        let mut messages = 0;
        loop {
            match recv(peer).await {
                Ok(Some(NotificationOrData::Data(_))) => messages += 1,
                Ok(Some(_)) => {}
                end => return (messages, end),
            }
        }
    }

    let (messages, end) = received_by_the_peer(true).await;
    assert_eq!(messages, 40);
    assert!(is_end(&end), "{end:?}");

    let (messages, end) = received_by_the_peer(false).await;
    assert!(messages < 40, "{messages} of 40 after an abort");
    assert_eq!(error_number(end), Some(libc::ECONNRESET));
}

/// A peer that keeps its socket and reads nothing does not hold `close` back: SCTP answers
/// the shutdown by itself.
#[tokio::test]
async fn close_completes_when_the_peer_reads_nothing() {
    let (server, peer) = pair().await;
    let closed = server.close(Duration::from_secs(2)).await;
    assert!(closed.is_ok(), "{closed:?}");
    drop(peer);
}
