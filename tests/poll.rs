#![cfg(target_os = "linux")]
//! The `poll_*` methods: what a `Stream` or a future of a program's own builds on. They wait
//! for the socket to be readable or writable, and end and fail as the async methods do.

use oxirush_sctp::*;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(2);

async fn listener() -> (Listener, SocketAddr) {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(4).unwrap();
    let address = listener.local_addr().unwrap();
    (listener, address)
}

async fn pair() -> (ConnectedSocket, ConnectedSocket) {
    let (listener, address) = listener().await;
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = client.connect(address).await.unwrap();
    let (peer, _) = poll_fn(|cx| listener.poll_accept(cx)).await.unwrap();
    (client, peer)
}

/// Receive through `poll_recv` until the end, and return how it ended.
async fn until_the_end(socket: &ConnectedSocket) -> Option<std::io::ErrorKind> {
    loop {
        match poll_fn(|cx| socket.poll_recv(cx)).await {
            Err(error) => return Some(error.kind()),
            Ok(None) => return None,
            Ok(Some(_)) => {}
        }
    }
}

#[tokio::test]
async fn messages_go_both_ways_through_the_polls() {
    let (client, peer) = pair().await;
    let options = SendOptions::new().stream(1).ppid(60);
    poll_fn(|cx| client.poll_send(cx, b"request", options))
        .await
        .unwrap();
    let Some(NotificationOrData::Data(message)) = poll_fn(|cx| peer.poll_recv(cx)).await.unwrap()
    else {
        panic!("not data");
    };
    assert_eq!(
        (message.payload.as_slice(), message.ppid()),
        (&b"request"[..], Some(60))
    );
    poll_fn(|cx| peer.poll_send(cx, b"answer", options))
        .await
        .unwrap();
    let mut buffer = [0; 64];
    let received = poll_fn(|cx| client.poll_recv_into(cx, &mut buffer))
        .await
        .unwrap();
    let Some(ReceivedInto::Data { len, .. }) = received else {
        panic!("{received:?}");
    };
    assert_eq!(&buffer[..len], b"answer");
    // A connected socket has one association.
    let elsewhere = SendOptions::new().association(7);
    let error = poll_fn(|cx| client.poll_send(cx, b"x", elsewhere))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    // The peer shuts down: the end, at each call.
    peer.shutdown(std::net::Shutdown::Both).unwrap();
    let ended = tokio::time::timeout(WAIT, until_the_end(&client)).await;
    assert_eq!(ended.expect("the end comes"), None);
    let received = poll_fn(|cx| client.poll_recv_into(cx, &mut buffer)).await;
    assert!(matches!(received, Ok(None)), "{received:?}");
}

/// Linux reports an abort as an error alone, which leaves a socket that is not readable:
/// a caller that waits in `poll_recv` for it to be readable learns of the end because the
/// crate subscribes to the association changes, which make it readable, for itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_recv_learns_of_an_abort_that_nothing_else_saw() {
    for subscribed in [false, true] {
        let (app, peer) = pair().await;
        if subscribed {
            app.options()
                .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
                .unwrap();
        }
        let app = Arc::new(app);
        let receiver = tokio::spawn({
            let app = app.clone();
            async move { until_the_end(&app).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        peer.options().set_linger(Some(Duration::ZERO)).unwrap();
        drop(peer);
        let ended = tokio::time::timeout(WAIT, receiver)
            .await
            .unwrap_or_else(|_| panic!("poll_recv still waits 2 s after the abort ({subscribed})"))
            .unwrap();
        assert_eq!(ended, Some(std::io::ErrorKind::ConnectionReset));
        // And at each call.
        let again = poll_fn(|cx| app.poll_recv(cx)).await.unwrap_err();
        assert_eq!(again.kind(), std::io::ErrorKind::ConnectionReset);
    }
}

/// The receive loop of a server and a task that sends, through the polls: the peer aborts
/// while the receiving task is busy or waits, with association events or without.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_receive_loop_of_polls_ends_after_an_abort_that_the_sending_task_saw() {
    for (subscribed, busy) in [(false, false), (true, false), (false, true), (true, true)] {
        for trial in 0..10_u64 {
            let (app, peer) = pair().await;
            if subscribed {
                app.options()
                    .subscribe_events(&[Event::Association], SubscribeEventAssocId::All)
                    .unwrap();
            }
            let app = Arc::new(app);
            let receiver = tokio::spawn({
                let app = app.clone();
                async move {
                    if busy {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                    }
                    until_the_end(&app).await
                }
            });
            let sender = tokio::spawn({
                let app = app.clone();
                async move {
                    loop {
                        let options = SendOptions::new().ppid(60);
                        if let Err(error) =
                            poll_fn(|cx| app.poll_send(cx, b"downlink", options)).await
                        {
                            return error.raw_os_error();
                        }
                        tokio::time::sleep(Duration::from_micros(20)).await;
                    }
                }
            });
            tokio::time::sleep(Duration::from_micros(500 + (trial * 37) % 1500)).await;
            peer.options().set_linger(Some(Duration::ZERO)).unwrap();
            drop(peer);
            let sent = tokio::time::timeout(WAIT, sender)
                .await
                .expect("the sending task learns of the abort")
                .unwrap();
            assert_eq!(sent, Some(libc::ECONNRESET));
            let ended = tokio::time::timeout(WAIT, receiver)
                .await
                .unwrap_or_else(|_| {
                    panic!("the loop still waits (events: {subscribed}, busy: {busy}, {trial})")
                })
                .unwrap();
            assert_eq!(ended, Some(std::io::ErrorKind::ConnectionReset));
        }
    }
}

/// The association changes that the crate subscribes to for itself reach no caller who has
/// not subscribed to them.
#[tokio::test]
async fn the_association_changes_of_the_crate_are_not_passed_on() {
    let (client, peer) = pair().await;
    poll_fn(|cx| client.poll_send(cx, b"first", SendOptions::new()))
        .await
        .unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(WAIT, poll_fn(|cx| peer.poll_recv(cx)))
            .await
            .unwrap()
            .unwrap()
        {
            None => break,
            Some(message) => received.push(message),
        }
    }
    assert_eq!(received.len(), 1, "{received:?}");
    assert!(
        matches!(&received[0], NotificationOrData::Data(message) if message.payload == b"first")
    );
}

#[tokio::test]
async fn an_endpoint_sends_and_receives_through_the_polls() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(4).unwrap();
    let (listener, address) = listener().await;
    let options = SendOptions::new().ppid(18);
    poll_fn(|cx| endpoint.poll_send_to(cx, address, b"hello", options))
        .await
        .unwrap();
    let (peer, _) = poll_fn(|cx| listener.poll_accept(cx)).await.unwrap();
    let Some(NotificationOrData::Data(hello)) = peer.recv().await.unwrap() else {
        panic!("not data");
    };
    assert_eq!(hello.payload, b"hello");
    peer.send(b"answer", SendOptions::new()).await.unwrap();
    let NotificationOrData::Data(answer) = poll_fn(|cx| endpoint.poll_recv(cx)).await.unwrap()
    else {
        panic!("not data");
    };
    assert_eq!(answer.payload, b"answer");
    let association = answer.assoc_id().unwrap();
    poll_fn(|cx| endpoint.poll_send(cx, b"again", options.association(association)))
        .await
        .unwrap();
    peer.send(b"last", SendOptions::new()).await.unwrap();
    let mut buffer = [0; 64];
    let received = poll_fn(|cx| endpoint.poll_recv_into(cx, &mut buffer))
        .await
        .unwrap();
    let ReceivedInto::Data { len, .. } = received else {
        panic!("{received:?}");
    };
    assert_eq!(&buffer[..len], b"last");
    let Some(NotificationOrData::Data(again)) = peer.recv().await.unwrap() else {
        panic!("not data");
    };
    assert_eq!(again.payload, b"again");
    // A send of an endpoint names its association.
    let error = poll_fn(|cx| endpoint.poll_send(cx, b"x", SendOptions::new()))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
