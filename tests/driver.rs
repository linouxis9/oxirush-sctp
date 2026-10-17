//! The driver: one task that writes a socket in batches, one that reads it, and the overload
//! of the writer's queue, which its sojourns tell.
//!
//! The tests that carry thousands of messages have two threads, a sender's and a receiver's:
//! a receiver on the sender's thread falls behind by more than its receive buffer holds, and
//! Linux then drops packets, which wait for a retransmission timeout of a second.

use oxirush_sctp::driver::{self, DriverConfig, OnOverload, SendError};
use oxirush_sctp::{ConnectedSocket, NotificationOrData, SendOptions, Socket, SocketToAssociation};
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(10);

/// What the receivers ask for, as a peer that takes bursts of thousands of small messages
/// needs: Linux counts the memory of each message in the receive buffer, several times its
/// octets, and stops a sender that fills it until a retransmission timeout.
const RECEIVE_BUFFER: usize = 2 << 20;

async fn pair() -> (ConnectedSocket, ConnectedSocket) {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket
        .options()
        .set_recv_buffer_size(RECEIVE_BUFFER)
        .unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let (client, _) = client
        .connect(listener.local_addrs().unwrap()[0])
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

fn numbered(index: u32) -> Vec<u8> {
    let mut payload = vec![0x5a; 200];
    payload[..4].copy_from_slice(&index.to_be_bytes());
    payload
}

fn number(payload: &[u8]) -> u32 {
    u32::from_be_bytes(payload[..4].try_into().unwrap())
}

/// Messages queued by several tasks of one writer arrive whole, each task's in its order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_writer_keeps_the_order_of_what_it_queues() {
    let (client, server) = pair().await;
    let (writer, _) = driver::spawn(client, DriverConfig::default()).unwrap();
    let (_, mut reader) = driver::spawn(server, DriverConfig::default()).unwrap();
    for index in 0..10_000 {
        writer
            .send(numbered(index), SendOptions::new().ppid(60))
            .await
            .unwrap();
    }
    for index in 0..10_000 {
        match tokio::time::timeout(WAIT, reader.recv()).await.unwrap() {
            Some(Ok(NotificationOrData::Data(message))) => {
                assert_eq!(number(&message.payload), index);
                assert_eq!(message.ppid(), Some(60));
            }
            other => panic!("{other:?}"),
        }
    }
    // The writer counts a batch once the socket took it, which may be after the peer read it
    let deadline = Instant::now() + WAIT;
    while writer.overload().sent < 10_000 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let overload = writer.overload();
    assert_eq!(overload.sent, 10_000);
    assert!(!overload.overloaded);
}

/// The associations of an endpoint take turns: a message for one comes before the backlog of
/// another has gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_associations_of_an_endpoint_take_turns() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.into_endpoint(4).unwrap();
    let address = endpoint.local_addr().unwrap();
    let mut clients = Vec::new();
    let mut identities = Vec::new();
    for _ in 0..2 {
        let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        client
            .options()
            .set_recv_buffer_size(RECEIVE_BUFFER)
            .unwrap();
        let (client, _) = client.connect(address).await.unwrap();
        client.send(b"hello", SendOptions::new()).await.unwrap();
        let identity = loop {
            if let NotificationOrData::Data(message) = endpoint.recv().await.unwrap() {
                break message.assoc_id().unwrap();
            }
        };
        clients.push(client);
        identities.push(identity);
    }
    let (writer, _) = driver::spawn(endpoint, DriverConfig::default()).unwrap();
    let busy = SendOptions::new().association(identities[0]);
    for index in 0..5000 {
        writer.send(numbered(index), busy).await.unwrap();
    }
    writer
        .send(
            b"the other".to_vec(),
            SendOptions::new().association(identities[1]),
        )
        .await
        .unwrap();
    let quiet = clients.pop().unwrap();
    let busy = clients.pop().unwrap();
    let started = Instant::now();
    let busy_reader = tokio::spawn(async move {
        let mut times = Vec::new();
        for _ in 0..5000 {
            busy.recv().await.unwrap().unwrap();
            times.push(Instant::now());
        }
        times
    });
    let other = tokio::time::timeout(WAIT, quiet.recv())
        .await
        .unwrap()
        .unwrap();
    let other_at = Instant::now();
    assert!(
        matches!(other, Some(NotificationOrData::Data(message)) if message.payload == b"the other")
    );
    let times = busy_reader.await.unwrap();
    // The endpoint's own socket has Nagle's algorithm on: the driver turns it off, or each
    // batch would wait for the acknowledgement that the peer delays, and this would take 11 s.
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?} for 5000 messages",
        started.elapsed()
    );
    let before = times.iter().filter(|at| **at < other_at).count();
    assert!(
        before < 2500,
        "{before} of 5000 came before the other association's"
    );
}

/// A burst that the socket drains is no overload: the shortest sojourn of its interval is
/// that of its first messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_that_drains_is_no_overload() {
    let (client, server) = pair().await;
    let (writer, _) = driver::spawn(client, DriverConfig::default()).unwrap();
    let (_, mut reader) = driver::spawn(server, DriverConfig::default()).unwrap();
    for index in 0..3000 {
        writer
            .try_send(numbered(index), SendOptions::new())
            .unwrap();
    }
    for _ in 0..3000 {
        tokio::time::timeout(WAIT, reader.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let overload = writer.overload();
    assert!(!overload.overloaded, "{overload:?}");
    assert_eq!(overload.refused, 0);
}

/// A queue that stands, here because the peer reads nothing, is in overload once the
/// shortest sojourn of an interval is above the target: a send fails at once, and is
/// counted. Once the peer reads, the overload ends.
#[tokio::test]
async fn a_standing_queue_is_in_overload_until_it_drains() {
    let (client, server) = pair().await;
    let mut config = DriverConfig::default();
    config.target = Duration::from_millis(2);
    config.interval = Duration::from_millis(20);
    let (writer, _) = driver::spawn(client, config).unwrap();
    // Large, for the peer's receive buffer to fill soon
    let payload = vec![0x5a; 10_000];
    let started = Instant::now();
    let refused = loop {
        match writer.try_send(payload.clone(), SendOptions::new()) {
            Ok(()) => {}
            Err(SendError::Overloaded) => break started.elapsed(),
            Err(other) => panic!("{other}"),
        }
        if started.elapsed() > WAIT {
            panic!("no overload: {:?}", writer.overload());
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    };
    let overload = writer.overload();
    assert!(overload.overloaded && overload.refused == 1, "{overload:?}");
    assert!(overload.min > config.target, "{overload:?}");
    assert!(overload.queued > 0);
    eprintln!("overload after {refused:?}: {overload:?}");
    // With `Wait`, a send waits for the end of the overload: the peer reads, the queue
    // drains, and it goes.
    let (_, mut reader) = driver::spawn(server, DriverConfig::default()).unwrap();
    let draining = tokio::spawn(async move {
        while let Some(received) = reader.recv().await {
            received.unwrap();
        }
    });
    let deadline = Instant::now() + WAIT;
    while writer.overload().overloaded {
        assert!(
            Instant::now() < deadline,
            "the overload did not end: {:?}",
            writer.overload()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    writer
        .send(payload.clone(), SendOptions::new())
        .await
        .unwrap();
    draining.abort();
}

/// With `OnOverload::Wait`, a send in overload waits for its end instead of failing.
#[tokio::test]
async fn a_send_can_wait_for_the_end_of_the_overload() {
    let (client, server) = pair().await;
    let mut config = DriverConfig::default();
    config.target = Duration::from_millis(2);
    config.interval = Duration::from_millis(20);
    config.on_overload = OnOverload::Wait;
    let (writer, _) = driver::spawn(client, config).unwrap();
    let payload = vec![0x5a; 10_000];
    let deadline = Instant::now() + WAIT;
    while !writer.overload().overloaded {
        assert!(
            Instant::now() < deadline,
            "no overload: {:?}",
            writer.overload()
        );
        let _ = writer.try_send(payload.clone(), SendOptions::new());
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    let waiting = tokio::spawn({
        let writer = writer.clone();
        let payload = payload.clone();
        async move { writer.send(payload, SendOptions::new()).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "the send did not wait");
    let (_, mut reader) = driver::spawn(server, DriverConfig::default()).unwrap();
    tokio::spawn(async move { while reader.recv().await.is_some() {} });
    tokio::time::timeout(WAIT, waiting)
        .await
        .expect("the send waited past the overload")
        .unwrap()
        .unwrap();
    assert_eq!(writer.overload().refused, 0);
}

/// The queue holds its octets at most: `try_send` is refused past them, and `send` waits.
#[tokio::test]
async fn the_queue_has_a_limit_in_octets() {
    let (client, _server) = pair().await;
    let mut config = DriverConfig::default();
    config.max_queued = 10_000;
    // No overload in this test: only the limit.
    config.target = Duration::from_secs(3600);
    config.ceiling = Duration::from_secs(3600);
    let (writer, _) = driver::spawn(client, config).unwrap();
    let payload = vec![0x5a; 1000];
    let deadline = Instant::now() + WAIT;
    loop {
        match writer.try_send(payload.clone(), SendOptions::new()) {
            Ok(()) => {}
            Err(SendError::Full) => break,
            Err(other) => panic!("{other}"),
        }
        assert!(
            Instant::now() < deadline,
            "never full: {:?}",
            writer.overload()
        );
        tokio::task::yield_now().await;
    }
    assert!(writer.overload().queued_octets <= 10_000);
    let waiting = writer.send(payload, SendOptions::new());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), waiting)
            .await
            .is_err(),
        "a send did not wait for room"
    );
}

/// The end of the association: the reader ends, and the writer's sends fail.
#[tokio::test]
async fn the_end_of_the_association_ends_the_reader_and_the_writer() {
    let (client, server) = pair().await;
    let (writer, mut reader) = driver::spawn(server, DriverConfig::default()).unwrap();
    client.send(b"last", SendOptions::new()).await.unwrap();
    client.close(WAIT).await.unwrap();
    match tokio::time::timeout(WAIT, reader.recv()).await.unwrap() {
        Some(Ok(NotificationOrData::Data(message))) => assert_eq!(message.payload, b"last"),
        other => panic!("{other:?}"),
    }
    assert!(
        tokio::time::timeout(WAIT, reader.recv())
            .await
            .unwrap()
            .is_none()
    );
    let deadline = Instant::now() + WAIT;
    loop {
        match writer.send(b"late".to_vec(), SendOptions::new()).await {
            Ok(()) => {}
            Err(SendError::Closed(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
                break;
            }
            Err(other) => panic!("{other}"),
        }
        assert!(Instant::now() < deadline, "the writer never stopped");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(writer.overload().failed >= 1);
    assert!(matches!(
        writer.try_send(Vec::new(), SendOptions::new()),
        Err(SendError::Closed(_) | SendError::Empty)
    ));
}
