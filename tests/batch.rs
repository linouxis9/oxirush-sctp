//! Messages sent and received several at a time: `send_many` with `sendmmsg`, `recv_many` with
//! `recvmmsg`, on a connected socket and on an endpoint.

use oxirush_sctp::{
    ConnectedSocket, NotificationOrData, OneToManyEndpoint, ReceivedData, SendOptions, Socket,
    SocketToAssociation,
};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);

/// Two ends of an association on loopback, the sending one without delay.
async fn pair() -> (ConnectedSocket, ConnectedSocket) {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    client.options().set_nodelay(true).unwrap();
    let (client, _) = client
        .connect(listener.local_addrs().unwrap()[0])
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

/// The messages of `socket` until `count` of them, by as many `recv_many` as it takes, with
/// the number of calls.
async fn received(socket: &ConnectedSocket, count: usize) -> (Vec<ReceivedData>, usize) {
    let mut messages = Vec::new();
    let mut calls = 0;
    while messages.len() < count {
        let mut out = Vec::new();
        let got = tokio::time::timeout(WAIT, socket.recv_many(&mut out, 64))
            .await
            .expect("the messages come")
            .unwrap();
        assert_eq!(got, out.len());
        assert!(got > 0, "the association ended after {}", messages.len());
        calls += 1;
        for item in out {
            match item {
                NotificationOrData::Data(message) => messages.push(message),
                other => panic!("{other:?}"),
            }
        }
    }
    (messages, calls)
}

/// The payload of message `index`, of `octets` octets.
fn payload(index: usize, octets: usize) -> Vec<u8> {
    let mut payload = vec![(index % 251) as u8; octets];
    payload[..8].copy_from_slice(&(index as u64).to_be_bytes());
    payload
}

/// A batch is sent and received whole and in order, each message with its stream and payload
/// protocol identifier.
#[tokio::test]
async fn a_batch_keeps_each_message_with_its_stream_and_its_order() {
    let (client, server) = pair().await;
    let payloads: Vec<Vec<u8>> = (0..300).map(|index| payload(index, 200)).collect();
    let messages: Vec<(&[u8], SendOptions)> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            let options = SendOptions::new()
                .stream((index % 3) as u16)
                .ppid(60 + (index % 2) as u32);
            (payload.as_slice(), options)
        })
        .collect();
    let mut sent = 0;
    while sent < messages.len() {
        sent += client.send_many(&messages[sent..]).await.unwrap();
    }
    let (got, calls) = received(&server, messages.len()).await;
    // The messages of a stream come in their order.
    for stream in 0..3 {
        let of_stream: Vec<u64> = got
            .iter()
            .filter(|message| message.stream_id() == Some(stream))
            .map(|message| u64::from_be_bytes(message.payload[..8].try_into().unwrap()))
            .collect();
        let expected: Vec<u64> = (0..300).filter(|i| i % 3 == stream as u64).collect();
        assert_eq!(of_stream, expected);
    }
    for message in &got {
        let index = u64::from_be_bytes(message.payload[..8].try_into().unwrap()) as usize;
        assert_eq!(message.payload, payloads[index]);
        assert_eq!(message.ppid(), Some(60 + (index % 2) as u32));
    }
    assert!(calls < messages.len(), "one message a call: {calls}");
}

/// A send buffer that a batch does not fit in takes a prefix of it: the number sent says
/// where the next call starts, and nothing is lost or repeated.
#[tokio::test]
async fn a_full_send_buffer_takes_a_prefix_of_the_batch() {
    let (client, server) = pair().await;
    client.options().set_send_buffer_size(16 << 10).unwrap();
    let payloads: Vec<Vec<u8>> = (0..400).map(|index| payload(index, 1000)).collect();
    let messages: Vec<(&[u8], SendOptions)> = payloads
        .iter()
        .map(|payload| (payload.as_slice(), SendOptions::new()))
        .collect();
    let reader = tokio::spawn(async move { received(&server, 400).await.0 });
    let mut sent = 0;
    let mut calls = 0;
    while sent < messages.len() {
        let count = tokio::time::timeout(WAIT, client.send_many(&messages[sent..]))
            .await
            .expect("the buffer gets room")
            .unwrap();
        assert!(count > 0);
        sent += count;
        calls += 1;
    }
    assert!(calls > 1, "a buffer of 16 KiB took 400 KB at once");
    let got = reader.await.unwrap();
    for (index, message) in got.iter().enumerate() {
        assert_eq!(message.payload, payloads[index]);
    }
}

/// What a batch sent leaves without a later call, when its messages would fill the send
/// buffer: the kernel holds back a message sent with `MSG_MORE` until the next send, so the
/// last that a call sends must not have it, and those before it must leave room for it.
#[tokio::test]
async fn a_batch_that_fills_the_send_buffer_holds_nothing_back() {
    for octets in [100, 1000, 3000] {
        let (client, server) = pair().await;
        // The kernel doubles it, to 8 KiB: a few messages held back fill it
        client.options().set_send_buffer_size(4096).unwrap();
        let payloads: Vec<Vec<u8>> = (0..64).map(|index| payload(index, octets)).collect();
        let messages: Vec<(&[u8], SendOptions)> = payloads
            .iter()
            .map(|payload| (payload.as_slice(), SendOptions::new()))
            .collect();
        let sent = client.send_many(&messages).await.unwrap();
        let mut got = 0;
        while got < sent {
            let mut out = Vec::new();
            match tokio::time::timeout(Duration::from_secs(2), server.recv_many(&mut out, 64)).await
            {
                Ok(result) => got += result.unwrap(),
                Err(_) => panic!("{got} of the {sent} messages of {octets} octets came"),
            }
        }
        assert_eq!(got, sent);
    }
}

/// A message to a stream that the association does not have is refused, and the batch ends
/// before it: those before it are on their way, none held back for it.
#[tokio::test]
async fn a_message_to_a_stream_that_the_association_lacks_ends_the_batch_before_it() {
    let (client, server) = pair().await;
    let streams = client.status().unwrap().outstreams;
    let payloads: Vec<Vec<u8>> = (0..3).map(|index| payload(index, 200)).collect();
    let messages: Vec<(&[u8], SendOptions)> = vec![
        (&payloads[0], SendOptions::new()),
        (&payloads[1], SendOptions::new().stream(1)),
        (&payloads[2], SendOptions::new().stream(streams)),
    ];
    assert_eq!(client.send_many(&messages).await.unwrap(), 2);
    let mut got = 0;
    while got < 2 {
        let mut out = Vec::new();
        match tokio::time::timeout(Duration::from_secs(2), server.recv_many(&mut out, 64)).await {
            Ok(result) => got += result.unwrap(),
            Err(_) => panic!("{got} of the 2 messages before the refused one came"),
        }
    }
    let error = client.send_many(&messages[2..]).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("outbound streams"), "{error}");
}

/// A message longer than a slot of the batch comes in parts that the next slots or the next
/// calls receive: it is assembled, between messages that each fit.
#[tokio::test]
async fn a_message_longer_than_a_slot_is_assembled_between_short_ones() {
    let (client, server) = pair().await;
    let lengths = [100, 5000, 100, 70_000, 300, 4096, 4097, 64];
    let payloads: Vec<Vec<u8>> = lengths
        .iter()
        .enumerate()
        .map(|(index, octets)| payload(index, *octets))
        .collect();
    let messages: Vec<(&[u8], SendOptions)> = payloads
        .iter()
        .map(|payload| (payload.as_slice(), SendOptions::new()))
        .collect();
    let reader = tokio::spawn(async move { received(&server, lengths.len()).await.0 });
    let mut sent = 0;
    while sent < messages.len() {
        sent += client.send_many(&messages[sent..]).await.unwrap();
    }
    let got = reader.await.unwrap();
    let got: Vec<&[u8]> = got
        .iter()
        .map(|message| message.payload.as_slice())
        .collect();
    let expected: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
    assert_eq!(got, expected);
}

/// With nothing to read, `recv_many` waits; `limit` 0 returns at once.
#[tokio::test]
async fn recv_many_waits_for_a_first_message() {
    let (client, server) = pair().await;
    let mut out = Vec::new();
    assert_eq!(server.recv_many(&mut out, 0).await.unwrap(), 0);
    let waiting = tokio::time::timeout(Duration::from_millis(100), server.recv_many(&mut out, 8));
    assert!(
        waiting.await.is_err(),
        "recv_many returned with nothing to read"
    );
    client.send(b"one", SendOptions::new()).await.unwrap();
    let got = tokio::time::timeout(WAIT, server.recv_many(&mut out, 8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, 1);
    assert!(matches!(&out[0], NotificationOrData::Data(message) if message.payload == b"one"));
    // An empty message is refused, and an empty batch sends nothing.
    let error = client
        .send_many(&[(b"x", SendOptions::new()), (b"", SendOptions::new())])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(client.send_many(&[]).await.unwrap(), 0);
}

/// The end of the association in a batch: the messages before it, then 0, at each call.
#[tokio::test]
async fn the_end_of_the_association_comes_after_its_last_messages() {
    let (client, server) = pair().await;
    let messages = [
        (b"first".as_slice(), SendOptions::new()),
        (b"second".as_slice(), SendOptions::new()),
        (b"third".as_slice(), SendOptions::new()),
    ];
    assert_eq!(client.send_many(&messages).await.unwrap(), 3);
    client.close(WAIT).await.unwrap();
    let mut out = Vec::new();
    loop {
        let got = tokio::time::timeout(WAIT, server.recv_many(&mut out, 64))
            .await
            .unwrap()
            .unwrap();
        if got == 0 {
            break;
        }
    }
    let payloads: Vec<&[u8]> = out
        .iter()
        .map(|item| match item {
            NotificationOrData::Data(message) => message.payload.as_slice(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(payloads, [b"first".as_slice(), b"second", b"third"]);
    let mut more = Vec::new();
    assert_eq!(server.recv_many(&mut more, 64).await.unwrap(), 0);
    assert!(more.is_empty());
    let error = server
        .send_many(&[(b"late", SendOptions::new())])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
}

/// A message over the receive limit is discarded, and the others of its batch delivered: its
/// error comes with the next call.
#[tokio::test]
async fn a_message_over_the_limit_in_a_batch_is_reported_after_the_others() {
    let (client, server) = pair().await;
    server.options().set_max_message_size(1000);
    let long = vec![7; 2000];
    let messages = [
        (b"before".as_slice(), SendOptions::new()),
        (long.as_slice(), SendOptions::new()),
        (b"after".as_slice(), SendOptions::new()),
    ];
    assert_eq!(client.send_many(&messages).await.unwrap(), 3);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut out = Vec::new();
    let mut errors = 0;
    while out.len() < 2 {
        match tokio::time::timeout(WAIT, server.recv_many(&mut out, 64))
            .await
            .unwrap()
        {
            Ok(got) => assert!(got > 0),
            Err(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
                errors += 1;
            }
        }
    }
    if errors == 0 {
        let error = server.recv_many(&mut out, 64).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
    let payloads: Vec<&[u8]> = out
        .iter()
        .map(|item| match item {
            NotificationOrData::Data(message) => message.payload.as_slice(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(payloads, [b"before".as_slice(), b"after"]);
}

/// An endpoint sends a batch to several associations, in their turns, and receives the
/// messages of all of them in batches, each with its association.
#[tokio::test]
async fn an_endpoint_sends_and_receives_the_batches_of_several_associations() {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint: OneToManyEndpoint = socket.into_endpoint(16).unwrap();
    let address = endpoint.local_addr().unwrap();
    let mut clients = Vec::new();
    for _ in 0..4 {
        let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        client.options().set_nodelay(true).unwrap();
        let (client, _) = client.connect(address).await.unwrap();
        client.send(b"hello", SendOptions::new()).await.unwrap();
        clients.push(client);
    }
    // The endpoint learns the associations from their first messages.
    let mut associations = Vec::new();
    let mut out = Vec::new();
    while associations.len() < 4 {
        out.clear();
        tokio::time::timeout(WAIT, endpoint.recv_many(&mut out, 64))
            .await
            .unwrap()
            .unwrap();
        for item in &out {
            if let NotificationOrData::Data(message) = item {
                associations.push(message.assoc_id().unwrap());
            }
        }
    }
    // Ten messages to each, in runs and in turns.
    let payloads: Vec<(usize, Vec<u8>)> = (0..40)
        .map(|index| {
            let association = if index < 20 { index / 5 % 4 } else { index % 4 };
            (association, payload(index, 150))
        })
        .collect();
    let messages: Vec<(&[u8], SendOptions)> = payloads
        .iter()
        .map(|(association, payload)| {
            let options = SendOptions::new().association(associations[*association]);
            (payload.as_slice(), options)
        })
        .collect();
    let mut sent = 0;
    while sent < messages.len() {
        sent += endpoint.send_many(&messages[sent..]).await.unwrap();
    }
    for (association, client) in clients.iter().enumerate() {
        let (got, _) = received(client, 10).await;
        let expected: Vec<&[u8]> = payloads
            .iter()
            .filter(|(of, _)| *of == association)
            .map(|(_, payload)| payload.as_slice())
            .collect();
        let got: Vec<&[u8]> = got
            .iter()
            .map(|message| message.payload.as_slice())
            .collect();
        assert_eq!(got, expected, "association {association}");
    }
    // Each message needs its association.
    let error = endpoint
        .send_many(&[(b"nowhere", SendOptions::new())])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
