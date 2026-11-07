#![cfg(target_os = "linux")]

use oxirush_sctp::{SendOptions, Socket, SocketToAssociation};
/// What Linux refuses of an argument with a bare `EINVAL` is said in words.
#[tokio::test]
async fn a_refused_argument_is_named() {
    let message = |error: std::io::Error| {
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{error}");
        error.to_string()
    };
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let error = socket.bind("[::1]:0".parse().unwrap()).unwrap_err();
    assert!(message(error).contains("IPv6 address"));

    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let addresses = [
        "127.0.0.1:4000".parse().unwrap(),
        "127.0.0.2:4001".parse().unwrap(),
    ];
    let error = socket.connectx(&addresses).await.unwrap_err();
    assert!(message(error).contains("share a port"));

    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    let client = client
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let error = client.send(b"", SendOptions::new()).await.unwrap_err();
    assert!(message(error).contains("no empty message"));
    let streams = client.status().unwrap().outstreams;
    let error = client
        .send(b"data", SendOptions::new().stream(streams))
        .await
        .unwrap_err();
    assert!(message(error).contains("outbound streams"));
    let error = client
        .send(b"data", SendOptions::new().association(12345))
        .await
        .unwrap_err();
    assert!(message(error).contains("one association"));
}
