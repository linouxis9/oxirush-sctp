//! File descriptor ownership of the socket types.
//!
//! These checks count the process's open file descriptors, so they live in their own test binary
//! and in a single test function: no other test may open or close descriptors meanwhile.

use oxirush_sctp::{Socket, SocketToAssociation};
use std::future::Future;
use std::net::SocketAddr;
use std::task::Poll;
use std::time::Duration;

fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[tokio::test]
async fn sockets_close_their_descriptor_whatever_happens_to_them() {
    // Let the runtime open whatever it opens lazily before counting.
    tokio::task::yield_now().await;
    let before = open_fds();

    // Dropped before any use.
    for _ in 0..10 {
        drop(Socket::new_v4(SocketToAssociation::OneToOne).unwrap());
        drop(Socket::new_v6(SocketToAssociation::OneToMany).unwrap());
    }
    assert_eq!(
        open_fds(),
        before,
        "a dropped `Socket` leaks its descriptor"
    );

    // Dropped after a failed `bind`.
    for _ in 0..10 {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        let v6: SocketAddr = "[::1]:0".parse().unwrap();
        assert!(socket.bind(v6).is_err());
    }
    assert_eq!(open_fds(), before, "a failed `bind` leaks the descriptor");

    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(10).unwrap();
    let address = listener.local_addrs(0).unwrap()[0];
    let with_listener = open_fds();

    // A connect dropped before it completes, as `tokio::time::timeout` does.
    let mut cancelled = 0;
    for _ in 0..10 {
        let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        let addresses = [address];
        let mut connect = Box::pin(client.connectx(&addresses));
        let polled = std::future::poll_fn(|cx| Poll::Ready(connect.as_mut().poll(cx))).await;
        if polled.is_pending() {
            cancelled += 1;
        }
        drop(connect);
        while let Ok(Ok((accepted, _))) =
            tokio::time::timeout(Duration::from_millis(50), listener.accept()).await
        {
            drop(accepted);
        }
    }
    assert!(cancelled > 0, "no connect was cancelled");
    assert_eq!(
        open_fds(),
        with_listener,
        "a cancelled `connectx` leaks the descriptor"
    );

    // A refused connect.
    for _ in 0..10 {
        let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        let refused: SocketAddr = "127.0.0.1:9".parse().unwrap();
        assert!(client.connect(refused).await.is_err());
    }
    assert_eq!(
        open_fds(),
        with_listener,
        "a refused connect leaks the descriptor"
    );

    // Connected and accepted sockets.
    for _ in 0..10 {
        let client = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        let (connected, _) = client.connectx(&[address]).await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        drop(connected);
        drop(accepted);
    }
    assert_eq!(
        open_fds(),
        with_listener,
        "connected sockets leak descriptors"
    );

    drop(listener);
    assert_eq!(
        open_fds(),
        before,
        "a dropped `Listener` leaks its descriptor"
    );
}
