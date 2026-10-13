#![cfg(target_os = "linux")]

use oxirush_sctp::{SendOptions, Socket, SocketToAssociation};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::span;

// The error events that the subscriber has seen.
static ERRORS: AtomicUsize = AtomicUsize::new(0);
static EVENTS: AtomicUsize = AtomicUsize::new(0);

// A subscriber whose output operation changes this thread's errno, as one that writes to a
// full disk would.
struct ErrnoSubscriber;

impl tracing::Subscriber for ErrnoSubscriber {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::ERROR {
            ERRORS.fetch_add(1, Ordering::Relaxed);
        }
        EVENTS.fetch_add(1, Ordering::Relaxed);
        // Safety: Linux returns a valid pointer to the calling thread's errno.
        unsafe { *libc::__errno_location() = libc::ENOSPC };
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

/// A call that fails returns the error of the kernel whatever a subscriber does to `errno`
/// meanwhile, and the library does not log at ERROR what it returns: the caller decides
/// what an error is worth.
#[tokio::test]
async fn syscall_errors_are_preserved_and_are_not_logged_as_errors() {
    // An address that this host does not have
    let foreign_address = "192.0.2.1:0".parse().unwrap();
    let bind = || {
        Socket::new_v4(SocketToAssociation::OneToOne)
            .unwrap()
            .bind(foreign_address)
            .unwrap_err()
    };
    let baseline = bind();
    let traced = tracing::subscriber::with_default(ErrnoSubscriber, bind);
    assert_eq!(baseline.raw_os_error(), Some(libc::EADDRNOTAVAIL));
    assert_eq!(traced.raw_os_error(), baseline.raw_os_error());

    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.into_endpoint(1).unwrap();
    let baseline = listener.peeloff(0).unwrap_err();
    let traced =
        tracing::subscriber::with_default(ErrnoSubscriber, || listener.peeloff(0).unwrap_err());
    assert_eq!(traced.raw_os_error(), baseline.raw_os_error());

    // Both failures went through the subscriber, and neither as an error.
    assert!(EVENTS.load(Ordering::Relaxed) >= 2);
    assert_eq!(ERRORS.load(Ordering::Relaxed), 0);
}

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
    let (client, _) = client
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
