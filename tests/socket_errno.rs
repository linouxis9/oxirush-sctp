#![cfg(target_os = "linux")]

use oxirush_sctp::{Socket, SocketToAssociation};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::span;

// The error events that the subscriber has seen.
static ERRORS: AtomicUsize = AtomicUsize::new(0);

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
            // Safety: Linux returns a valid pointer to the calling thread's errno.
            unsafe { *libc::__errno_location() = libc::ENOSPC };
        }
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

#[tokio::test]
async fn syscall_errors_are_preserved_when_the_subscriber_changes_errno() {
    let invalid_address = "[::1]:0".parse().unwrap();
    let bind = || {
        Socket::new_v4(SocketToAssociation::OneToOne)
            .unwrap()
            .bind(invalid_address)
            .unwrap_err()
    };
    let baseline = bind();
    let traced = tracing::subscriber::with_default(ErrnoSubscriber, bind);
    assert_eq!(baseline.raw_os_error(), Some(libc::EINVAL));
    assert_eq!(traced.raw_os_error(), baseline.raw_os_error());

    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.into_endpoint(1).unwrap();
    let baseline = listener.peeloff(0).unwrap_err();
    let traced =
        tracing::subscriber::with_default(ErrnoSubscriber, || listener.peeloff(0).unwrap_err());
    assert_eq!(traced.raw_os_error(), baseline.raw_os_error());

    // Both failures went through the subscriber.
    assert_eq!(ERRORS.load(Ordering::Relaxed), 2);
}
