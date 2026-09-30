#![cfg(target_os = "linux")]

use oxirush_sctp::{Socket, SocketToAssociation};

struct ErrnoLogger;
static LOGGER: ErrnoLogger = ErrnoLogger;

impl log::Log for ErrnoLogger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if record.level() == log::Level::Error {
            // Simulate a logger whose output operation changes this thread's errno.
            // Safety: Linux returns a valid pointer to the calling thread's errno.
            unsafe { *libc::__errno_location() = libc::ENOSPC };
        }
    }

    fn flush(&self) {}
}

#[tokio::test]
async fn syscall_errors_are_preserved_when_the_logger_changes_errno() {
    log::set_logger(&LOGGER).unwrap();
    let invalid_address = "[::1]:0".parse().unwrap();
    log::set_max_level(log::LevelFilter::Off);
    let baseline = Socket::new_v4(SocketToAssociation::OneToOne)
        .unwrap()
        .bind(invalid_address)
        .unwrap_err();
    log::set_max_level(log::LevelFilter::Error);
    let logged = Socket::new_v4(SocketToAssociation::OneToOne)
        .unwrap()
        .bind(invalid_address)
        .unwrap_err();
    assert_eq!(baseline.raw_os_error(), Some(libc::EINVAL));
    assert_eq!(logged.raw_os_error(), baseline.raw_os_error());

    let socket = Socket::new_v4(SocketToAssociation::OneToMany).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    log::set_max_level(log::LevelFilter::Off);
    let baseline = listener.sctp_peeloff(0).unwrap_err();
    log::set_max_level(log::LevelFilter::Error);
    let logged = listener.sctp_peeloff(0).unwrap_err();
    assert_eq!(logged.raw_os_error(), baseline.raw_os_error());
}
