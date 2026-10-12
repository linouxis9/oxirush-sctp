//! One registered descriptor and its record state, shared by the public socket roles.
use crate::{
    AssocChangeState, AssociationId, ConnState, Event, Notification, NotificationOrData,
    ReceivedData, SendInfo, SocketToAssociation, SubscribeEventAssocId,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use tokio::io::{unix::AsyncFd, Interest};

#[cfg(fuzzing)]
pub(crate) mod fuzzing;
mod notifications;
mod receive;
pub(crate) mod sys;
use receive::{PartialMessage, DEFAULT_MAX_MESSAGE_SIZE};

#[derive(Debug)]
pub(crate) struct SocketCore {
    fd: AsyncFd<OwnedFd>,
    style: SocketToAssociation,
    partial: Mutex<Option<PartialMessage>>,
    max_message_size: AtomicUsize,
    // How the association of a one-to-one socket ended, once `recv` has seen it: `RUNNING`,
    // `SHUT_DOWN`, or the error number of an association that was aborted or lost. Linux
    // reports either end once, or not at all, and `recv` repeats it.
    end: AtomicI32,
    // This side has shut the association down for writing. Linux says that such a shutdown
    // is complete in an association change notification, and in nothing else.
    write_shutdown: AtomicBool,
    // Association changes are subscribed for `recv` alone, to learn of that, and not passed
    // on to the caller, who has not asked for them.
    quiet_association_events: AtomicBool,
}

const RUNNING: i32 = 0;
const SHUT_DOWN: i32 = -1;

impl SocketCore {
    pub(crate) fn new(domain: libc::c_int, style: SocketToAssociation) -> std::io::Result<Self> {
        let fd = sys::sctp_socket_internal(domain, style.clone())?;
        // The stream, payload protocol and association of each message. Accepted and
        // peeled-off sockets inherit the setting.
        sys::request_rcvinfo_internal(fd.as_fd(), true)?;
        Self::register(fd, style)
    }

    fn register(fd: OwnedFd, style: SocketToAssociation) -> std::io::Result<Self> {
        Ok(Self {
            fd: AsyncFd::new(fd)?,
            style,
            partial: Mutex::new(None),
            max_message_size: AtomicUsize::new(DEFAULT_MAX_MESSAGE_SIZE),
            end: AtomicI32::new(RUNNING),
            write_shutdown: AtomicBool::new(false),
            quiet_association_events: AtomicBool::new(false),
        })
    }

    pub(crate) fn from_association(fd: OwnedFd) -> std::io::Result<Self> {
        // accept and peeloff return a single-association descriptor. Linux keeps SO_TYPE as
        // SOCK_SEQPACKET on peeled-off associations, so their role cannot be inferred from it.
        Self::register(fd, SocketToAssociation::OneToOne)
    }

    pub(crate) fn require_style(&self, style: SocketToAssociation) -> std::io::Result<()> {
        if self.style == style {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "operation is incompatible with the SCTP socket style",
            ))
        }
    }

    pub(crate) fn max_message_size(&self) -> usize {
        self.max_message_size.load(Ordering::Relaxed)
    }

    pub(crate) fn set_max_message_size(&self, octets: usize) {
        self.max_message_size.store(octets, Ordering::Relaxed);
    }

    pub(crate) async fn connect(
        self,
        addrs: &[SocketAddr],
    ) -> std::io::Result<(Self, AssociationId)> {
        self.require_style(SocketToAssociation::OneToOne)?;
        let assoc_id = sys::initiate_connect(self.as_fd(), addrs)?;
        loop {
            // Failure can make a socket readable when an association event is queued. Successful
            // connection leaves that event queued for the first receive.
            let mut guard = self
                .fd
                .ready(Interest::READABLE | Interest::WRITABLE)
                .await?;
            let error = sys::socket_error(self.as_fd())?;
            if error != 0 {
                return Err(std::io::Error::from_raw_os_error(error));
            }
            let status =
                sys::sctp_get_status_internal(self.as_fd(), assoc_id).map_err(|error| {
                    if error.raw_os_error() == Some(libc::EINVAL) {
                        std::io::Error::from_raw_os_error(libc::ECONNREFUSED)
                    } else {
                        error
                    }
                })?;
            if status.state == ConnState::Established {
                return Ok((self, assoc_id));
            }
            guard.clear_ready();
        }
    }

    pub(crate) async fn accept(&self) -> std::io::Result<(Self, SocketAddr)> {
        self.require_style(SocketToAssociation::OneToOne)?;
        let (fd, address) = loop {
            let mut guard = self.fd.readable().await?;
            if let Ok(result) = guard.try_io(|inner| sys::accept_once(inner.as_fd())) {
                break result?;
            }
        };
        let accepted = Self::from_association(fd)?;
        accepted.set_max_message_size(self.max_message_size());
        Ok((accepted, address))
    }

    pub(crate) fn peeloff(&self, assoc_id: AssociationId) -> std::io::Result<Self> {
        self.require_style(SocketToAssociation::OneToMany)?;
        let partial = self.partial.lock().unwrap_or_else(PoisonError::into_inner);
        if partial
            .as_ref()
            .is_some_and(|record| record.belongs_to(assoc_id))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot peel off an association during partial record delivery",
            ));
        }
        let peeled = Self::from_association(sys::sctp_peeloff_internal(self.as_fd(), assoc_id)?)?;
        peeled.set_max_message_size(self.max_message_size());
        Ok(peeled)
    }

    pub(crate) async fn recv(&self) -> std::io::Result<NotificationOrData> {
        loop {
            if let Some(end) = self.end() {
                return end;
            }
            let mut guard = self.fd.ready(Interest::READABLE | Interest::ERROR).await?;
            // Another task may have seen the end while this one waited.
            if let Some(end) = self.end() {
                return end;
            }
            let received = guard.try_io(|inner| {
                let mut partial = self.partial.lock().unwrap_or_else(PoisonError::into_inner);
                receive::receive_message(
                    &mut partial,
                    self.max_message_size(),
                    self.style == SocketToAssociation::OneToOne,
                    |buffer| sys::sctp_recvmsg_once(inner.as_fd(), buffer),
                )
            });
            if let Ok(result) = received {
                if let Some(result) = self.note_end(result) {
                    return result;
                }
            }
        }
    }

    // What `recv` returns once the association of a one-to-one socket has ended.
    fn end(&self) -> Option<std::io::Result<NotificationOrData>> {
        match self.end.load(Ordering::Acquire) {
            RUNNING => None,
            SHUT_DOWN => Some(Ok(NotificationOrData::Data(ReceivedData {
                payload: vec![],
                from: None,
                rcv_info: None,
                nxt_info: None,
            }))),
            error => Some(Err(std::io::Error::from_raw_os_error(error))),
        }
    }

    // Remembers the end of the association of a one-to-one socket when `received` is its
    // last sign. `None` for a notification that the caller has not subscribed to.
    fn note_end(
        &self,
        received: std::io::Result<NotificationOrData>,
    ) -> Option<std::io::Result<NotificationOrData>> {
        if self.style != SocketToAssociation::OneToOne {
            return Some(received);
        }
        match &received {
            Ok(NotificationOrData::Notification(Notification::AssociationChange(change))) => {
                if change.state == AssocChangeState::ShutdownComplete {
                    self.set_end(SHUT_DOWN);
                }
                if self.quiet_association_events.load(Ordering::Acquire) {
                    return None;
                }
            }
            Err(error) => {
                // The error of a socket whose association is gone is the last thing Linux
                // says of it. Another error leaves the association as it was.
                let gone = sys::sctp_get_status_internal(self.as_fd(), 0)
                    .is_err_and(|status| status.raw_os_error() == Some(libc::EINVAL));
                if let (Some(number), true) = (error.raw_os_error(), gone) {
                    self.set_end(number);
                }
            }
            Ok(_) => {}
        }
        Some(received)
    }

    fn set_end(&self, end: i32) {
        if self
            .end
            .compare_exchange(RUNNING, end, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Nothing more arrives, so nothing would wake the other tasks that wait in
            // `recv`. Shutting the read side down does: the socket is then readable for
            // good. It fails with `ENOTCONN` on a socket without an association, after it
            // has done that.
            let _ = sys::shutdown_internal(self.as_fd(), std::net::Shutdown::Read);
        }
    }

    pub(crate) fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
        if how == std::net::Shutdown::Write && self.style == SocketToAssociation::OneToOne {
            self.write_shutdown.store(true, Ordering::Release);
            if !sys::sctp_event_subscribed_internal(self.as_fd(), Event::Association)? {
                self.subscribe_association_events(true)?;
                self.quiet_association_events.store(true, Ordering::Release);
            }
        }
        sys::shutdown_internal(self.as_fd(), how)
    }

    fn subscribe_association_events(&self, on: bool) -> std::io::Result<()> {
        sys::sctp_subscribe_event_internal(
            self.as_fd(),
            Event::Association,
            SubscribeEventAssocId::All,
            on,
        )
    }

    pub(crate) fn subscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
        on: bool,
    ) -> std::io::Result<()> {
        let association = events.contains(&Event::Association);
        if association && !on && self.write_shutdown.load(Ordering::Acquire) {
            // `recv` still needs them: they stay subscribed and are no longer passed on.
            self.quiet_association_events.store(true, Ordering::Release);
            let others: Vec<Event> = events
                .iter()
                .filter(|event| **event != Event::Association)
                .cloned()
                .collect();
            return sys::sctp_subscribe_events_internal(self.as_fd(), &others, assoc_id, on);
        }
        sys::sctp_subscribe_events_internal(self.as_fd(), events, assoc_id, on)?;
        if association && on {
            self.quiet_association_events
                .store(false, Ordering::Release);
        }
        Ok(())
    }

    pub(crate) async fn send(
        &self,
        to: Option<SocketAddr>,
        payload: &[u8],
        info: Option<&SendInfo>,
    ) -> std::io::Result<()> {
        loop {
            let mut guard = self.fd.writable().await?;
            if let Ok(result) =
                guard.try_io(|inner| sys::sctp_sendmsg_once(inner.as_fd(), to, payload, info))
            {
                return result;
            }
        }
    }
}

impl AsFd for SocketCore {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for SocketCore {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn peeloff_rejects_partial_record_ownership_before_touching_the_kernel() {
        for identity in [Some(7), None] {
            let core = SocketCore::new(libc::AF_INET, SocketToAssociation::OneToMany).unwrap();
            let mut partial = None;
            let mut first = true;
            let error = receive::receive_message(&mut partial, 100, false, |buffer| {
                if !first {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                }
                first = false;
                buffer[..7].copy_from_slice(b"partial");
                Ok(receive::Piece {
                    len: 7,
                    notification: false,
                    end_of_record: false,
                    rcv_info: identity.map(|assoc_id| crate::RcvInfo {
                        assoc_id,
                        ..Default::default()
                    }),
                    nxt_info: None,
                    from: None,
                })
            })
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            *core.partial.lock().unwrap() = partial;
            let error = core.peeloff(7).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(error.raw_os_error(), None, "peeloff reached the kernel");
            assert!(core.partial.lock().unwrap().is_some());
            if identity.is_some() {
                let error = core.peeloff(8).unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
                assert!(core.partial.lock().unwrap().is_some());
            }
        }
    }
}
