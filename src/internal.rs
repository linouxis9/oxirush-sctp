//! One registered descriptor and its record state, shared by the public socket roles.
use crate::{
    AssocChangeState, AssociationId, ConnState, Event, Notification, NotificationOrData,
    ReceivedInto, SendInfo, SocketToAssociation, SubscribeEventAssocId,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use tokio::io::{Interest, unix::AsyncFd};

#[cfg(fuzzing)]
pub(crate) mod fuzzing;
mod notifications;
mod receive;
pub(crate) mod sys;
use receive::{DEFAULT_MAX_MESSAGE_SIZE, PartialMessage};

#[derive(Debug)]
pub(crate) struct SocketCore {
    fd: AsyncFd<OwnedFd>,
    style: SocketToAssociation,
    partial: Mutex<Option<PartialMessage>>,
    max_message_size: AtomicUsize,
    // How the association of a one-to-one socket ended, once a call has learned of it:
    // `RUNNING`, `SHUT_DOWN`, or the error number of an association that was aborted or lost.
    // Linux reports either end once, to the call that happens to be there, or not at all:
    // `recv` and `send` both record it here, and both repeat it.
    end: AtomicI32,
    // This side has shut the association down for writing. Linux says that such a shutdown
    // is complete in an association change notification, and in nothing else.
    write_shutdown: AtomicBool,
    // This side has shut the socket down for reading: `recv` then returns the end of the
    // stream while the association lives, and `send` still sends.
    read_shutdown: AtomicBool,
    // Association changes are subscribed for `recv` alone, to learn of that, and not passed
    // on to the caller, who has not asked for them.
    quiet_association_events: AtomicBool,
}

const RUNNING: i32 = 0;
const SHUT_DOWN: i32 = -1;

impl SocketCore {
    pub(crate) fn new(domain: libc::c_int, style: SocketToAssociation) -> std::io::Result<Self> {
        let fd = sys::sctp_socket_internal(domain, style)?;
        // The stream, payload protocol and association of each message. Accepted and
        // peeled-off sockets inherit the setting.
        sys::request_rcvinfo_internal(fd.as_fd(), true)?;
        Self::register(fd, style)
    }

    fn register(fd: OwnedFd, style: SocketToAssociation) -> std::io::Result<Self> {
        // Tokio 1.53.3 deprecates `AsyncFd::new` for a value that may close its descriptor
        // while it is registered. An `OwnedFd` keeps it open until the `AsyncFd` drops it,
        // after the registration. `AsyncFd::register`, which says so, is not in the
        // versions of Tokio before it, which the crate takes.
        #[allow(deprecated)]
        let fd = AsyncFd::new(fd)?;
        Ok(Self {
            fd,
            style,
            partial: Mutex::new(None),
            max_message_size: AtomicUsize::new(DEFAULT_MAX_MESSAGE_SIZE),
            end: AtomicI32::new(RUNNING),
            write_shutdown: AtomicBool::new(false),
            read_shutdown: AtomicBool::new(false),
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

    // The next message or notification. `None` is the end of an association that was shut
    // down, which a one-to-one socket alone has.
    pub(crate) async fn recv(&self) -> std::io::Result<Option<NotificationOrData>> {
        loop {
            if let Some(end) = self.end() {
                return self.after_the_end(end);
            }
            let mut guard = self.fd.ready(Interest::READABLE | Interest::ERROR).await?;
            // Another task may have seen the end while this one waited.
            if let Some(end) = self.end() {
                return self.after_the_end(end);
            }
            let received = guard.try_io(|inner| self.receive_once(inner.as_fd()));
            if let Ok(result) = received
                && let Some(result) = self.note_received(result)
            {
                return result;
            }
        }
    }

    fn receive_once(&self, fd: BorrowedFd<'_>) -> std::io::Result<Option<NotificationOrData>> {
        let mut partial = self.partial.lock().unwrap_or_else(PoisonError::into_inner);
        receive::receive_message(
            &mut partial,
            self.max_message_size(),
            self.style == SocketToAssociation::OneToOne,
            |buffer| sys::sctp_recvmsg_once(fd, buffer),
        )
    }

    // The next message, into the buffer of the caller when one read gives it whole: no
    // allocation and no copy then. Anything else goes the way of `recv`, and a message is
    // copied to the buffer once it is whole.
    pub(crate) async fn recv_into(
        &self,
        buffer: &mut [u8],
    ) -> std::io::Result<Option<ReceivedInto>> {
        loop {
            if self.end().is_none() {
                let mut guard = self.fd.ready(Interest::READABLE | Interest::ERROR).await?;
                if self.end().is_none() {
                    // An error of `try_io` is a read that would block: it has cleared
                    // the readiness, and the loop waits again
                    match guard.try_io(|inner| self.receive_direct(inner.as_fd(), buffer)) {
                        Ok(Ok(Direct::Data(received))) => return Ok(Some(received)),
                        Ok(Ok(Direct::Assembling)) => {}
                        Ok(Ok(Direct::Other(result))) => match self.note_received(result) {
                            Some(result) => return into_buffer(result?, buffer),
                            None => continue,
                        },
                        Ok(Err(_)) | Err(_) => continue,
                    }
                }
            }
            return into_buffer(self.recv().await?, buffer);
        }
    }

    // One read into the buffer of the caller, when no message is being assembled. The
    // only error is that of a read that would block.
    fn receive_direct(&self, fd: BorrowedFd<'_>, buffer: &mut [u8]) -> std::io::Result<Direct> {
        let mut partial = self.partial.lock().unwrap_or_else(PoisonError::into_inner);
        if partial.is_some() || buffer.is_empty() {
            return Ok(Direct::Assembling);
        }
        let piece = match sys::sctp_recvmsg_once(fd, buffer) {
            Ok(piece) => piece,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                return Ok(Direct::Assembling);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Err(error),
            Err(error) => return Ok(Direct::Other(Err(error))),
        };
        if piece.len == 0 && !piece.notification {
            return Ok(Direct::Other(Ok(None)));
        }
        if piece.end_of_record && piece.notification {
            let notification = notifications::notification_from_message(&buffer[..piece.len]);
            return Ok(Direct::Other(Ok(Some(NotificationOrData::Notification(
                notification,
            )))));
        }
        if piece.end_of_record {
            let limit = self.max_message_size();
            if piece.len > limit {
                return Ok(Direct::Other(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "discarded a message of {} octets, longer than {limit}",
                        piece.len
                    ),
                ))));
            }
            return Ok(Direct::Data(ReceivedInto::Data {
                len: piece.len,
                from: piece.from,
                rcv_info: piece.rcv_info,
            }));
        }
        // The first part of a longer message: the assembler goes on from it
        *partial = Some(PartialMessage::begun(&buffer[..piece.len], &piece));
        Ok(Direct::Assembling)
    }

    // How the association of a one-to-one socket has ended, once a call has learned of it:
    // `Ok` for one that was shut down, and the error of one that was aborted or lost.
    fn end(&self) -> Option<std::io::Result<()>> {
        match self.end.load(Ordering::Acquire) {
            RUNNING => None,
            SHUT_DOWN => Some(Ok(())),
            error => Some(Err(std::io::Error::from_raw_os_error(error))),
        }
    }

    // What `recv` returns once the end is known: what the kernel still holds of the
    // association, which a `send` may have learned the end of before it was read, and then
    // the end itself, at each call. Nothing more arrives, so this does not wait.
    fn after_the_end(
        &self,
        end: std::io::Result<()>,
    ) -> std::io::Result<Option<NotificationOrData>> {
        loop {
            match self.receive_once(self.as_fd()) {
                Ok(Some(NotificationOrData::Notification(Notification::AssociationChange(_))))
                    if self.quiet_association_events.load(Ordering::Acquire) => {}
                Ok(Some(received)) => return Ok(Some(received)),
                Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                    // A message longer than the limit was discarded: the caller is told
                    return Err(error);
                }
                Ok(None) | Err(_) => return end.map(|()| None),
            }
        }
    }

    // Remembers the end of the association of a one-to-one socket when `received` is a sign
    // of it. `None` for a notification that the caller has not subscribed to.
    fn note_received(
        &self,
        received: std::io::Result<Option<NotificationOrData>>,
    ) -> Option<std::io::Result<Option<NotificationOrData>>> {
        if self.style != SocketToAssociation::OneToOne {
            return Some(received);
        }
        match &received {
            Ok(Some(NotificationOrData::Notification(Notification::AssociationChange(change)))) => {
                match change.state {
                    AssocChangeState::ShutdownComplete => self.set_end(SHUT_DOWN),
                    // The error of the socket says whether the peer aborted or was lost. A
                    // `send` that has taken it has recorded the end already.
                    AssocChangeState::CommLost => self.set_end(
                        sys::socket_error(self.as_fd())
                            .ok()
                            .filter(|error| *error != 0)
                            .unwrap_or(libc::ECONNRESET),
                    ),
                    _ => {}
                }
                if self.quiet_association_events.load(Ordering::Acquire) {
                    return None;
                }
            }
            // The end of the stream: the peer shut the association down, unless this side
            // has only stopped reading
            Ok(None) => {
                if !self.read_shutdown.load(Ordering::Acquire) {
                    self.set_end(SHUT_DOWN);
                }
                // Another call may have recorded the end first, and shut the socket down
                // for reading to wake this one: the end that was recorded is the answer
                if let Some(end) = self.end() {
                    return Some(end.map(|()| None));
                }
            }
            Err(error) => {
                self.note_error(error);
                if error.raw_os_error().is_some()
                    && let Some(end) = self.end()
                {
                    return Some(end.map(|()| None));
                }
            }
            Ok(Some(_)) => {}
        }
        Some(received)
    }

    // Remembers the end of the association of a one-to-one socket when a call failed with
    // `error` and the association is gone: that error is the last thing Linux says of it, to
    // this call alone. Another error leaves the association as it was.
    fn note_error(&self, error: &std::io::Error) {
        if self.style != SocketToAssociation::OneToOne {
            return;
        }
        let gone = sys::sctp_get_status_internal(self.as_fd(), 0)
            .is_err_and(|status| status.raw_os_error() == Some(libc::EINVAL));
        if let (Some(number), true) = (error.raw_os_error(), gone) {
            // A broken pipe is what a send gets of an association that was shut down:
            // one that was aborted or lost gives its own error first
            self.set_end(if number == libc::EPIPE {
                SHUT_DOWN
            } else {
                number
            });
        }
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
        if how != std::net::Shutdown::Write {
            self.read_shutdown.store(true, Ordering::Release);
        }
        if how != std::net::Shutdown::Read && self.style == SocketToAssociation::OneToOne {
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
        // SCTP has no empty message, and Linux answers a bare `EINVAL`. A send that only
        // aborts an association or shuts it down has none on purpose.
        let controls = info.is_some_and(|info| {
            info.flags & (crate::consts::SCTP_ABORT | crate::consts::SCTP_EOF) != 0
        });
        if payload.is_empty() && !controls {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a message has one octet at least: SCTP has no empty message",
            ));
        }
        loop {
            // After the end, the same answer at each call: the error of an association that
            // was aborted or lost, and a broken pipe for one that was shut down.
            if let Some(end) = self.end() {
                return Err(end
                    .err()
                    .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE)));
            }
            let mut guard = self.fd.writable().await?;
            if let Ok(result) =
                guard.try_io(|inner| sys::sctp_sendmsg_once(inner.as_fd(), to, payload, info))
            {
                if let Err(error) = &result {
                    // This call may be the one that Linux tells of the end
                    self.note_error(error);
                    if let Some(end) = self.end() {
                        return Err(end
                            .err()
                            .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE)));
                    }
                    if let Some(stream) = self.stream_out_of_range(error, info) {
                        return Err(stream);
                    }
                }
                return result;
            }
        }
    }
}

// What one read into the buffer of a caller gave.
enum Direct {
    // A whole message, in the buffer.
    Data(ReceivedInto),
    // The end of the stream, an error or a notification, for `note_received`.
    Other(std::io::Result<Option<NotificationOrData>>),
    // Nothing for the caller yet: a message is being assembled.
    Assembling,
}

// What `recv` returned, with a message copied to the buffer of a caller.
fn into_buffer(
    received: Option<NotificationOrData>,
    buffer: &mut [u8],
) -> std::io::Result<Option<ReceivedInto>> {
    Ok(match received {
        None => None,
        Some(NotificationOrData::Notification(notification)) => {
            Some(ReceivedInto::Notification(notification))
        }
        Some(NotificationOrData::Data(message)) => {
            let len = message.payload.len();
            let Some(room) = buffer.get_mut(..len) else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "discarded a message of {len} octets, longer than the buffer of {}",
                        buffer.len()
                    ),
                ));
            };
            room.copy_from_slice(&message.payload);
            Some(ReceivedInto::Data {
                len,
                from: message.from,
                rcv_info: message.rcv_info,
            })
        }
    })
}

impl SocketCore {
    // The `EINVAL` of a send on a stream that the association does not have, said in words.
    fn stream_out_of_range(
        &self,
        error: &std::io::Error,
        info: Option<&SendInfo>,
    ) -> Option<std::io::Error> {
        let info = info.filter(|_| error.raw_os_error() == Some(libc::EINVAL))?;
        let status = sys::sctp_get_status_internal(self.as_fd(), info.assoc_id).ok()?;
        (info.sid >= status.outstreams).then(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "stream {} is not one of the {} outbound streams of the association",
                    info.sid, status.outstreams
                ),
            )
        })
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
