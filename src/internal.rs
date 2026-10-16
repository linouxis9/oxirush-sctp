//! One registered descriptor and its record state, shared by the public socket roles.
use crate::{
    AssocChangeState, AssociationId, ConnState, Event, Notification, NotificationOrData,
    ReceivedInto, SendInfo, SocketToAssociation, SubscribeEventAssocId,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll, ready};
use tokio::io::{Interest, unix::AsyncFd};

#[cfg(fuzzing)]
pub(crate) mod fuzzing;
mod notifications;
mod receive;
pub(crate) mod sys;
use receive::{DEFAULT_MAX_MESSAGE_SIZE, PartialMessage, Partials};

#[derive(Debug)]
pub(crate) struct SocketCore {
    fd: AsyncFd<OwnedFd>,
    style: SocketToAssociation,
    partials: Mutex<Partials>,
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
    // The events that are subscribed for the crate alone and not passed on to the caller, who
    // has not asked for them, one bit an event: association changes for `recv` to learn of
    // the end of an association after a shutdown, partial deliveries given up for an
    // endpoint to drop the parts that it has of a message.
    quiet_events: AtomicU32,
    // The association came up while its changes were the crate's alone: the notification of
    // that, which comes first, is not passed on to a caller who subscribes to them later.
    quiet_comm_up: AtomicBool,
}

// The bit of `event` among the quiet events.
fn bit(event: Event) -> u32 {
    1 << (event as u16 - Event::DataIo as u16)
}

const RUNNING: i32 = 0;
const SHUT_DOWN: i32 = -1;

impl SocketCore {
    pub(crate) fn new(domain: libc::c_int, style: SocketToAssociation) -> std::io::Result<Self> {
        let fd = sys::sctp_socket_internal(domain, style)?;
        // The stream, payload protocol and association of each message. Accepted and
        // peeled-off sockets inherit the setting.
        sys::request_rcvinfo_internal(fd.as_fd(), true)?;
        let core = Self::register(fd, style)?;
        match style {
            SocketToAssociation::OneToMany => core.interleave(true)?,
            // Listeners, the associations that they accept and those that connect.
            SocketToAssociation::OneToOne => core.hear_of_the_end()?,
        }
        Ok(core)
    }

    // On a one-to-one socket, the association changes, which make the socket readable when
    // the association ends: Linux reports an abort as an error alone otherwise, which wakes
    // `recv` and not a caller of `poll_recv`, who waits for the socket to be readable.
    fn hear_of_the_end(&self) -> std::io::Result<()> {
        if !sys::sctp_event_subscribed_internal(self.as_fd(), Event::Association)? {
            self.subscribe_quietly(Event::Association)?;
        }
        Ok(())
    }

    // On an endpoint, whether the parts of the messages of several associations come in
    // turns, which their receive information tells apart: a long message of one association
    // then holds back none of the others. The parts of a message whose delivery is given up
    // are dropped with the notification of it, which is subscribed for that.
    fn interleave(&self, on: bool) -> std::io::Result<()> {
        sys::set_fragment_interleave_internal(self.as_fd(), on)?;
        if on && !sys::sctp_event_subscribed_internal(self.as_fd(), Event::PartialDelivery)? {
            self.subscribe_quietly(Event::PartialDelivery)?;
        }
        self.partials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_interleaved(on);
        Ok(())
    }

    // Receive the receive information of each message, or not: an endpoint without it cannot
    // tell the parts of messages of several associations apart, which then come one message
    // after the other.
    pub(crate) fn request_rcvinfo(&self, on: bool) -> std::io::Result<()> {
        sys::request_rcvinfo_internal(self.as_fd(), on)?;
        if self.style == SocketToAssociation::OneToMany {
            self.interleave(on)?;
        }
        Ok(())
    }

    fn subscribe_quietly(&self, event: Event) -> std::io::Result<()> {
        sys::sctp_subscribe_event_internal(self.as_fd(), event, SubscribeEventAssocId::All, true)?;
        self.quiet_events.fetch_or(bit(event), Ordering::AcqRel);
        Ok(())
    }

    // Whether `notification` is not for the caller: of an event that the caller has not
    // subscribed to, or the notification that the association came up, which came while it
    // had not.
    fn quiet(&self, notification: &Notification) -> bool {
        if let Notification::AssociationChange(change) = notification
            && change.state == AssocChangeState::CommUp
            && self.quiet_comm_up.swap(false, Ordering::AcqRel)
        {
            return true;
        }
        let event = match notification {
            Notification::AssociationChange(_) => Event::Association,
            Notification::PartialDelivery(_) => Event::PartialDelivery,
            _ => return false,
        };
        self.quiet_events.load(Ordering::Acquire) & bit(event) != 0
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
            partials: Mutex::new(Partials::default()),
            max_message_size: AtomicUsize::new(DEFAULT_MAX_MESSAGE_SIZE),
            end: AtomicI32::new(RUNNING),
            write_shutdown: AtomicBool::new(false),
            read_shutdown: AtomicBool::new(false),
            quiet_events: AtomicU32::new(0),
            quiet_comm_up: AtomicBool::new(false),
        })
    }

    pub(crate) fn from_association(fd: OwnedFd) -> std::io::Result<Self> {
        // accept and peeloff return a single-association descriptor. Linux keeps SO_TYPE as
        // SOCK_SEQPACKET on peeled-off associations, so their role cannot be inferred from it.
        Self::register(fd, SocketToAssociation::OneToOne)
    }

    // A descriptor of one association that the crate did not make.
    pub(crate) fn import(fd: OwnedFd) -> std::io::Result<Self> {
        let core = Self::from_association(fd)?;
        core.hear_of_the_end()?;
        Ok(core)
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
                self.came_up();
                return Ok((self, assoc_id));
            }
            guard.clear_ready();
        }
    }

    pub(crate) async fn accept(&self) -> std::io::Result<(Self, SocketAddr)> {
        std::future::poll_fn(|cx| self.poll_accept(cx)).await
    }

    pub(crate) fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<(Self, SocketAddr)>> {
        self.require_style(SocketToAssociation::OneToOne)?;
        let (fd, address) = loop {
            let mut guard = ready!(self.fd.poll_read_ready(cx))?;
            if let Ok(result) = guard.try_io(|inner| sys::accept_once(inner.as_fd())) {
                break result?;
            }
        };
        Poll::Ready(self.accepted(fd).map(|accepted| (accepted, address)))
    }

    fn accepted(&self, fd: OwnedFd) -> std::io::Result<Self> {
        let accepted = Self::from_association(fd)?;
        accepted.set_max_message_size(self.max_message_size());
        // It has the subscriptions of this socket.
        accepted
            .quiet_events
            .store(self.quiet_events.load(Ordering::Acquire), Ordering::Release);
        accepted.came_up();
        Ok(accepted)
    }

    // The association of a one-to-one socket is up.
    fn came_up(&self) {
        let quiet = self.quiet_events.load(Ordering::Acquire) & bit(Event::Association) != 0;
        self.quiet_comm_up.store(quiet, Ordering::Release);
    }

    pub(crate) fn peeloff(&self, assoc_id: AssociationId) -> std::io::Result<Self> {
        self.require_style(SocketToAssociation::OneToMany)?;
        let partials = self.partials.lock().unwrap_or_else(PoisonError::into_inner);
        if partials.hold(assoc_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot peel off an association during partial record delivery",
            ));
        }
        let peeled = Self::from_association(sys::sctp_peeloff_internal(self.as_fd(), assoc_id)?)?;
        peeled.set_max_message_size(self.max_message_size());
        // It has the subscriptions of the endpoint, and those of a one-to-one socket.
        peeled
            .quiet_events
            .store(self.quiet_events.load(Ordering::Acquire), Ordering::Release);
        peeled.hear_of_the_end()?;
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

    // `recv` as a poll: it waits for the socket to be readable, which the end of an
    // association of a one-to-one socket makes it, through its association changes.
    pub(crate) fn poll_recv(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<Option<NotificationOrData>>> {
        loop {
            if let Some(end) = self.end() {
                return Poll::Ready(self.after_the_end(end));
            }
            let mut guard = ready!(self.fd.poll_read_ready(cx))?;
            if let Some(end) = self.end() {
                return Poll::Ready(self.after_the_end(end));
            }
            let received = guard.try_io(|inner| self.receive_once(inner.as_fd()));
            if let Ok(result) = received
                && let Some(result) = self.note_received(result)
            {
                return Poll::Ready(result);
            }
        }
    }

    // `recv_into` as a poll.
    pub(crate) fn poll_recv_into(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<std::io::Result<Option<ReceivedInto>>> {
        loop {
            if self.end().is_none() {
                let mut guard = ready!(self.fd.poll_read_ready(cx))?;
                if self.end().is_none() {
                    match guard.try_io(|inner| self.receive_direct(inner.as_fd(), buffer)) {
                        Ok(Ok(Direct::Data(received))) => return Poll::Ready(Ok(Some(received))),
                        Ok(Ok(Direct::Assembling)) => {}
                        Ok(Ok(Direct::Other(result))) => match self.note_received(result) {
                            Some(result) => {
                                return Poll::Ready(result.and_then(|r| into_buffer(r, buffer)));
                            }
                            None => continue,
                        },
                        Ok(Err(_)) | Err(_) => continue,
                    }
                }
            }
            return self
                .poll_recv(cx)
                .map(|result| result.and_then(|received| into_buffer(received, buffer)));
        }
    }

    fn receive_once(&self, fd: BorrowedFd<'_>) -> std::io::Result<Option<NotificationOrData>> {
        let mut partials = self.partials.lock().unwrap_or_else(PoisonError::into_inner);
        receive::receive_message(
            &mut partials,
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
        let mut partials = self.partials.lock().unwrap_or_else(PoisonError::into_inner);
        if !partials.is_empty() || buffer.is_empty() {
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
        partials.begin(PartialMessage::begun(&buffer[..piece.len], &piece));
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
                Ok(Some(NotificationOrData::Notification(notification)))
                    if self.quiet(&notification) => {}
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
            if let Ok(Some(NotificationOrData::Notification(notification))) = &received
                && self.quiet(notification)
            {
                return None;
            }
            return Some(received);
        }
        match &received {
            Ok(Some(NotificationOrData::Notification(
                notification @ Notification::AssociationChange(change),
            ))) => {
                let quiet = self.quiet(notification);
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
                if quiet {
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
            Ok(Some(NotificationOrData::Notification(notification)))
                if self.quiet(notification) =>
            {
                return None;
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
                self.subscribe_quietly(Event::Association)?;
            }
        }
        sys::shutdown_internal(self.as_fd(), how)
    }

    pub(crate) fn subscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
        on: bool,
    ) -> std::io::Result<()> {
        if on {
            sys::sctp_subscribe_events_internal(self.as_fd(), events, assoc_id, on)?;
            for event in events {
                self.quiet_events.fetch_and(!bit(*event), Ordering::AcqRel);
            }
            return Ok(());
        }
        // What the crate still needs stays subscribed, and is no longer passed on.
        let needed = |event: &Event| match event {
            Event::Association => self.style == SocketToAssociation::OneToOne,
            Event::PartialDelivery => self
                .partials
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .interleaved(),
            _ => false,
        };
        let (kept, others): (Vec<Event>, Vec<Event>) =
            events.iter().partition(|event| needed(event));
        for event in kept {
            self.quiet_events.fetch_or(bit(event), Ordering::AcqRel);
        }
        sys::sctp_subscribe_events_internal(self.as_fd(), &others, assoc_id, on)
    }

    pub(crate) async fn send(
        &self,
        to: Option<SocketAddr>,
        payload: &[u8],
        info: Option<&SendInfo>,
    ) -> std::io::Result<()> {
        std::future::poll_fn(|cx| self.poll_send(cx, to, payload, info)).await
    }

    pub(crate) fn poll_send(
        &self,
        cx: &mut Context<'_>,
        to: Option<SocketAddr>,
        payload: &[u8],
        info: Option<&SendInfo>,
    ) -> Poll<std::io::Result<()>> {
        // SCTP has no empty message, and Linux answers a bare `EINVAL`. A send that only
        // aborts an association or shuts it down has none on purpose.
        let controls = info.is_some_and(|info| {
            info.flags & (crate::consts::SCTP_ABORT | crate::consts::SCTP_EOF) != 0
        });
        if payload.is_empty() && !controls {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a message has one octet at least: SCTP has no empty message",
            )));
        }
        loop {
            // After the end, the same answer at each call: the error of an association that
            // was aborted or lost, and a broken pipe for one that was shut down.
            if let Some(end) = self.end() {
                return Poll::Ready(Err(end
                    .err()
                    .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE))));
            }
            let mut guard = ready!(self.fd.poll_write_ready(cx))?;
            if let Ok(result) =
                guard.try_io(|inner| sys::sctp_sendmsg_once(inner.as_fd(), to, payload, info))
            {
                if let Err(error) = &result {
                    // This call may be the one that Linux tells of the end
                    self.note_error(error);
                    if let Some(end) = self.end() {
                        return Poll::Ready(Err(end
                            .err()
                            .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE))));
                    }
                    if let Some(stream) = self.stream_out_of_range(error, info) {
                        return Poll::Ready(Err(stream));
                    }
                }
                return Poll::Ready(result);
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
            let mut partials = Partials::default();
            let mut first = true;
            let error = receive::receive_message(&mut partials, 100, false, |buffer| {
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
            *core.partials.lock().unwrap() = partials;
            let error = core.peeloff(7).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(error.raw_os_error(), None, "peeloff reached the kernel");
            assert!(!core.partials.lock().unwrap().is_empty());
            if identity.is_some() {
                let error = core.peeloff(8).unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
                assert!(!core.partials.lock().unwrap().is_empty());
            }
        }
    }
}
