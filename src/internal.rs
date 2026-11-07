//! One registered descriptor and its record state, shared by the public socket roles.
use crate::types::internal::SndInfoWire;
use crate::{
    AssocChangeState, AssociationId, ConnState, Event, Notification, NotificationOrData,
    SendOptions, SocketToAssociation,
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
use receive::{DEFAULT_MAX_MESSAGE_SIZE, Partials};

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
    // The errors of messages that a batch of reads discarded after others that it delivered,
    // which the next calls return first, one each.
    pending: Mutex<std::collections::VecDeque<std::io::Error>>,
    has_pending: AtomicBool,
    // Held by every send while it calls the kernel: a batch of sends counts on the room
    // that it found in the send buffer, which no send of another task takes meanwhile.
    sending: Mutex<()>,
}

std::thread_local! {
    // The buffers of the batches of reads of the thread, made at its first batch.
    static SLOTS: std::cell::RefCell<Option<sys::Slots>> = const { std::cell::RefCell::new(None) };
    // Whether each message of a batch of sends goes with `MSG_MORE`.
    static FLAGS: std::cell::RefCell<Vec<bool>> = const { std::cell::RefCell::new(Vec::new()) };
    // What the reads of a batch gave, kept for the vector's room.
    static RESULTS: std::cell::Cell<Vec<std::io::Result<Option<NotificationOrData>>>> =
        const { std::cell::Cell::new(Vec::new()) };
}

// What the kernel counts in the send buffer for a message of `octets`, at most: its buffer
// and its chunk, 1216 octets for one of 200 and 2560 for one of 1400 on Linux 7.0, about
// twice its octets for a longer one, and three times when it is cut in small chunks.
fn queued_at_most(octets: usize) -> usize {
    octets.saturating_mul(3).saturating_add(2048)
}

// A message of a batch of sends: the association that it goes to, which a one-to-one socket
// ignores, its payload and its options.
pub(crate) trait Outgoing {
    fn parts(&self) -> (AssociationId, &[u8], SendOptions);
}

impl Outgoing for (&[u8], SendOptions) {
    fn parts(&self) -> (AssociationId, &[u8], SendOptions) {
        (0, self.0, self.1)
    }
}

impl Outgoing for (AssociationId, &[u8], SendOptions) {
    fn parts(&self) -> (AssociationId, &[u8], SendOptions) {
        *self
    }
}

// What one batch of reads gave.
enum Batch {
    // Parts of messages, none whole yet.
    Parts,
    // This many messages and notifications, appended.
    Delivered(usize),
    // The end of the association of a one-to-one socket that was shut down.
    End,
}

fn style_name(style: SocketToAssociation) -> &'static str {
    match style {
        SocketToAssociation::OneToOne => "one-to-one",
        _ => "one-to-many",
    }
}

// The bit of `event` among the quiet events: the types of the notifications follow 0x8000.
fn bit(event: Event) -> u32 {
    1 << (event as u16 - 0x8000)
}

const RUNNING: i32 = 0;
const SHUT_DOWN: i32 = -1;

impl SocketCore {
    pub(crate) fn new(domain: libc::c_int, style: SocketToAssociation) -> std::io::Result<Self> {
        let fd = sys::sctp_socket_internal(domain, style)?;
        // The stream, payload protocol and association of each message. Accepted and
        // peeled-off sockets inherit the setting.
        sys::request_rcvinfo_internal(fd.as_fd())?;
        let core = Self::register(fd, style)?;
        match style {
            SocketToAssociation::OneToMany => core.interleave()?,
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

    // On an endpoint, the parts of the messages of several associations come in turns,
    // which their receive information tells apart: a long message of one association then
    // holds back none of the others. The parts of a message whose delivery is given up are
    // dropped with the notification of it, which is subscribed for that.
    fn interleave(&self) -> std::io::Result<()> {
        sys::set_fragment_interleave_internal(self.as_fd())?;
        if !sys::sctp_event_subscribed_internal(self.as_fd(), Event::PartialDelivery)? {
            self.subscribe_quietly(Event::PartialDelivery)?;
        }
        self.partials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_interleaved(true);
        Ok(())
    }

    fn subscribe_quietly(&self, event: Event) -> std::io::Result<()> {
        sys::sctp_subscribe_event_internal(self.as_fd(), event, true)?;
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
            pending: Mutex::new(std::collections::VecDeque::new()),
            has_pending: AtomicBool::new(false),
            sending: Mutex::new(()),
        })
    }

    pub(crate) fn from_association(fd: OwnedFd) -> std::io::Result<Self> {
        // accept and peeloff return a single-association descriptor. Linux keeps SO_TYPE as
        // SOCK_SEQPACKET on peeled-off associations, so their role cannot be inferred from it.
        Self::register(fd, SocketToAssociation::OneToOne)
    }

    // A descriptor of one association that the crate did not make.
    pub(crate) fn import(fd: OwnedFd) -> std::io::Result<Self> {
        sys::request_rcvinfo_internal(fd.as_fd())?;
        let core = Self::from_association(fd)?;
        core.hear_of_the_end()?;
        Ok(core)
    }

    // That `call` is for a socket of `style`, and what to call on a socket of the other.
    pub(crate) fn require_style(
        &self,
        style: SocketToAssociation,
        call: &str,
    ) -> std::io::Result<()> {
        if self.style == style {
            return Ok(());
        }
        let instead = match style {
            SocketToAssociation::OneToOne => {
                "`Socket::into_endpoint` makes it an endpoint, whose `connect` starts an \
                 association and which receives those of its peers"
            }
            _ => "`Socket::listen` accepts associations on it, and `Socket::connect` starts one",
        };
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "`{call}` is for a {} socket, and this one is {}: {instead}",
                style_name(style),
                style_name(self.style)
            ),
        ))
    }

    pub(crate) fn max_message_size(&self) -> usize {
        self.max_message_size.load(Ordering::Relaxed)
    }

    pub(crate) fn set_max_message_size(&self, octets: usize) {
        self.max_message_size.store(octets, Ordering::Relaxed);
    }

    pub(crate) async fn connect(self, addrs: &[SocketAddr]) -> std::io::Result<Self> {
        self.require_style(SocketToAssociation::OneToOne, "connect")?;
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
                return Ok(self);
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
        self.require_style(SocketToAssociation::OneToOne, "accept")?;
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
        self.require_style(SocketToAssociation::OneToMany, "peeloff")?;
        let partials = self.partials.lock().unwrap_or_else(PoisonError::into_inner);
        if partials.hold(assoc_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot peel off an association during partial record delivery",
            ));
        }
        let peeled = sys::sctp_peeloff_internal(self.as_fd(), assoc_id).map_err(|error| {
            // Linux's answer for an association that the endpoint does not have
            if error.raw_os_error() == Some(libc::EINVAL) {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("the endpoint has no association {assoc_id} to peel off"),
                )
            } else {
                error
            }
        })?;
        let peeled = Self::from_association(peeled)?;
        peeled.set_max_message_size(self.max_message_size());
        // It has the subscriptions of the endpoint, and those of a one-to-one socket.
        peeled
            .quiet_events
            .store(self.quiet_events.load(Ordering::Acquire), Ordering::Release);
        peeled.hear_of_the_end()?;
        Ok(peeled)
    }

    // The next message or notification. `None` is the end of an association that was shut
    // down, which a one-to-one socket alone has. Not `poll_recv` in a `poll_fn`: the
    // readiness that a poll waits for keeps one waker, and of two tasks that receive at once
    // one would never wake; `ready` wakes each task that waits.
    pub(crate) async fn recv(&self) -> std::io::Result<Option<NotificationOrData>> {
        if let Some(error) = self.take_pending() {
            return Err(error);
        }
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
        if let Some(error) = self.take_pending() {
            return Poll::Ready(Err(error));
        }
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

    fn receive_once(&self, fd: BorrowedFd<'_>) -> std::io::Result<Option<NotificationOrData>> {
        let mut partials = self.partials.lock().unwrap_or_else(PoisonError::into_inner);
        let received = receive::receive_message(&mut partials, self.max_message_size(), |buffer| {
            sys::sctp_recvmsg_once(fd, buffer)
        });
        Self::abort_past_the_limit(fd, &mut partials);
        received
    }

    // Abort the associations whose messages in assembly went past the limit of the endpoint.
    fn abort_past_the_limit(fd: BorrowedFd<'_>, partials: &mut Partials) {
        for assoc_id in partials.take_aborts() {
            let info = SndInfoWire::control(crate::consts::SCTP_ABORT, assoc_id);
            // An association that cannot be aborted is past saving: nothing more to do.
            let _ = sys::sctp_sendmsg_once(fd, None, b"messages in assembly past the limit", info);
        }
    }

    // The next error that a batch of reads left for a later call.
    fn take_pending(&self) -> Option<std::io::Error> {
        if !self.has_pending.load(Ordering::Acquire) {
            return None;
        }
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let error = pending.pop_front();
        self.has_pending
            .store(!pending.is_empty(), Ordering::Release);
        error
    }

    fn leave_pending(&self, error: std::io::Error) {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        pending.push_back(error);
        self.has_pending.store(true, Ordering::Release);
    }

    // Up to `limit` messages and notifications, appended to `out`, waiting for the first:
    // those that one batch of reads makes whole. 0 is the end of the association of a
    // one-to-one socket that was shut down, as `None` is for `recv`.
    pub(crate) fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> Poll<std::io::Result<usize>> {
        if limit == 0 {
            return Poll::Ready(Ok(0));
        }
        if let Some(error) = self.take_pending() {
            return Poll::Ready(Err(error));
        }
        loop {
            if let Some(end) = self.end() {
                return Poll::Ready(self.after_the_end(end).map(|received| match received {
                    Some(received) => {
                        out.push(received);
                        1
                    }
                    None => 0,
                }));
            }
            let mut guard = ready!(self.fd.poll_read_ready(cx))?;
            if self.end().is_some() {
                continue;
            }
            match guard.try_io(|inner| self.receive_batch_once(inner.as_fd(), out, limit)) {
                Ok(Ok(Batch::Parts)) | Err(_) => {}
                Ok(Ok(Batch::Delivered(count))) => return Poll::Ready(Ok(count)),
                Ok(Ok(Batch::End)) => return Poll::Ready(Ok(0)),
                Ok(Err(error)) => {
                    if let Some(result) = self.note_received(Err(error)) {
                        return Poll::Ready(result.map(|_| 0));
                    }
                }
            }
        }
    }

    pub(crate) async fn recv_many(
        &self,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> std::io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_recv_many(cx, out, limit)).await
    }

    // One batch of reads, without waiting, its parts assembled. `WouldBlock` when the kernel
    // has nothing, which clears the readiness. An error of a message that comes after others
    // that are delivered is left for the next call.
    fn receive_batch_once(
        &self,
        fd: BorrowedFd<'_>,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> std::io::Result<Batch> {
        let mut partials = self.partials.lock().unwrap_or_else(PoisonError::into_inner);
        let mut results = RESULTS.take();
        let read = SLOTS.with_borrow_mut(|slots| {
            let slots = slots.get_or_insert_with(sys::Slots::new);
            match sys::sctp_recvmmsg_once(fd, slots, limit) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => return Ok(()),
                Err(error) => return Err(error),
            }
            let slots = &*slots;
            receive::receive_batch(
                &mut partials,
                self.max_message_size(),
                slots
                    .pieces()
                    .iter()
                    .enumerate()
                    .map(|(index, piece)| (slots.octets(index, piece), piece)),
                &mut results,
            );
            Ok(())
        });
        if let Err(error) = read {
            // Mostly `WouldBlock`: the vector stays for the next batch
            RESULTS.set(results);
            return Err(error);
        }
        Self::abort_past_the_limit(fd, &mut partials);
        drop(partials);
        let mut delivered = 0;
        let mut first_error = None;
        let mut end = false;
        for result in results.drain(..) {
            match self.note_received(result) {
                None => {}
                Some(Ok(Some(received))) => {
                    out.push(received);
                    delivered += 1;
                }
                // Nothing comes after the end
                Some(Ok(None)) => end = true,
                Some(Err(error)) if delivered == 0 && first_error.is_none() => {
                    first_error = Some(error);
                }
                Some(Err(error)) => self.leave_pending(error),
            }
        }
        RESULTS.set(results);
        if let Some(error) = first_error {
            if delivered == 0 {
                return Err(error);
            }
            self.leave_pending(error);
        }
        Ok(if delivered > 0 {
            Batch::Delivered(delivered)
        } else if end {
            Batch::End
        } else {
            Batch::Parts
        })
    }

    // Send a prefix of `messages`, waiting for the socket to take the first: how many it took.
    // The messages of an association that follow one another are sent with `MSG_MORE` but
    // their last, for the kernel to put their DATA chunks in fewer packets.
    pub(crate) fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        messages: &[impl Outgoing],
    ) -> Poll<std::io::Result<usize>> {
        let Some((assoc_id, first, options)) = messages.first().map(Outgoing::parts) else {
            return Poll::Ready(Ok(0));
        };
        if messages.iter().any(|message| message.parts().1.is_empty()) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a message has one octet at least: SCTP has no empty message",
            )));
        }
        loop {
            if let Some(end) = self.end() {
                return Poll::Ready(Err(end
                    .err()
                    .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE))));
            }
            let mut guard = ready!(self.fd.poll_write_ready(cx))?;
            let sent = guard.try_io(|inner| self.send_batch_once(inner.as_fd(), messages));
            match sent {
                Ok(Ok(count)) => return Poll::Ready(Ok(count)),
                Ok(Err(error)) => {
                    self.note_error(&error);
                    if let Some(end) = self.end() {
                        return Poll::Ready(Err(end
                            .err()
                            .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE))));
                    }
                    if let Some(stream) = self.stream_out_of_range(&error, assoc_id, options) {
                        return Poll::Ready(Err(stream));
                    }
                    if error.raw_os_error() == Some(libc::EMSGSIZE) {
                        return Poll::Ready(Err(self.too_long(first.len())));
                    }
                    return Poll::Ready(Err(error));
                }
                Err(_) => {}
            }
        }
    }

    pub(crate) async fn send_many(&self, messages: &[impl Outgoing]) -> std::io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_send_many(cx, messages)).await
    }

    // One prefix of `messages` sent without waiting, in runs of one flag each: `WouldBlock`
    // when the socket takes none, and how many it took otherwise.
    //
    // The kernel holds back the DATA chunks of an association that a send with `MSG_MORE`
    // left, until a send without it, or until they fill a packet: one of 64 KiB on loopback.
    // Nothing else sends them, not even the acknowledgement of all that went before. So a
    // message goes with the flag only when the kernel is sure to take the next, which goes
    // to the same association: the send buffer has room for both, as `SO_MEMINFO` counts it,
    // and the next goes to a stream that the association has. The last message that a batch sends then has no flag, and nothing
    // that it took is held back after it, unless the system runs out of memory for SCTP in
    // the middle of it.
    fn send_batch_once(
        &self,
        fd: BorrowedFd<'_>,
        messages: &[impl Outgoing],
    ) -> std::io::Result<usize> {
        let _sending = self.sending.lock().unwrap_or_else(PoisonError::into_inner);
        FLAGS.with_borrow_mut(|flags| Self::send_flagged(fd, messages, flags))
    }

    // `send_batch_once`, with a vector of the thread for the flags.
    fn send_flagged(
        fd: BorrowedFd<'_>,
        messages: &[impl Outgoing],
        flags: &mut Vec<bool>,
    ) -> std::io::Result<usize> {
        let bundling = messages.len() > 1;
        let (size, free) = if bundling {
            sys::send_buffer(fd).unwrap_or((0, 0))
        } else {
            (0, 0)
        };
        flags.clear();
        // What the messages so far take of the send buffer, at most
        let mut used = 0_usize;
        // The streams of the association of the message, below this one, that it has: those
        // of the messages of the association that come before, or all of them once its
        // status was asked for
        let mut streams = 0_u32;
        let mut asked = false;
        let mut previous = None;
        for (index, message) in messages.iter().enumerate() {
            let (assoc_id, payload, options) = message.parts();
            used = used.saturating_add(queued_at_most(payload.len()));
            if previous.is_some_and(|previous| previous != assoc_id) {
                (streams, asked) = (0, false);
            }
            previous = Some(assoc_id);
            streams = streams.max(u32::from(options.stream_id) + 1);
            let Some(next) =
                messages
                    .get(index + 1)
                    .map(Outgoing::parts)
                    .filter(|(next, payload, _)| {
                        bundling
                            && *next == assoc_id
                            && payload.len() <= size
                            && used.saturating_add(queued_at_most(payload.len())) <= free
                    })
            else {
                flags.push(false);
                continue;
            };
            let next = next.2.stream_id;
            if u32::from(next) >= streams && !asked {
                asked = true;
                if let Ok(status) = sys::sctp_get_status_internal(fd, assoc_id) {
                    streams = streams.max(u32::from(status.outstreams));
                }
            }
            flags.push(u32::from(next) < streams);
        }
        let mut sent = 0;
        while sent < messages.len() {
            let flag = flags[sent];
            let run = flags[sent..]
                .iter()
                .take_while(|more| **more == flag)
                .count();
            match sys::sctp_sendmmsg_once(fd, &messages[sent..sent + run], flag) {
                Ok(count) => {
                    sent += count;
                    if count < run {
                        return Ok(sent);
                    }
                }
                // The next call has the error again, on the message that this one did not
                // send
                Err(error) => return if sent > 0 { Ok(sent) } else { Err(error) },
            }
        }
        Ok(sent)
    }

    pub(crate) fn max_assembly(&self) -> usize {
        self.partials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .max_assembly()
    }

    pub(crate) fn set_max_assembly(&self, octets: usize) {
        self.partials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_max_assembly(octets);
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

    pub(crate) fn subscribe_events(&self, events: &[Event], on: bool) -> std::io::Result<()> {
        if on {
            sys::sctp_subscribe_events_internal(self.as_fd(), events, on)?;
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
        sys::sctp_subscribe_events_internal(self.as_fd(), &others, on)
    }

    // Sends `payload` with `options`, to `to` or to association `assoc_id`, which a one-to-one
    // socket ignores.
    pub(crate) async fn send(
        &self,
        to: Option<SocketAddr>,
        assoc_id: AssociationId,
        payload: &[u8],
        options: SendOptions,
    ) -> std::io::Result<()> {
        std::future::poll_fn(|cx| self.poll_send(cx, to, assoc_id, payload, options)).await
    }

    pub(crate) fn poll_send(
        &self,
        cx: &mut Context<'_>,
        to: Option<SocketAddr>,
        assoc_id: AssociationId,
        payload: &[u8],
        options: SendOptions,
    ) -> Poll<std::io::Result<()>> {
        // SCTP has no empty message, and Linux answers a bare `EINVAL`.
        if payload.is_empty() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a message has one octet at least: SCTP has no empty message",
            )));
        }
        let info = SndInfoWire::new(options, assoc_id);
        loop {
            // After the end, the same answer at each call: the error of an association that
            // was aborted or lost, and a broken pipe for one that was shut down.
            if let Some(end) = self.end() {
                return Poll::Ready(Err(end
                    .err()
                    .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE))));
            }
            let mut guard = ready!(self.fd.poll_write_ready(cx))?;
            if let Ok(result) = guard.try_io(|inner| {
                let _sending = self.sending.lock().unwrap_or_else(PoisonError::into_inner);
                sys::sctp_sendmsg_once(inner.as_fd(), to, payload, info)
            }) {
                if let Err(error) = &result {
                    // This call may be the one that Linux tells of the end
                    self.note_error(error);
                    if let Some(end) = self.end() {
                        return Poll::Ready(Err(end
                            .err()
                            .unwrap_or_else(|| std::io::Error::from_raw_os_error(libc::EPIPE))));
                    }
                    if let Some(stream) = self.stream_out_of_range(error, assoc_id, options) {
                        return Poll::Ready(Err(stream));
                    }
                    if error.raw_os_error() == Some(libc::EMSGSIZE) {
                        return Poll::Ready(Err(self.too_long(payload.len())));
                    }
                }
                return Poll::Ready(result);
            }
        }
    }
}

impl SocketCore {
    // The `EMSGSIZE` of a message longer than the send buffer, said in words.
    fn too_long(&self, octets: usize) -> std::io::Error {
        let buffer = crate::SocketOptions { core: self }
            .send_buffer_size()
            .map(|size| format!(" of {size} octets"))
            .unwrap_or_default();
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "a message of {octets} octets does not fit in the send buffer{buffer} \
                 (SO_SNDBUF): `options().set_send_buffer_size` makes it larger, up to \
                 `net.core.wmem_max`"
            ),
        )
    }

    // The `EINVAL` of a send on a stream that the association does not have, said in words.
    fn stream_out_of_range(
        &self,
        error: &std::io::Error,
        assoc_id: AssociationId,
        options: SendOptions,
    ) -> Option<std::io::Error> {
        if error.raw_os_error() != Some(libc::EINVAL) {
            return None;
        }
        let status = sys::sctp_get_status_internal(self.as_fd(), assoc_id).ok()?;
        let stream = options.stream_id;
        (stream >= status.outstreams).then(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "stream {stream} is not one of the {} outbound streams of the association",
                    status.outstreams
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
    use crate::types::internal::RcvInfoWire;

    /// A message in assembly of an association, or a notification, which may be of any,
    /// keeps the association from being peeled off.
    #[tokio::test]
    async fn peeloff_rejects_partial_record_ownership_before_touching_the_kernel() {
        for notification in [false, true] {
            let core = SocketCore::new(libc::AF_INET, SocketToAssociation::OneToMany).unwrap();
            let mut partials = Partials::default();
            let mut first = true;
            let error = receive::receive_message(&mut partials, 100, |buffer| {
                if !first {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                }
                first = false;
                buffer[..7].copy_from_slice(b"partial");
                Ok(receive::Piece {
                    len: 7,
                    notification,
                    end_of_record: false,
                    info: RcvInfoWire {
                        assoc_id: if notification { 0 } else { 7 },
                        ..Default::default()
                    },
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
            if !notification {
                // The kernel's `EINVAL`, said in words: the endpoint has no association 8
                let error = core.peeloff(8).unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
                assert!(!core.partials.lock().unwrap().is_empty());
            }
        }
    }

    /// A socket of `style` bound to an address of the loopback interface, which listens.
    fn listening(style: SocketToAssociation) -> (SocketCore, SocketAddr) {
        let core = SocketCore::new(libc::AF_INET, style).unwrap();
        let any_port = "127.0.0.1:0".parse().unwrap();
        sys::sctp_bindx_internal(core.as_fd(), &[any_port], crate::BindxFlags::Add).unwrap();
        sys::sctp_listen_internal(core.as_fd(), 4).unwrap();
        let address = sys::local_addr(core.as_fd()).unwrap();
        (core, address)
    }

    /// A peer of `address`, which has sent it `message`.
    async fn peer_that_sent(address: SocketAddr, message: &[u8]) -> crate::ConnectedSocket {
        let peer = crate::Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
        let peer = peer.connect(address).await.unwrap();
        peer.send(message, crate::SendOptions::new()).await.unwrap();
        peer
    }

    /// A socket of one association that has an event subscribed for the crate alone, as a
    /// socket peeled off an endpoint has the partial deliveries, does not pass on its
    /// notifications.
    #[tokio::test]
    async fn a_notification_subscribed_for_the_crate_alone_is_not_passed_on() {
        let core = SocketCore::new(libc::AF_INET, SocketToAssociation::OneToOne).unwrap();
        core.quiet_events
            .fetch_or(bit(Event::PartialDelivery), Ordering::AcqRel);
        let given_up = Notification::PartialDelivery(crate::PartialDelivery {
            indication: 0,
            assoc_id: 1,
            stream: 0,
            sequence: 0,
        });
        let received = Ok(Some(NotificationOrData::Notification(given_up)));
        assert!(core.note_received(received).is_none());
    }

    /// A socket of one association keeps its association changes subscribed when the caller
    /// unsubscribes them: it learns of the end of its association through them.
    #[tokio::test]
    async fn a_socket_of_one_association_keeps_its_association_changes() {
        let core = SocketCore::new(libc::AF_INET, SocketToAssociation::OneToOne).unwrap();
        core.subscribe_events(&[Event::Association], false).unwrap();
        assert!(sys::sctp_event_subscribed_internal(core.as_fd(), Event::Association).unwrap());
        assert!(
            core.quiet(&Notification::AssociationChange(crate::AssociationChange {
                state: AssocChangeState::CommLost,
                cause: 0,
                outbound_streams: 0,
                inbound_streams: 0,
                assoc_id: 0,
                info: Vec::new(),
            }))
        );
    }

    /// A message of a batch goes with `MSG_MORE` when the next is to a stream that the
    /// association has, and not when the next is to one that it lacks, which the kernel
    /// refuses: nothing that it took is held back for that one.
    #[tokio::test]
    async fn a_message_before_one_to_a_stream_that_the_association_lacks_is_not_held() {
        let (_listener, address) = listening(SocketToAssociation::OneToOne);
        let peer = peer_that_sent(address, b"before").await;
        let streams = peer.status().unwrap().outstreams;
        let messages = [
            (&b"first"[..], crate::SendOptions::new()),
            (b"second", crate::SendOptions::new().stream(1)),
            (b"refused", crate::SendOptions::new().stream(streams)),
        ];
        let mut flags = Vec::new();
        let sent = SocketCore::send_flagged(peer.as_fd(), &messages, &mut flags);
        assert_eq!(sent.unwrap(), 2);
        assert_eq!(flags, [true, false, false]);
    }
}
