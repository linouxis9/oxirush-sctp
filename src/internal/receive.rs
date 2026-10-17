//! Whole-record assembly, independent of descriptor ownership and readiness.
use super::notifications::notification_from_message;
use crate::{NotificationOrData, NxtInfo, RcvInfo, ReceivedData};
use std::net::SocketAddr;

// Messages longer than this are discarded unless the socket sets another limit, so that a peer
// cannot make us buffer without bound.
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 << 20;

// The octets that an endpoint holds of the messages that it assembles, over its associations:
// sixteen messages of the longest size. Past it the association with the longest message in
// assembly is aborted, so that peers together cannot make it buffer without bound either.
pub(crate) const DEFAULT_MAX_ASSEMBLY: usize = 16 * DEFAULT_MAX_MESSAGE_SIZE;

// The associations aborted for that limit whose parts are dropped until the kernel says that it
// gave up their message: those it had queued before the abort would otherwise begin a message.
const DROPPING: usize = 64;

// Initial size of the buffer of a message that one read did not give whole. It doubles for
// longer messages.
const RECV_BUFFER_SIZE: usize = 4096;

// The first read of a message goes into a buffer of the thread, as long as the longest that a
// read gives at once: a message that it holds whole is then copied out of it to a payload of
// its length, with one read and one allocation where reads into a payload that doubles took
// three for 16 KiB.
const FIRST_READ_SIZE: usize = 65536;

std::thread_local! {
    static FIRST_READ: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

// Size of the reads that discard a message longer than the limit.
const DISCARD_BUFFER_SIZE: usize = 65536;

// The part of a message received so far.
#[derive(Default)]
pub(crate) struct PartialMessage {
    notification: bool,
    payload: Vec<u8>,
    rcv_info: Option<RcvInfo>,
    from: Option<SocketAddr>,
    // Octets received, also those discarded once the message is longer than the limit.
    received: usize,
}

impl PartialMessage {
    // A message of which a first part was read elsewhere.
    pub(super) fn begun(part: &[u8], piece: &Piece) -> Self {
        Self {
            notification: piece.notification,
            payload: part.to_vec(),
            rcv_info: piece.rcv_info.clone(),
            from: piece.from,
            received: piece.len,
        }
    }

    pub(crate) fn belongs_to(&self, assoc_id: crate::AssociationId) -> bool {
        self.rcv_info
            .as_ref()
            .is_none_or(|info| info.assoc_id == assoc_id)
    }
}

// Without the payload, which can be long.
impl std::fmt::Debug for PartialMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialMessage")
            .field("notification", &self.notification)
            .field("rcv_info", &self.rcv_info)
            .field("from", &self.from)
            .field("received", &self.received)
            .finish()
    }
}

// One part of a message, as returned by `recvmsg`.
pub(crate) struct Piece {
    pub(super) len: usize,
    pub(super) notification: bool,
    pub(super) end_of_record: bool,
    pub(super) rcv_info: Option<RcvInfo>,
    pub(super) nxt_info: Option<NxtInfo>,
    pub(super) from: Option<SocketAddr>,
}

// The messages of a socket that are being assembled.
//
// With `SCTP_FRAGMENT_INTERLEAVE` level 0, the parts of a message are not interleaved with other
// messages: there is one at most. The kernel only delivers something else after a part when it
// aborts the delivery of the message, e.g. because the association is aborted. With level 1,
// which an endpoint has, the parts of the messages of several associations come in turns, each
// with its receive information: there is one message of each association at most, and one
// notification.
#[derive(Debug)]
pub(crate) struct Partials {
    interleaved: bool,
    messages: Vec<PartialMessage>,
    // The octets of `messages`.
    held: usize,
    // The most that they may hold when the parts of several associations come in turns.
    max_assembly: usize,
    // The associations aborted for that limit, which the socket has still to abort.
    to_abort: Vec<crate::AssociationId>,
    // The associations aborted for it whose parts are dropped.
    dropping: Vec<crate::AssociationId>,
}

impl Default for Partials {
    fn default() -> Self {
        Self {
            interleaved: false,
            messages: Vec::new(),
            held: 0,
            max_assembly: DEFAULT_MAX_ASSEMBLY,
            to_abort: Vec::new(),
            dropping: Vec::new(),
        }
    }
}

impl Partials {
    pub(crate) fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub(crate) fn set_max_assembly(&mut self, octets: usize) {
        self.max_assembly = octets;
    }

    pub(crate) fn max_assembly(&self) -> usize {
        self.max_assembly
    }

    // The associations to abort for the limit of the assembly, which the caller aborts.
    pub(crate) fn take_aborts(&mut self) -> Vec<crate::AssociationId> {
        std::mem::take(&mut self.to_abort)
    }

    fn pop(&mut self) -> Option<PartialMessage> {
        let message = self.messages.pop()?;
        self.held -= message.payload.len();
        Some(message)
    }

    fn clear(&mut self) {
        self.messages.clear();
        self.held = 0;
    }

    // Past the limit of the assembly, the association with the longest message in assembly is
    // to be aborted, and its message is dropped: the error says so. Only an endpoint, whose
    // associations come in turns, has several messages in assembly.
    fn within_limit(&mut self) -> std::io::Result<()> {
        if !self.interleaved || self.held <= self.max_assembly {
            return Ok(());
        }
        let held = self.held;
        let longest = self
            .messages
            .iter()
            .enumerate()
            .filter(|(_, message)| !message.notification)
            .filter_map(|(index, message)| {
                let assoc_id = message.rcv_info.as_ref()?.assoc_id;
                Some((message.payload.len(), index, assoc_id))
            })
            .max();
        let Some((octets, index, assoc_id)) = longest else {
            return Ok(());
        };
        let message = self.messages.swap_remove(index);
        self.held -= message.payload.len();
        self.to_abort.push(assoc_id);
        if self.dropping.len() == DROPPING {
            self.dropping.remove(0);
        }
        self.dropping.push(assoc_id);
        tracing::warn!(
            "Aborting association {assoc_id}: the endpoint held {held} octets of messages in \
             assembly, past its limit of {}, {octets} of them of its message.",
            self.max_assembly
        );
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "aborted association {assoc_id}, whose message of {octets} octets so far was the \
                 longest of the {held} octets in assembly, past the limit of {} \
                 (`max_assembly`)",
                self.max_assembly
            ),
        ))
    }

    // Whether the parts of the messages of several associations come in turns.
    pub(crate) fn set_interleaved(&mut self, interleaved: bool) {
        self.interleaved = interleaved;
    }

    pub(crate) fn interleaved(&self) -> bool {
        self.interleaved
    }

    // Whether a message of association `assoc_id` may be being assembled.
    pub(crate) fn hold(&self, assoc_id: crate::AssociationId) -> bool {
        self.messages
            .iter()
            .any(|message| message.belongs_to(assoc_id))
    }

    pub(crate) fn begin(&mut self, message: PartialMessage) {
        self.held += message.payload.len();
        self.messages.push(message);
    }

    // The message of the key of `piece`, if one is being assembled.
    fn take(&mut self, piece: &Piece) -> Option<PartialMessage> {
        let key = |info: &Option<RcvInfo>| info.as_ref().map(|info| info.assoc_id);
        let index = self.messages.iter().position(|message| {
            message.notification == piece.notification
                && key(&message.rcv_info) == key(&piece.rcv_info)
        })?;
        let message = self.messages.swap_remove(index);
        self.held -= message.payload.len();
        Some(message)
    }

    // Whether `piece` is of an association aborted for the limit, whose parts are dropped.
    fn dropped(&self, piece: &Piece) -> bool {
        !piece.notification
            && piece
                .rcv_info
                .as_ref()
                .is_some_and(|info| self.dropping.contains(&info.assoc_id))
    }

    // An association whose end `notification` tells has no more parts to give.
    fn learn(&mut self, notification: &crate::Notification) {
        use crate::{AssocChangeState, Notification};
        let ended = match notification {
            Notification::PartialDelivery(delivery) => delivery.assoc_id,
            Notification::AssociationChange(change)
                if matches!(
                    change.state,
                    AssocChangeState::CommLost
                        | AssocChangeState::Restart
                        | AssocChangeState::ShutdownComplete
                ) =>
            {
                change.assoc_id
            }
            _ => return,
        };
        self.messages.retain(|message| {
            message.notification
                || message
                    .rcv_info
                    .as_ref()
                    .is_some_and(|info| info.assoc_id != ended)
        });
        self.held = self
            .messages
            .iter()
            .map(|message| message.payload.len())
            .sum();
        self.dropping.retain(|assoc_id| *assoc_id != ended);
    }
}

// Reads the next message up to its end (`MSG_EOR`), or fails with `EWOULDBLOCK` after keeping the
// parts received so far in `partials`. `None` is the end of the stream: a read of no octets, which
// SCTP has no message for.
//
// The limit is for the messages of the peer. A notification is the kernel's, and is never
// discarded: the longest one carries a message that this side sent.
pub(super) fn receive_message(
    partials: &mut Partials,
    max_message_size: usize,
    one_association: bool,
    mut read: impl FnMut(&mut [u8]) -> std::io::Result<Piece>,
) -> std::io::Result<Option<NotificationOrData>> {
    let received = assemble(partials, max_message_size, one_association, &mut read);
    if let Ok(Some(NotificationOrData::Notification(notification))) = &received {
        partials.learn(notification);
    }
    received
}

fn assemble(
    partials: &mut Partials,
    max_message_size: usize,
    one_association: bool,
    read: &mut impl FnMut(&mut [u8]) -> std::io::Result<Piece>,
) -> std::io::Result<Option<NotificationOrData>> {
    loop {
        if partials.interleaved && (!partials.messages.is_empty() || !partials.dropping.is_empty())
        {
            match interleaved_read(partials, max_message_size, read)? {
                Some(received) => return Ok(received),
                None => {
                    partials.within_limit()?;
                    continue;
                }
            }
        }
        let mut message = match partials.pop() {
            Some(message) => message,
            None => match first_read(max_message_size, read)? {
                First::Whole(received) => return Ok(received),
                First::Part(message) => {
                    partials.begin(message);
                    continue;
                }
                First::Interrupted => continue,
            },
        };
        let limit = if message.notification {
            usize::MAX
        } else {
            max_message_size
        };
        let discarding = message.received > limit;
        if discarding {
            message.payload.clear();
        }
        let start = message.payload.len();
        let room = if discarding {
            DISCARD_BUFFER_SIZE
        } else {
            start
                .max(RECV_BUFFER_SIZE)
                .min(limit.saturating_sub(start).saturating_add(1))
        };
        // `Vec::resize` would grow geometrically beyond the configured limit. One extra octet
        // lets us detect an oversized message; subsequent reads use the fixed discard buffer.
        message.payload.reserve_exact(room);
        message.payload.resize(start + room, 0);

        let piece = read(&mut message.payload[start..]);
        let piece = match piece {
            Ok(piece) => piece,
            Err(e) => {
                message.payload.truncate(start);
                if e.kind() == std::io::ErrorKind::Interrupted {
                    partials.begin(message);
                    continue;
                }
                if e.kind() == std::io::ErrorKind::WouldBlock && message.received > 0 {
                    partials.begin(message);
                }
                return Err(e);
            }
        };
        message.payload.truncate(start + piece.len);

        if piece.len == 0 && !piece.notification {
            if message.received > 0 {
                tracing::warn!(
                    "End of stream after {} octets of a message.",
                    message.received
                );
            }
            tracing::debug!("Received end of stream.");
            return Ok(None);
        }

        let continued = message.received > 0;
        if continued && !continues(&message, &piece, one_association) {
            tracing::warn!(
                "Dropping {} octets of a message whose delivery was aborted.",
                message.received
            );
            message.payload.drain(..start);
            message.received = 0;
        }
        if message.received == 0 {
            message.notification = piece.notification;
            message.rcv_info = piece.rcv_info;
            message.from = piece.from;
        }
        message.received = message.received.saturating_add(piece.len);

        if !piece.end_of_record {
            partials.begin(message);
            continue;
        }
        return finish(message, piece.nxt_info, max_message_size);
    }
}

// Whether `piece` is the next part of `message`.
fn continues(message: &PartialMessage, piece: &Piece, one_association: bool) -> bool {
    let same_message = match (&message.rcv_info, &piece.rcv_info) {
        (Some(first), Some(next)) => {
            first.assoc_id == next.assoc_id
                && first.sid == next.sid
                && first.ppid == next.ppid
                && first.flags == next.flags
                // The SSN has no meaning for unordered messages.
                && (first.flags & 1 != 0 || first.ssn == next.ssn)
        }
        // Without receive information only the address tells senders apart, and the parts
        // of a message can come from several addresses of a multihomed peer.
        _ => one_association || message.from == piece.from,
    };
    piece.notification == message.notification && same_message
}

// A message whose last part has come.
fn finish(
    mut message: PartialMessage,
    nxt_info: Option<NxtInfo>,
    max_message_size: usize,
) -> std::io::Result<Option<NotificationOrData>> {
    if !message.notification && message.received > max_message_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "discarded a message of {} octets, longer than {}",
                message.received, max_message_size
            ),
        ));
    }
    if message.notification {
        tracing::debug!("Received Notification.");
        return Ok(Some(NotificationOrData::Notification(
            notification_from_message(&message.payload),
        )));
    }
    tracing::debug!("Received Data.");
    // The buffer had room for the largest read, 4 KiB at least, which a
    // program that keeps many short messages would pay for each
    message.payload.shrink_to_fit();
    Ok(Some(NotificationOrData::Data(ReceivedData {
        payload: message.payload,
        from: message.from,
        rcv_info: message.rcv_info,
        nxt_info,
    })))
}

// The next part of one of the messages of several associations that come in turns, read into the
// buffer of the thread and copied to the payload of its message: the association of a part is
// known once it is read. `None` when no message is whole yet.
fn interleaved_read(
    partials: &mut Partials,
    max_message_size: usize,
    read: &mut impl FnMut(&mut [u8]) -> std::io::Result<Piece>,
) -> std::io::Result<Option<Option<NotificationOrData>>> {
    FIRST_READ.with_borrow_mut(|buffer| {
        if buffer.is_empty() {
            buffer.resize(FIRST_READ_SIZE, 0);
        }
        let piece = match read(buffer) {
            Ok(piece) => piece,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => return Ok(None),
            Err(error) => return Err(error),
        };
        let octets = &buffer[..piece.len];
        if piece.len == 0 && !piece.notification {
            tracing::debug!("Received end of stream.");
            partials.clear();
            return Ok(Some(None));
        }
        if partials.dropped(&piece) {
            return Ok(None);
        }
        let message = match partials.take(&piece) {
            Some(message) if continues(&message, &piece, false) => Some(message),
            Some(message) => {
                tracing::warn!(
                    "Dropping {} octets of a message whose delivery was aborted.",
                    message.received
                );
                None
            }
            None => None,
        };
        let Some(mut message) = message else {
            return match begin(octets, &piece, max_message_size)? {
                First::Part(message) => {
                    partials.begin(message);
                    Ok(None)
                }
                First::Whole(received) => Ok(Some(received)),
                First::Interrupted => Ok(None),
            };
        };
        let limit = if message.notification {
            usize::MAX
        } else {
            max_message_size
        };
        message.received = message.received.saturating_add(piece.len);
        if message.received > limit {
            // Its octets are not kept: it is discarded at its end
            message.payload = Vec::new();
        } else {
            message.payload.extend_from_slice(octets);
        }
        if !piece.end_of_record {
            partials.begin(message);
            return Ok(None);
        }
        finish(message, piece.nxt_info, max_message_size).map(Some)
    })
}

// What the first read of a message gave.
enum First {
    // A whole message or notification, or the end of the stream.
    Whole(Option<NotificationOrData>),
    // The first part of a longer one.
    Part(PartialMessage),
    // Nothing: the read was interrupted.
    Interrupted,
}

// The first read of a message, into the buffer of the thread.
fn first_read(
    max_message_size: usize,
    read: &mut impl FnMut(&mut [u8]) -> std::io::Result<Piece>,
) -> std::io::Result<First> {
    FIRST_READ.with_borrow_mut(|buffer| {
        if buffer.is_empty() {
            buffer.resize(FIRST_READ_SIZE, 0);
        }
        let piece = match read(buffer) {
            Ok(piece) => piece,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                return Ok(First::Interrupted);
            }
            Err(error) => return Err(error),
        };
        begin(&buffer[..piece.len], &piece, max_message_size)
    })
}

// What the first part of a message gives, whose octets are `octets`.
fn begin(octets: &[u8], piece: &Piece, max_message_size: usize) -> std::io::Result<First> {
    {
        if piece.len == 0 && !piece.notification {
            tracing::debug!("Received end of stream.");
            return Ok(First::Whole(None));
        }
        let limit = if piece.notification {
            usize::MAX
        } else {
            max_message_size
        };
        if !piece.end_of_record {
            let mut message = PartialMessage::begun(octets, piece);
            if piece.len > limit {
                // Its octets are not kept: it is discarded at its end
                message.payload = Vec::new();
            }
            return Ok(First::Part(message));
        }
        if piece.len > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "discarded a message of {} octets, longer than {max_message_size}",
                    piece.len
                ),
            ));
        }
        if piece.notification {
            tracing::debug!("Received Notification.");
            return Ok(First::Whole(Some(NotificationOrData::Notification(
                notification_from_message(octets),
            ))));
        }
        tracing::debug!("Received Data.");
        Ok(First::Whole(Some(NotificationOrData::Data(ReceivedData {
            payload: octets.to_vec(),
            from: piece.from,
            rcv_info: piece.rcv_info.clone(),
            nxt_info: piece.nxt_info.clone(),
        }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::os::fd::AsFd;

    enum Step {
        Piece(Vec<u8>, Piece),
        Error(std::io::ErrorKind),
    }

    fn data(payload: &[u8], end_of_record: bool, rcv_info: Option<RcvInfo>) -> Step {
        Step::Piece(
            payload.to_vec(),
            Piece {
                len: payload.len(),
                notification: false,
                end_of_record,
                rcv_info,
                nxt_info: None,
                from: None,
            },
        )
    }

    fn reader(steps: Vec<Step>) -> impl FnMut(&mut [u8]) -> std::io::Result<Piece> {
        let mut steps = VecDeque::from(steps);
        move |buffer| match steps.pop_front().expect("unexpected read") {
            Step::Piece(payload, piece) => {
                assert!(payload.len() <= buffer.len());
                buffer[..payload.len()].copy_from_slice(&payload);
                Ok(piece)
            }
            Step::Error(kind) => Err(std::io::Error::from(kind)),
        }
    }

    fn payload(record: Option<NotificationOrData>) -> Vec<u8> {
        match record {
            Some(NotificationOrData::Data(data)) => data.payload,
            other => panic!("unexpected {other:?}"),
        }
    }

    /// A message as the kernel hands it over: as much of it as the buffer
    /// of each read holds, with `MSG_EOR` on the read of its end.
    fn kernel(
        message: Vec<u8>,
        reads: &std::cell::Cell<usize>,
    ) -> impl FnMut(&mut [u8]) -> std::io::Result<Piece> {
        let mut rest = message;
        move |buffer| {
            reads.set(reads.get() + 1);
            let len = rest.len().min(buffer.len());
            buffer[..len].copy_from_slice(&rest[..len]);
            rest.drain(..len);
            Ok(Piece {
                len,
                notification: false,
                end_of_record: rest.is_empty(),
                rcv_info: None,
                nxt_info: None,
                from: None,
            })
        }
    }

    /// A message of 16 KiB, which a read of the buffer of a doubling
    /// payload took three of, takes one, into a payload of its length.
    #[test]
    fn a_message_that_one_read_holds_takes_one_read() {
        for octets in [64, 1400, 16384, 65536] {
            let reads = std::cell::Cell::new(0);
            let mut partial = Partials::default();
            let message: Vec<u8> = (0..octets).map(|i| i as u8).collect();
            let read = kernel(message.clone(), &reads);
            let received = payload(receive_message(&mut partial, 1 << 20, true, read).unwrap());
            assert_eq!(received, message);
            assert_eq!(received.capacity(), octets);
            assert_eq!(reads.get(), 1, "{octets} octets");
        }
        // A longer one takes the reads that it needs.
        let reads = std::cell::Cell::new(0);
        let mut partial = Partials::default();
        let message = vec![7; 200_000];
        let received = payload(
            receive_message(&mut partial, 1 << 20, true, kernel(message.clone(), &reads)).unwrap(),
        );
        assert_eq!((received == message, received.capacity()), (true, 200_000));
        assert!(reads.get() <= 3, "{} reads", reads.get());
    }

    /// The buffer of a read has room for a whole packet of the kernel, and a
    /// message is mostly much shorter: what `recv` returns keeps the octets
    /// of the message and no more, in one read or in several.
    #[test]
    fn a_message_keeps_no_more_memory_than_its_octets() {
        let mut partial = Partials::default();
        let read = reader(vec![data(b"sixty-four octets, say", true, None)]);
        let short = payload(receive_message(&mut partial, 1 << 20, true, read).unwrap());
        assert_eq!(short.capacity(), short.len());

        let (first, last) = (vec![1; 4000], vec![2; 300]);
        let read = reader(vec![data(&first, false, None), data(&last, true, None)]);
        let long = payload(receive_message(&mut partial, 1 << 20, true, read).unwrap());
        assert_eq!((long.len(), long.capacity()), (4300, 4300));
    }

    #[test]
    fn interrupted_read_retains_the_partial_record() {
        let mut partial = Partials::default();
        let read = reader(vec![
            data(b"first", false, None),
            Step::Error(std::io::ErrorKind::Interrupted),
            data(b"last", true, None),
        ]);
        assert_eq!(
            payload(receive_message(&mut partial, 100, false, read).unwrap()),
            b"firstlast"
        );
        assert!(partial.is_empty());
    }

    #[test]
    fn would_block_and_resume_preserve_first_and_last_metadata() {
        let first = RcvInfo {
            sid: 2,
            assoc_id: 7,
            ..Default::default()
        };
        let next = NxtInfo {
            length: 5,
            assoc_id: 7,
            ..Default::default()
        };
        let mut last = data(b"last", true, Some(first.clone()));
        if let Step::Piece(_, piece) = &mut last {
            piece.nxt_info = Some(next.clone());
        }
        let mut read = reader(vec![
            data(b"first", false, Some(first.clone())),
            Step::Error(std::io::ErrorKind::WouldBlock),
            last,
        ]);
        let mut partial = Partials::default();
        assert_eq!(
            receive_message(&mut partial, 100, false, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(partial.messages[0].payload, b"first");
        let Some(NotificationOrData::Data(record)) =
            receive_message(&mut partial, 100, false, read).unwrap()
        else {
            panic!("not data");
        };
        assert_eq!(record.payload, b"firstlast");
        assert_eq!(record.rcv_info, Some(first));
        assert_eq!(record.nxt_info, Some(next));
        assert!(partial.is_empty());
    }

    #[test]
    fn oversized_record_is_drained_before_the_next_record() {
        let mut partial = Partials::default();
        let mut read = reader(vec![
            data(b"12345", false, None),
            data(b"67", true, None),
            data(b"ok", true, None),
        ]);
        assert_eq!(
            receive_message(&mut partial, 4, false, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert!(partial.is_empty());
        assert_eq!(
            payload(receive_message(&mut partial, 4, false, read).unwrap()),
            b"ok"
        );
    }

    #[test]
    fn reducing_limit_after_cancellation_drains_the_record() {
        let mut partial = Partials::default();
        let mut read = reader(vec![
            data(b"12345678", false, None),
            Step::Error(std::io::ErrorKind::WouldBlock),
            data(b"9", true, None),
            data(b"ok", true, None),
        ]);
        assert_eq!(
            receive_message(&mut partial, 10, false, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(
            receive_message(&mut partial, 4, false, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            payload(receive_message(&mut partial, 4, false, read).unwrap()),
            b"ok"
        );
    }

    #[test]
    fn aborted_record_does_not_contaminate_another_association() {
        let mut partial = Partials::default();
        let read = reader(vec![
            data(
                b"aborted",
                false,
                Some(RcvInfo {
                    assoc_id: 7,
                    ..Default::default()
                }),
            ),
            data(
                b"new",
                true,
                Some(RcvInfo {
                    assoc_id: 8,
                    ..Default::default()
                }),
            ),
        ]);
        assert_eq!(
            payload(receive_message(&mut partial, 100, false, read).unwrap()),
            b"new"
        );
    }

    fn of(assoc_id: i32) -> Option<RcvInfo> {
        Some(RcvInfo {
            assoc_id,
            ..Default::default()
        })
    }

    /// On an endpoint, the parts of the messages of several associations come in
    /// turns: each is assembled apart, and a long one holds back no other.
    #[test]
    fn the_messages_of_several_associations_are_assembled_apart() {
        let mut partials = Partials::default();
        partials.set_interleaved(true);
        let mut read = reader(vec![
            data(b"long ", false, of(7)),
            data(b"short", true, of(8)),
            data(b"long", false, of(7)),
            data(b"other ", false, of(9)),
            data(b" message", true, of(7)),
            data(b"message", true, of(9)),
            Step::Error(std::io::ErrorKind::WouldBlock),
        ]);
        let mut next = || {
            let message = match receive_message(&mut partials, 100, false, &mut read) {
                Ok(Some(NotificationOrData::Data(message))) => message,
                other => panic!("{other:?}"),
            };
            (
                message.assoc_id(),
                String::from_utf8(message.payload).unwrap(),
            )
        };
        assert_eq!(next(), (Some(8), "short".into()));
        assert_eq!(next(), (Some(7), "long long message".into()));
        assert_eq!(next(), (Some(9), "other message".into()));
        assert!(partials.is_empty());
    }

    /// The parts that an endpoint has of a message whose delivery the kernel gave up
    /// go with the notification of it, or of the end of its association: they are
    /// not taken for the beginning of the next message of that association.
    #[test]
    fn the_parts_of_a_message_given_up_are_dropped() {
        let notification = |event: crate::Event, assoc_id: i32| {
            let mut octets = (event as u16).to_ne_bytes().to_vec();
            octets.extend_from_slice(&0_u16.to_ne_bytes());
            let length: u32 = if event == crate::Event::PartialDelivery {
                24
            } else {
                20
            };
            octets.extend_from_slice(&length.to_ne_bytes());
            match event {
                // sctp_pdapi_event: indication, assoc_id, stream, sequence
                crate::Event::PartialDelivery => {
                    octets.extend_from_slice(&0_u32.to_ne_bytes());
                    octets.extend_from_slice(&assoc_id.to_ne_bytes());
                    octets.extend_from_slice(&0_u32.to_ne_bytes());
                    octets.extend_from_slice(&0_u32.to_ne_bytes());
                }
                // sctp_assoc_change: state COMM_LOST, error, streams, assoc_id
                _ => {
                    octets.extend_from_slice(&1_u16.to_ne_bytes());
                    octets.extend_from_slice(&0_u16.to_ne_bytes());
                    octets.extend_from_slice(&0_u16.to_ne_bytes());
                    octets.extend_from_slice(&0_u16.to_ne_bytes());
                    octets.extend_from_slice(&assoc_id.to_ne_bytes());
                }
            }
            Step::Piece(
                octets.clone(),
                Piece {
                    len: octets.len(),
                    notification: true,
                    end_of_record: true,
                    rcv_info: None,
                    nxt_info: None,
                    from: None,
                },
            )
        };
        for event in [crate::Event::PartialDelivery, crate::Event::Association] {
            let mut partials = Partials::default();
            partials.set_interleaved(true);
            let mut read = reader(vec![
                data(b"given up", false, of(7)),
                data(b"kept ", false, of(8)),
                notification(event, 7),
                data(b"new", true, of(7)),
                data(b"message", true, of(8)),
            ]);
            let mut next = || receive_message(&mut partials, 100, false, &mut read).unwrap();
            assert!(matches!(next(), Some(NotificationOrData::Notification(_))));
            assert_eq!(payload(next()), b"new");
            assert_eq!(payload(next()), b"kept message");
            assert!(partials.is_empty(), "{event:?}");
        }
    }

    /// The limit discards the message of one association, and not those of the
    /// others whose parts come in between.
    #[test]
    fn the_limit_discards_the_message_of_one_association_alone() {
        let mut partials = Partials::default();
        partials.set_interleaved(true);
        let mut read = reader(vec![
            data(b"0123456789", false, of(7)),
            data(b"short", true, of(8)),
            data(b"0123456789", true, of(7)),
            data(b"next", true, of(7)),
        ]);
        let mut next = || receive_message(&mut partials, 15, false, &mut read);
        assert_eq!(payload(next().unwrap()), b"short");
        assert_eq!(next().unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(payload(next().unwrap()), b"next");
        assert!(partials.is_empty());
    }

    /// Past the limit of the octets in assembly, the association with the longest message is
    /// to be aborted: its message is dropped, and so are the parts of it that come until the
    /// kernel says that it gave its delivery up. The others are assembled.
    #[test]
    fn past_the_limit_of_the_assembly_the_longest_message_goes_with_its_association() {
        let given_up = {
            // sctp_pdapi_event: type, flags, length, indication, assoc_id, stream, sequence
            let mut octets = (crate::Event::PartialDelivery as u16)
                .to_ne_bytes()
                .to_vec();
            octets.extend_from_slice(&0_u16.to_ne_bytes());
            octets.extend_from_slice(&24_u32.to_ne_bytes());
            for value in [0_i32, 7, 0, 0] {
                octets.extend_from_slice(&value.to_ne_bytes());
            }
            Step::Piece(
                octets.clone(),
                Piece {
                    len: octets.len(),
                    notification: true,
                    end_of_record: true,
                    rcv_info: None,
                    nxt_info: None,
                    from: None,
                },
            )
        };
        let mut partials = Partials::default();
        partials.set_interleaved(true);
        partials.set_max_assembly(25);
        let mut read = reader(vec![
            data(b"0123456789", false, of(7)),
            data(b"short ", false, of(8)),
            data(b"0123456789", false, of(7)),
            // 26 octets in assembly: 7 goes
            data(b"one", true, of(8)),
            data(b"a part of 7 still queued", true, of(7)),
            given_up,
            data(b"new", true, of(7)),
        ]);
        let mut next = || receive_message(&mut partials, 100, false, &mut read);
        let error = next().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "aborted association 7, whose message of 20 octets so far was the longest of the 26 \
             octets in assembly, past the limit of 25 (`max_assembly`)"
        );
        assert_eq!(partials.take_aborts(), [7]);
        assert_eq!(partials.held, 6);
        let mut next = || receive_message(&mut partials, 100, false, &mut read);
        assert_eq!(payload(next().unwrap()), b"short one");
        assert!(matches!(
            next().unwrap(),
            Some(NotificationOrData::Notification(_))
        ));
        assert_eq!(payload(next().unwrap()), b"new");
        assert!(partials.is_empty() && partials.dropping.is_empty());
        assert_eq!(partials.held, 0);
    }

    #[test]
    fn one_association_keeps_a_record_whose_pieces_come_from_two_peer_addresses() {
        let piece = |payload: &[u8], end_of_record, from: &str| {
            let mut step = data(payload, end_of_record, None);
            if let Step::Piece(_, piece) = &mut step {
                piece.from = Some(from.parse().unwrap());
            }
            step
        };
        let pieces = || {
            vec![
                piece(b"first", false, "192.0.2.1:38412"),
                piece(b"last", true, "198.51.100.1:38412"),
            ]
        };
        assert_eq!(
            payload(
                receive_message(&mut Partials::default(), 100, true, reader(pieces())).unwrap()
            ),
            b"firstlast"
        );
        // The address is all that tells the associations of a one-to-many socket apart.
        assert_eq!(
            payload(
                receive_message(&mut Partials::default(), 100, false, reader(pieces())).unwrap()
            ),
            b"last"
        );
    }

    #[test]
    fn notification_after_partial_delivery_keeps_its_original_octets() {
        let raw = (crate::Event::PartialDelivery as u16)
            .to_ne_bytes()
            .to_vec();
        let mut partial = Partials::default();
        let read = reader(vec![
            data(b"aborted", false, None),
            Step::Piece(
                raw.clone(),
                Piece {
                    len: raw.len(),
                    notification: true,
                    end_of_record: true,
                    rcv_info: None,
                    nxt_info: None,
                    from: None,
                },
            ),
        ]);
        assert_eq!(
            receive_message(&mut partial, 100, false, read).unwrap(),
            Some(NotificationOrData::Notification(
                crate::Notification::Unsupported {
                    kind: crate::Event::PartialDelivery as u16,
                    data: raw,
                }
            ))
        );
    }

    #[test]
    fn unordered_continuation_does_not_depend_on_stream_sequence_number() {
        let first = RcvInfo {
            assoc_id: 7,
            sid: 2,
            flags: 1,
            ssn: 1,
            ..Default::default()
        };
        let last = RcvInfo {
            ssn: 99,
            ..first.clone()
        };
        let read = reader(vec![
            data(b"first", false, Some(first)),
            data(b"last", true, Some(last)),
        ]);
        assert_eq!(
            payload(receive_message(&mut Partials::default(), 100, false, read).unwrap()),
            b"firstlast"
        );
    }

    #[test]
    fn eof_discards_the_incomplete_record() {
        let mut partial = Partials::default();
        let read = reader(vec![data(b"partial", false, None), data(b"", false, None)]);
        assert_eq!(
            receive_message(&mut partial, 100, false, read).unwrap(),
            None
        );
        assert!(partial.is_empty());
    }

    /// The limit is for what the peer sends. A notification is the kernel's, and the
    /// longest carries a message that this side sent: none is discarded, in one read or in
    /// several.
    #[test]
    fn the_limit_never_discards_a_notification() {
        let mut shutdown = (crate::Event::Shutdown as u16).to_ne_bytes().to_vec();
        shutdown.extend_from_slice(&0_u16.to_ne_bytes());
        shutdown.extend_from_slice(&12_u32.to_ne_bytes());
        shutdown.extend_from_slice(&7_i32.to_ne_bytes());
        let part = |octets: &[u8], end_of_record| {
            Step::Piece(
                octets.to_vec(),
                Piece {
                    len: octets.len(),
                    notification: true,
                    end_of_record,
                    rcv_info: None,
                    nxt_info: None,
                    from: None,
                },
            )
        };
        let expected = Some(NotificationOrData::Notification(
            crate::Notification::Shutdown(crate::Shutdown { assoc_id: 7 }),
        ));
        // A limit of 10 octets gives reads of 11: the notification of 12 comes in two parts
        let mut partial = Partials::default();
        let mut read = reader(vec![
            part(&shutdown[..11], false),
            part(&shutdown[11..], true),
            data(b"01234567890", true, None),
            data(b"ok", true, None),
        ]);
        assert_eq!(
            receive_message(&mut partial, 10, true, &mut read).unwrap(),
            expected
        );
        // A message of the peer over the limit is still discarded
        assert_eq!(
            receive_message(&mut partial, 10, true, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            payload(receive_message(&mut partial, 10, true, read).unwrap()),
            b"ok"
        );
    }
    #[test]
    fn receive_growth_stays_within_the_message_limit() {
        use std::os::unix::net::UnixDatagram;
        let (socket, _peer) = UnixDatagram::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        for limit in [1000, 3 << 20, DEFAULT_MAX_MESSAGE_SIZE] {
            let mut partial = Partials::default();
            partial.begin(PartialMessage {
                payload: vec![0; limit],
                received: limit,
                ..Default::default()
            });
            assert_eq!(
                receive_message(&mut partial, limit, false, |buffer| {
                    super::super::sys::sctp_recvmsg_once(socket.as_fd(), buffer)
                })
                .unwrap_err()
                .kind(),
                std::io::ErrorKind::WouldBlock
            );
            let message = partial.messages.pop().unwrap();
            assert_eq!(message.payload.len(), limit);
            assert!(
                message.payload.capacity() <= limit + 1,
                "{} octets allocated for limit {}",
                message.payload.capacity(),
                limit
            );
        }
    }
}
