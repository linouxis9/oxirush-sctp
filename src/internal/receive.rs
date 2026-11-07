//! Whole-record assembly, independent of descriptor ownership and readiness.
//!
//! With `SCTP_FRAGMENT_INTERLEAVE` level 0, Linux's default, the parts of a message are not
//! interleaved with other messages, also of the other associations of an endpoint: a socket
//! has one message in parts at most. The kernel delivers something else after a part only
//! when it gives up the delivery of the message, as when the association is aborted: what
//! comes next is then not the next part, and the message is dropped.
use super::notifications::notification_from_message;
use crate::types::internal::RcvInfoWire;
use crate::{NotificationOrData, ReceivedData};
use std::net::SocketAddr;

// Messages longer than this are discarded unless the socket sets another limit, so that a peer
// cannot make us buffer without bound.
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 << 20;

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
    // The receive information of its first part, none for a notification.
    info: RcvInfoWire,
    from: Option<SocketAddr>,
    // Octets received, also those discarded once the message is longer than the limit.
    received: usize,
}

impl PartialMessage {
    // A message of which a first part was read elsewhere.
    fn begun(part: &[u8], piece: &Piece) -> Self {
        Self {
            notification: piece.notification,
            payload: part.to_vec(),
            info: piece.info,
            from: piece.from,
            received: piece.len,
        }
    }

    // Whether it may be of association `assoc_id`: a notification may be of any.
    pub(crate) fn belongs_to(&self, assoc_id: crate::AssociationId) -> bool {
        self.notification || self.info.assoc_id == assoc_id
    }
}

// Without the payload, which can be long.
impl std::fmt::Debug for PartialMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialMessage")
            .field("notification", &self.notification)
            .field("info", &self.info)
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
    // Zero for a notification, which has none.
    pub(super) info: RcvInfoWire,
    pub(super) from: Option<SocketAddr>,
}

// Reads the next message up to its end (`MSG_EOR`), or fails with `EWOULDBLOCK` after keeping the
// part received so far in `partial`. `None` is the end of the stream: a read of no octets, which
// SCTP has no message for.
//
// The limit is for the messages of the peer. A notification is the kernel's, and is never
// discarded: the longest one carries a message that this side sent.
pub(super) fn receive_message(
    partial: &mut Option<PartialMessage>,
    max_message_size: usize,
    mut read: impl FnMut(&mut [u8]) -> std::io::Result<Piece>,
) -> std::io::Result<Option<NotificationOrData>> {
    loop {
        let mut message = match partial.take() {
            Some(message) => message,
            None => match first_read(max_message_size, &mut read)? {
                First::Whole(received) => return Ok(received),
                First::Part(message) => {
                    *partial = Some(message);
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
                    *partial = Some(message);
                    continue;
                }
                if e.kind() == std::io::ErrorKind::WouldBlock && message.received > 0 {
                    *partial = Some(message);
                }
                return Err(e);
            }
        };
        message.payload.truncate(start + piece.len);

        if piece.len == 0 && !piece.notification {
            return Ok(None);
        }

        let continued = message.received > 0;
        if continued && !continues(&message, &piece) {
            message.payload.drain(..start);
            message.received = 0;
        }
        if message.received == 0 {
            message.notification = piece.notification;
            message.info = piece.info;
            message.from = piece.from;
        }
        message.received = message.received.saturating_add(piece.len);

        if !piece.end_of_record {
            *partial = Some(message);
            continue;
        }
        return finish(message, max_message_size);
    }
}

// Whether `piece` is the next part of `message`. The parts of a message can come from several
// addresses of a multihomed peer: its receive information tells it apart.
fn continues(message: &PartialMessage, piece: &Piece) -> bool {
    let (first, next) = (&message.info, &piece.info);
    piece.notification == message.notification
        && first.assoc_id == next.assoc_id
        && first.sid == next.sid
        && first.ppid == next.ppid
        && first.flags == next.flags
        // The SSN has no meaning for unordered messages.
        && (first.flags & crate::consts::SCTP_UNORDERED != 0 || first.ssn == next.ssn)
}

// A message whose last part has come.
fn finish(
    mut message: PartialMessage,
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
        return Ok(Some(NotificationOrData::Notification(
            notification_from_message(&message.payload),
        )));
    }
    // The buffer had room for the largest read, 4 KiB at least, which a
    // program that keeps many short messages would pay for each
    message.payload.shrink_to_fit();
    Ok(Some(data(message.payload, &message.info, message.from)))
}

// The message of `payload`, with the receive information of its first part.
fn data(payload: Vec<u8>, info: &RcvInfoWire, from: Option<SocketAddr>) -> NotificationOrData {
    NotificationOrData::Data(ReceivedData {
        payload,
        stream: info.sid,
        ppid: u32::from_be(info.ppid),
        assoc_id: info.assoc_id,
        unordered: info.flags & crate::consts::SCTP_UNORDERED != 0,
        from,
    })
}

// One part of a message, `octets`, read elsewhere: it continues the message in parts, or
// begins one. `None` when no message is whole yet, `Some(None)` at the end of the stream.
fn push(
    partial: &mut Option<PartialMessage>,
    octets: &[u8],
    piece: &Piece,
    max_message_size: usize,
) -> std::io::Result<Option<Option<NotificationOrData>>> {
    if piece.len == 0 && !piece.notification {
        *partial = None;
        return Ok(Some(None));
    }
    // A message whose delivery was given up is dropped.
    let Some(mut message) = partial.take().filter(|message| continues(message, piece)) else {
        return match begin(octets, piece, max_message_size)? {
            First::Part(message) => {
                *partial = Some(message);
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
        *partial = Some(message);
        return Ok(None);
    }
    finish(message, max_message_size).map(Some)
}

// The messages and notifications that one batch of reads makes whole, in their order, each as
// `receive_message` returns it: the parts of each are assembled with those that came before,
// and with those of the next batches when it is not whole yet. `pieces` are the reads, whose
// octets `octets` gives.
pub(super) fn receive_batch<'a>(
    partial: &mut Option<PartialMessage>,
    max_message_size: usize,
    pieces: impl IntoIterator<Item = (&'a [u8], &'a Piece)>,
    received: &mut Vec<std::io::Result<Option<NotificationOrData>>>,
) {
    for (octets, piece) in pieces {
        if let Some(result) = push(partial, octets, piece, max_message_size).transpose() {
            received.push(result);
        }
    }
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
            return Ok(First::Whole(Some(NotificationOrData::Notification(
                notification_from_message(octets),
            ))));
        }
        Ok(First::Whole(Some(data(
            octets.to_vec(),
            &piece.info,
            piece.from,
        ))))
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

    fn data(payload: &[u8], end_of_record: bool, info: Option<RcvInfoWire>) -> Step {
        Step::Piece(
            payload.to_vec(),
            Piece {
                len: payload.len(),
                notification: false,
                end_of_record,
                info: info.unwrap_or_default(),
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
                info: RcvInfoWire::default(),
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
            let mut partial = None;
            let message: Vec<u8> = (0..octets).map(|i| i as u8).collect();
            let read = kernel(message.clone(), &reads);
            let received = payload(receive_message(&mut partial, 1 << 20, read).unwrap());
            assert_eq!(received, message);
            assert_eq!(received.capacity(), octets);
            assert_eq!(reads.get(), 1, "{octets} octets");
        }
        // A longer one takes the reads that it needs.
        let reads = std::cell::Cell::new(0);
        let mut partial = None;
        let message = vec![7; 200_000];
        let received = payload(
            receive_message(&mut partial, 1 << 20, kernel(message.clone(), &reads)).unwrap(),
        );
        assert_eq!((received == message, received.capacity()), (true, 200_000));
        assert!(reads.get() <= 3, "{} reads", reads.get());
    }

    /// The buffer of a read has room for a whole packet of the kernel, and a
    /// message is mostly much shorter: what `recv` returns keeps the octets
    /// of the message and no more, in one read or in several.
    #[test]
    fn a_message_keeps_no_more_memory_than_its_octets() {
        let mut partial = None;
        let read = reader(vec![data(b"sixty-four octets, say", true, None)]);
        let short = payload(receive_message(&mut partial, 1 << 20, read).unwrap());
        assert_eq!(short.capacity(), short.len());

        let (first, last) = (vec![1; 4000], vec![2; 300]);
        let read = reader(vec![data(&first, false, None), data(&last, true, None)]);
        let long = payload(receive_message(&mut partial, 1 << 20, read).unwrap());
        assert_eq!((long.len(), long.capacity()), (4300, 4300));
    }

    #[test]
    fn interrupted_read_retains_the_partial_record() {
        let mut partial = None;
        let read = reader(vec![
            data(b"first", false, None),
            Step::Error(std::io::ErrorKind::Interrupted),
            data(b"last", true, None),
        ]);
        assert_eq!(
            payload(receive_message(&mut partial, 100, read).unwrap()),
            b"firstlast"
        );
        assert!(partial.is_none());
    }

    #[test]
    fn would_block_and_resume_preserve_the_receive_information() {
        let first = RcvInfoWire {
            sid: 2,
            ppid: 60_u32.to_be(),
            flags: crate::consts::SCTP_UNORDERED,
            assoc_id: 7,
            ..Default::default()
        };
        let mut read = reader(vec![
            data(b"first", false, Some(first)),
            Step::Error(std::io::ErrorKind::WouldBlock),
            data(b"last", true, Some(first)),
        ]);
        let mut partial = None;
        assert_eq!(
            receive_message(&mut partial, 100, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(partial.as_ref().unwrap().payload, b"first");
        let Some(NotificationOrData::Data(record)) =
            receive_message(&mut partial, 100, read).unwrap()
        else {
            panic!("not data");
        };
        assert_eq!(record.payload, b"firstlast");
        let info = (
            record.stream,
            record.ppid,
            record.unordered,
            record.assoc_id,
        );
        assert_eq!(info, (2, 60, true, 7));
        assert!(partial.is_none());
    }

    #[test]
    fn oversized_record_is_drained_before_the_next_record() {
        let mut partial = None;
        let mut read = reader(vec![
            data(b"12345", false, None),
            data(b"67", true, None),
            data(b"ok", true, None),
        ]);
        assert_eq!(
            receive_message(&mut partial, 4, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert!(partial.is_none());
        assert_eq!(
            payload(receive_message(&mut partial, 4, read).unwrap()),
            b"ok"
        );
    }

    #[test]
    fn reducing_limit_after_cancellation_drains_the_record() {
        let mut partial = None;
        let mut read = reader(vec![
            data(b"12345678", false, None),
            Step::Error(std::io::ErrorKind::WouldBlock),
            data(b"9", true, None),
            data(b"ok", true, None),
        ]);
        assert_eq!(
            receive_message(&mut partial, 10, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(
            receive_message(&mut partial, 4, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            payload(receive_message(&mut partial, 4, read).unwrap()),
            b"ok"
        );
    }

    #[test]
    fn aborted_record_does_not_contaminate_another_association() {
        let mut partial = None;
        let read = reader(vec![
            data(
                b"aborted",
                false,
                Some(RcvInfoWire {
                    assoc_id: 7,
                    ..Default::default()
                }),
            ),
            data(
                b"new",
                true,
                Some(RcvInfoWire {
                    assoc_id: 8,
                    ..Default::default()
                }),
            ),
        ]);
        assert_eq!(
            payload(receive_message(&mut partial, 100, read).unwrap()),
            b"new"
        );
    }

    fn of(assoc_id: i32) -> Option<RcvInfoWire> {
        Some(RcvInfoWire {
            assoc_id,
            ..Default::default()
        })
    }

    #[test]
    fn a_record_whose_pieces_come_from_two_peer_addresses_is_one() {
        let piece = |payload: &[u8], end_of_record, from: &str| {
            let mut step = data(payload, end_of_record, of(7));
            if let Step::Piece(_, piece) = &mut step {
                piece.from = Some(from.parse().unwrap());
            }
            step
        };
        let pieces = vec![
            piece(b"first", false, "192.0.2.1:38412"),
            piece(b"last", true, "198.51.100.1:38412"),
        ];
        let received = receive_message(&mut None, 100, reader(pieces));
        assert_eq!(payload(received.unwrap()), b"firstlast");
    }

    #[test]
    fn notification_after_partial_delivery_keeps_its_original_octets() {
        let raw = (crate::Event::PartialDelivery as u16)
            .to_ne_bytes()
            .to_vec();
        let mut partial = None;
        let read = reader(vec![
            data(b"aborted", false, None),
            Step::Piece(
                raw.clone(),
                Piece {
                    len: raw.len(),
                    notification: true,
                    end_of_record: true,
                    info: RcvInfoWire::default(),
                    from: None,
                },
            ),
        ]);
        assert_eq!(
            receive_message(&mut partial, 100, read).unwrap(),
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
        let first = RcvInfoWire {
            assoc_id: 7,
            sid: 2,
            flags: 1,
            ssn: 1,
            ..Default::default()
        };
        let last = RcvInfoWire { ssn: 99, ..first };
        let read = reader(vec![
            data(b"first", false, Some(first)),
            data(b"last", true, Some(last)),
        ]);
        assert_eq!(
            payload(receive_message(&mut None, 100, read).unwrap()),
            b"firstlast"
        );
    }

    #[test]
    fn eof_discards_the_incomplete_record() {
        let mut partial = None;
        let read = reader(vec![data(b"partial", false, None), data(b"", false, None)]);
        assert_eq!(receive_message(&mut partial, 100, read).unwrap(), None);
        assert!(partial.is_none());
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
                    info: RcvInfoWire::default(),
                    from: None,
                },
            )
        };
        let expected = Some(NotificationOrData::Notification(
            crate::Notification::Shutdown(crate::Shutdown { assoc_id: 7 }),
        ));
        // A limit of 10 octets gives reads of 11: the notification of 12 comes in two parts
        let mut partial = None;
        let mut read = reader(vec![
            part(&shutdown[..11], false),
            part(&shutdown[11..], true),
            data(b"01234567890", true, None),
            data(b"ok", true, None),
        ]);
        assert_eq!(
            receive_message(&mut partial, 10, &mut read).unwrap(),
            expected
        );
        // A message of the peer over the limit is still discarded
        assert_eq!(
            receive_message(&mut partial, 10, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            payload(receive_message(&mut partial, 10, read).unwrap()),
            b"ok"
        );
    }
    #[test]
    fn receive_growth_stays_within_the_message_limit() {
        use std::os::unix::net::UnixDatagram;
        let (socket, _peer) = UnixDatagram::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        for limit in [1000, 3 << 20, DEFAULT_MAX_MESSAGE_SIZE] {
            let mut partial = Some(PartialMessage {
                payload: vec![0; limit],
                received: limit,
                ..Default::default()
            });
            assert_eq!(
                receive_message(&mut partial, limit, |buffer| {
                    super::super::sys::sctp_recvmsg_once(socket.as_fd(), buffer)
                })
                .unwrap_err()
                .kind(),
                std::io::ErrorKind::WouldBlock
            );
            let message = partial.take().unwrap();
            assert_eq!(message.payload.len(), limit);
            assert!(
                message.payload.capacity() <= limit + 1,
                "{} octets allocated for limit {}",
                message.payload.capacity(),
                limit
            );
        }
    }

    /// The end of the stream read first is the end.
    #[test]
    fn the_end_of_the_stream_read_first_is_the_end() {
        let mut partial = None;
        let read = reader(vec![data(b"", false, None)]);
        assert_eq!(receive_message(&mut partial, 100, read).unwrap(), None);
        assert!(partial.is_none());
    }

    /// A read that fails otherwise than by blocking drops the message in assembly: only one
    /// that would block keeps it, for the next read.
    #[test]
    fn a_failed_read_drops_the_message_in_assembly() {
        let mut partials = None;
        let read = reader(vec![
            data(b"first", false, None),
            Step::Error(std::io::ErrorKind::ConnectionReset),
        ]);
        let error = receive_message(&mut partials, 100, read).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        assert!(partials.is_none());
    }

    /// A read that would block keeps a message of which octets were received, and not one
    /// of which none were.
    #[test]
    fn a_read_that_would_block_keeps_a_message_of_octets_alone() {
        let mut partials = Some(PartialMessage::default());
        let read = reader(vec![Step::Error(std::io::ErrorKind::WouldBlock)]);
        let error = receive_message(&mut partials, 100, read).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(partials.is_none());
    }

    /// The parts of an ordered message have its stream sequence number: a part of another
    /// begins another message.
    #[test]
    fn the_parts_of_an_ordered_message_have_its_stream_sequence_number() {
        let first = RcvInfoWire {
            assoc_id: 7,
            sid: 2,
            ssn: 1,
            ..Default::default()
        };
        let other = RcvInfoWire { ssn: 2, ..first };
        let read = reader(vec![
            data(b"first", false, Some(first)),
            data(b"other", true, Some(other)),
        ]);
        let received = receive_message(&mut None, 100, read).unwrap();
        assert_eq!(payload(received), b"other");
    }

    /// An interrupted first read of a message is tried again and loses nothing.
    #[test]
    fn an_interrupted_read_is_tried_again() {
        let read = reader(vec![
            Step::Error(std::io::ErrorKind::Interrupted),
            data(b"whole", true, None),
        ]);
        let received = receive_message(&mut None, 100, read).unwrap();
        assert_eq!(payload(received), b"whole");
    }

    /// A part of another message of the association drops the message in parts, whose
    /// delivery was given up.
    #[test]
    fn a_part_of_another_message_drops_the_message_of_its_association() {
        let stream = |sid| {
            Some(RcvInfoWire {
                assoc_id: 7,
                sid,
                ..Default::default()
            })
        };
        let mut partial = None;
        let read = reader(vec![
            data(b"given up", false, stream(1)),
            data(b"new", true, stream(2)),
        ]);
        let received = receive_message(&mut partial, 100, read).unwrap();
        assert_eq!(payload(received), b"new");
        assert!(partial.is_none());
    }

    /// A message of exactly the limit is whole, in parts.
    #[test]
    fn a_message_of_the_limit_is_whole() {
        let read = reader(vec![
            data(b"01234", false, of(7)),
            data(b"56789", true, of(7)),
        ]);
        let received = receive_message(&mut None, 10, read).unwrap();
        assert_eq!(payload(received), b"0123456789");
    }

    /// The first part of a message is kept up to the limit: past it, the message is
    /// discarded at its end and nothing of it is kept meanwhile.
    #[test]
    fn the_first_part_of_a_message_is_kept_up_to_the_limit() {
        for (part, kept) in [(&b"1234"[..], 4), (b"12345", 0)] {
            let mut partial = None;
            let read = reader(vec![
                data(part, false, of(7)),
                Step::Error(std::io::ErrorKind::WouldBlock),
            ]);
            let error = receive_message(&mut partial, 4, read).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            let message = partial.unwrap();
            assert_eq!(
                message.payload.len(),
                kept,
                "a first part of {} octets",
                part.len()
            );
        }
    }

    /// In a batch of reads, a part that does not continue the message in parts drops it,
    /// whose delivery was given up, and the parts of the next message are assembled.
    #[test]
    fn a_batch_drops_a_message_that_the_next_part_does_not_continue() {
        let part = |octets: &'static [u8], end_of_record, assoc_id| {
            let piece = Piece {
                len: octets.len(),
                notification: false,
                end_of_record,
                info: of(assoc_id).unwrap(),
                from: None,
            };
            (octets, piece)
        };
        let pieces = [
            part(b"given up", false, 7),
            part(b"whole", true, 8),
            part(b"first ", false, 8),
            part(b"last", true, 8),
        ];
        let (mut partial, mut received) = (None, Vec::new());
        let pieces = pieces.iter().map(|(octets, piece)| (*octets, piece));
        receive_batch(&mut partial, 100, pieces, &mut received);
        let payloads: Vec<Vec<u8>> = received
            .into_iter()
            .map(|received| payload(received.unwrap()))
            .collect();
        assert_eq!(payloads, [b"whole".to_vec(), b"first last".to_vec()]);
        assert!(partial.is_none());
    }

    /// A message in assembly prints where it comes from and how much of it came, and not its
    /// octets.
    #[test]
    fn a_message_in_assembly_prints_without_its_octets() {
        let message = PartialMessage {
            notification: false,
            payload: b"secret".to_vec(),
            info: of(7).unwrap(),
            from: Some("192.0.2.1:38412".parse().unwrap()),
            received: 6,
        };
        let printed = format!("{message:?}");
        assert!(printed.starts_with("PartialMessage {"), "{printed}");
        for part in [
            "notification: false",
            "assoc_id: 7",
            "192.0.2.1:38412",
            "received: 6",
        ] {
            assert!(printed.contains(part), "{part} in {printed}");
        }
        assert!(!printed.contains("payload"), "{printed}");
    }

    /// The end of the stream ends a message that it cuts, and one that has no part yet.
    #[test]
    fn the_end_of_the_stream_ends_a_message() {
        let read = reader(vec![data(b"partial", false, None), data(b"", false, None)]);
        assert_eq!(receive_message(&mut None, 100, read).unwrap(), None);
        let mut partials = Some(PartialMessage::default());
        let read = reader(vec![data(b"", false, None)]);
        assert_eq!(receive_message(&mut partials, 100, read).unwrap(), None);
    }

    /// A message of which nothing was received is begun by the next part, from whatever
    /// address.
    #[test]
    fn a_message_of_no_octets_is_begun_by_the_next_part() {
        let mut partials = Some(PartialMessage::default());
        let mut piece = data(b"whole", true, of(7));
        if let Step::Piece(_, piece) = &mut piece {
            piece.from = Some("192.0.2.1:38412".parse().unwrap());
        }
        let received = receive_message(&mut partials, 100, reader(vec![piece]));
        assert_eq!(payload(received.unwrap()), b"whole");
    }
}
