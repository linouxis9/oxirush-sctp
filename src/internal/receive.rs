//! Whole-record assembly, independent of descriptor ownership and readiness.
use super::sys::notification_from_message;
use crate::{NotificationOrData, NxtInfo, RcvInfo, ReceivedData};
use std::net::SocketAddr;

// Messages longer than this are discarded unless the socket sets another limit, so that a peer
// cannot make us buffer without bound.
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 << 20;

// Initial size of the buffer of a message. It doubles for longer messages.
const RECV_BUFFER_SIZE: usize = 4096;

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
pub(super) struct Piece {
    pub(super) len: usize,
    pub(super) notification: bool,
    pub(super) end_of_record: bool,
    pub(super) rcv_info: Option<RcvInfo>,
    pub(super) nxt_info: Option<NxtInfo>,
    pub(super) from: Option<SocketAddr>,
}

// Reads the next message up to its end (`MSG_EOR`), or fails with `EWOULDBLOCK` after keeping the
// part received so far in `partial`.
//
// With the default `SCTP_FRAGMENT_INTERLEAVE` level 0, the parts of a message are not interleaved
// with other messages. The kernel only delivers something else after a part when it aborts the
// delivery of the message, e.g. because the association is aborted.
pub(super) fn receive_message(
    partial: &mut Option<PartialMessage>,
    max_message_size: usize,
    mut read: impl FnMut(&mut [u8]) -> std::io::Result<Piece>,
) -> std::io::Result<NotificationOrData> {
    loop {
        let mut message = partial.take().unwrap_or_default();
        let discarding = message.received > max_message_size;
        if discarding {
            message.payload.clear();
        }
        let start = message.payload.len();
        let room = if discarding {
            DISCARD_BUFFER_SIZE
        } else {
            start
                .max(RECV_BUFFER_SIZE)
                .min(max_message_size.saturating_sub(start).saturating_add(1))
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
            if message.received > 0 {
                log::warn!(
                    "End of stream after {} octets of a message.",
                    message.received
                );
            }
            log::debug!("Received end of stream.");
            return Ok(NotificationOrData::Data(ReceivedData {
                payload: vec![],
                from: None,
                rcv_info: None,
                nxt_info: None,
            }));
        }

        let continued = message.received > 0;
        let same_message = match (&message.rcv_info, &piece.rcv_info) {
            (Some(first), Some(next)) => {
                first.assoc_id == next.assoc_id
                    && first.sid == next.sid
                    && first.ppid == next.ppid
                    && first.flags == next.flags
                    // The SSN has no meaning for unordered messages.
                    && (first.flags & 1 != 0 || first.ssn == next.ssn)
            }
            _ => message.from == piece.from,
        };
        if continued && (piece.notification != message.notification || !same_message) {
            log::warn!(
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
            *partial = Some(message);
            continue;
        }
        if message.received > max_message_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "discarded a message of {} octets, longer than {}",
                    message.received, max_message_size
                ),
            ));
        }
        if message.notification {
            log::debug!("Received Notification.");
            return Ok(NotificationOrData::Notification(notification_from_message(
                &message.payload,
            )));
        }
        log::debug!("Received Data.");
        return Ok(NotificationOrData::Data(ReceivedData {
            payload: message.payload,
            from: message.from,
            rcv_info: message.rcv_info,
            nxt_info: piece.nxt_info,
        }));
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

    fn payload(record: NotificationOrData) -> Vec<u8> {
        match record {
            NotificationOrData::Data(data) => data.payload,
            other => panic!("unexpected {:?}", other),
        }
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
        let mut partial = None;
        assert_eq!(
            receive_message(&mut partial, 100, &mut read)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(partial.as_ref().unwrap().payload, b"first");
        let NotificationOrData::Data(record) = receive_message(&mut partial, 100, read).unwrap()
        else {
            panic!("not data");
        };
        assert_eq!(record.payload, b"firstlast");
        assert_eq!(record.rcv_info, Some(first));
        assert_eq!(record.nxt_info, Some(next));
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
            payload(receive_message(&mut partial, 100, read).unwrap()),
            b"new"
        );
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
                    rcv_info: None,
                    nxt_info: None,
                    from: None,
                },
            ),
        ]);
        assert_eq!(
            receive_message(&mut partial, 100, read).unwrap(),
            NotificationOrData::Notification(crate::Notification::Unsupported {
                ev_type: crate::Event::PartialDelivery,
                data: raw,
            })
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
            payload(receive_message(&mut None, 100, read).unwrap()),
            b"firstlast"
        );
    }

    #[test]
    fn eof_discards_the_incomplete_record() {
        let mut partial = None;
        let read = reader(vec![data(b"partial", false, None), data(b"", false, None)]);
        assert!(payload(receive_message(&mut partial, 100, read).unwrap()).is_empty());
        assert!(partial.is_none());
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
            let message = partial.unwrap();
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
