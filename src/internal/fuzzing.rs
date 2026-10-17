//! Entry points for the fuzz targets of `fuzz/`.
use super::receive::{Partials, Piece, receive_message};
use super::{notifications, sys};
use crate::{Notification, NotificationOrData, NxtInfo, RcvInfo};

/// A notification of any octets.
pub fn notification(octets: &[u8]) -> Notification {
    notifications::notification_from_message(octets)
}

/// The receive information of any control messages.
pub fn ancillary(control: &[u8], truncated: bool) -> (Option<RcvInfo>, Option<NxtInfo>) {
    let flags = if truncated { libc::MSG_CTRUNC } else { 0 };
    sys::ancillary_info(control, flags)
}

/// Assembles messages from the parts that `script` describes, as `recv` does from what
/// `recvmsg` returns: each step is an octet of flags, an octet of length and that many octets.
pub fn receive(script: &[u8]) {
    const LIMIT: usize = 64;
    let mut script = script;
    let mut partial = Partials::default();
    // The parts of the messages of several associations in turns, as on an endpoint, which
    // holds three messages of the longest size in assembly at most.
    partial.set_interleaved(script.first().is_some_and(|first| first & 0x40 != 0));
    partial.set_max_assembly(3 * LIMIT);
    // Every call consumes a step, so the loop ends with the script.
    while !script.is_empty() {
        let received = receive_message(&mut partial, LIMIT, script[0] & 0x80 != 0, |buffer| {
            let [flags, length, rest @ ..] = script else {
                script = &[];
                return Err(std::io::ErrorKind::WouldBlock.into());
            };
            let length = usize::from(*length).min(rest.len()).min(buffer.len());
            let (part, rest) = rest.split_at(length);
            script = rest;
            match flags & 0x07 {
                0 => return Err(std::io::ErrorKind::WouldBlock.into()),
                1 => return Err(std::io::ErrorKind::Interrupted.into()),
                2 => return Err(std::io::ErrorKind::ConnectionReset.into()),
                _ => {}
            }
            buffer[..length].copy_from_slice(part);
            Ok(Piece {
                len: length,
                notification: flags & 0x08 != 0,
                end_of_record: flags & 0x10 != 0,
                rcv_info: (flags & 0x20 != 0).then(|| RcvInfo {
                    assoc_id: i32::from(flags >> 6),
                    ..Default::default()
                }),
                nxt_info: None,
                from: None,
            })
        });
        if let Ok(Some(NotificationOrData::Data(message))) = received {
            assert!(message.payload.len() <= LIMIT);
        }
        partial.take_aborts();
    }
}
