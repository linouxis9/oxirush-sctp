//! Notification decoding from initialized native-format records; no socket operations.

use crate::{
    AssocChangeState, AssociationChange, Event, Notification, PartialDelivery, PeerAddressChange,
    PeerAddressState, RemoteError, SendFailure, SendInfo, Shutdown,
};
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
use std::net::SocketAddr;

pub(crate) fn notification_from_message(data: &[u8]) -> Notification {
    // `struct sctp_assoc_change` without `sac_info`, and the notifications of a fixed length.
    const ASSOC_CHANGE_LEN: usize = 20;
    const SHUTDOWN_LEN: usize = 12;
    const SENDER_DRY_LEN: usize = 12;
    const PARTIAL_DELIVERY_LEN: usize = 24;
    const REMOTE_ERROR_LEN: usize = 16;

    let u16_at = |at: usize| u16::from_ne_bytes(data[at..at + 2].try_into().unwrap());
    let u32_at = |at: usize| u32::from_ne_bytes(data[at..at + 4].try_into().unwrap());
    let i32_at = |at: usize| i32::from_ne_bytes(data[at..at + 4].try_into().unwrap());
    let unsupported = || Notification::Unsupported {
        kind: if data.len() < 2 { 0 } else { u16_at(0) },
        data: data.to_vec(),
    };

    if data.len() < 2 {
        tracing::warn!("Notification of {} octets.", data.len());
        return unsupported();
    }
    let kind = u16_at(0);
    if data.len() < 8 || u32_at(4) as usize != data.len() {
        return unsupported();
    }

    match kind {
        kind if kind == Event::Address as u16 && data.len() == 148 => {
            let Some(address) = socket_address(&data[8..136]) else {
                return unsupported();
            };
            Notification::PeerAddressChange(PeerAddressChange {
                flags: u16_at(2),
                address,
                state: PeerAddressState::from_i32(i32_at(136)),
                error: i32_at(140),
                assoc_id: i32_at(144),
            })
        }
        kind if kind == Event::SendFailureEvent as u16 && data.len() >= 32 => {
            Notification::SendFailure(SendFailure {
                sent: u16_at(2) & 1 != 0,
                // The cause code of SCTP, which Linux stores in network byte order in a
                // number of 32 bits
                cause: u16::from_be(u32_at(8) as u16),
                snd_info: SendInfo {
                    sid: u16_at(12),
                    flags: u16_at(14),
                    ppid: u32::from_be(u32_at(16)),
                    context: u32_at(20),
                    assoc_id: i32_at(24),
                },
                assoc_id: i32_at(28),
                payload: data[32..].to_vec(),
            })
        }
        kind if kind == Event::Association as u16 && data.len() >= ASSOC_CHANGE_LEN => {
            Notification::AssociationChange(AssociationChange {
                state: AssocChangeState::from_u16(u16_at(8)),
                cause: u16::from_be(u16_at(10)),
                outbound_streams: u16_at(12),
                inbound_streams: u16_at(14),
                assoc_id: i32_at(16),
                info: data[20..].into(),
            })
        }
        kind if kind == Event::Shutdown as u16 && data.len() == SHUTDOWN_LEN => {
            Notification::Shutdown(Shutdown {
                assoc_id: i32_at(8),
            })
        }
        kind if kind == Event::PeerError as u16 && data.len() >= REMOTE_ERROR_LEN => {
            Notification::RemoteError(RemoteError {
                cause: u16::from_be(u16_at(8)),
                assoc_id: i32_at(12),
                data: data[16..].to_vec(),
            })
        }
        kind if kind == Event::PartialDelivery as u16 && data.len() == PARTIAL_DELIVERY_LEN => {
            Notification::PartialDelivery(PartialDelivery {
                indication: u32_at(8),
                assoc_id: i32_at(12),
                stream: u32_at(16),
                sequence: u32_at(20),
            })
        }
        kind if kind == Event::SenderDry as u16 && data.len() == SENDER_DRY_LEN => {
            Notification::SenderDry {
                assoc_id: i32_at(8),
            }
        }
        _ => {
            tracing::debug!(
                "Unsupported notification received: type {kind:x}, {} octets.",
                data.len()
            );
            unsupported()
        }
    }
}

fn socket_address(storage: &[u8]) -> Option<SocketAddr> {
    let family = u16::from_ne_bytes(storage.get(..2)?.try_into().ok()?) as i32;
    let len = match family {
        libc::AF_INET => std::mem::size_of::<libc::sockaddr_in>(),
        libc::AF_INET6 => std::mem::size_of::<libc::sockaddr_in6>(),
        _ => return None,
    };
    let bytes = storage.get(..len)?;
    let mut address = OsSocketAddr::new();
    address.as_mut()[..len].copy_from_slice(bytes);
    address.into_addr()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A notification of a type and a length, with the length that it declares.
    fn notification(event: Event, len: usize) -> Vec<u8> {
        let mut bytes = vec![0_u8; len];
        bytes[..2].copy_from_slice(&(event as u16).to_ne_bytes());
        bytes[4..8].copy_from_slice(&(len as u32).to_ne_bytes());
        bytes
    }

    #[test]
    fn declared_notification_lengths_and_fixed_shutdown_size_are_enforced() {
        for (event, actual, declared) in [
            (Event::Association, 20, 19),
            (Event::Association, 23, 20),
            (Event::Shutdown, 12, 13),
            (Event::Shutdown, 15, 15),
            (Event::SenderDry, 13, 13),
            (Event::PartialDelivery, 23, 23),
            (Event::PeerError, 15, 15),
        ] {
            let mut bytes = notification(event, actual);
            bytes[4..8].copy_from_slice(&(declared as u32).to_ne_bytes());
            assert_eq!(
                notification_from_message(&bytes),
                Notification::Unsupported {
                    kind: event as u16,
                    data: bytes
                }
            );
        }
    }

    #[test]
    fn peer_address_notifications_parse_ipv4_ipv6_and_unknown_states() {
        for address in [
            "192.0.2.1:38412".parse::<SocketAddr>().unwrap(),
            "[fe80::1%7]:38412".parse::<SocketAddr>().unwrap(),
        ] {
            let mut bytes = notification(Event::Address, 148);
            let storage = OsSocketAddr::from(address);
            bytes[8..8 + storage.len() as usize].copy_from_slice(storage.as_ref());
            bytes[136..140].copy_from_slice(&1_i32.to_ne_bytes());
            bytes[140..144].copy_from_slice(&libc::ETIMEDOUT.to_ne_bytes());
            bytes[144..148].copy_from_slice(&7_i32.to_ne_bytes());
            assert_eq!(
                notification_from_message(&bytes),
                Notification::PeerAddressChange(PeerAddressChange {
                    flags: 0,
                    address,
                    state: PeerAddressState::Unreachable,
                    error: libc::ETIMEDOUT,
                    assoc_id: 7,
                })
            );

            bytes[136..140].copy_from_slice(&99_i32.to_ne_bytes());
            assert!(matches!(
                notification_from_message(&bytes),
                Notification::PeerAddressChange(PeerAddressChange {
                    state: PeerAddressState::Unknown(99),
                    ..
                })
            ));

            bytes[8..10].fill(0);
            assert_eq!(
                notification_from_message(&bytes),
                Notification::Unsupported {
                    kind: Event::Address as u16,
                    data: bytes.clone()
                }
            );
        }
    }

    /// The cause of a send failure is the error cause of SCTP, which Linux gives in network
    /// byte order: 12, a user-initiated abort, is the number 3072 on a little-endian
    /// machine. The payload protocol identifier is a number as well.
    #[test]
    fn a_send_failure_has_its_cause_and_its_identifier_as_numbers() {
        let mut bytes = notification(Event::SendFailureEvent, 35);
        bytes[2..4].copy_from_slice(&1_u16.to_ne_bytes());
        bytes[8..12].copy_from_slice(&u32::from(12_u16.to_be()).to_ne_bytes());
        bytes[12..14].copy_from_slice(&3_u16.to_ne_bytes());
        bytes[16..20].copy_from_slice(&60_u32.to_be().to_ne_bytes());
        bytes[20..24].copy_from_slice(&42_u32.to_ne_bytes());
        bytes[24..28].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[28..32].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[32..].copy_from_slice(b"pdu");
        assert_eq!(
            notification_from_message(&bytes),
            Notification::SendFailure(SendFailure {
                sent: true,
                cause: 12,
                snd_info: SendInfo {
                    sid: 3,
                    ppid: 60,
                    context: 42,
                    assoc_id: 7,
                    ..Default::default()
                },
                assoc_id: 7,
                payload: b"pdu".to_vec(),
            })
        );

        // One whose length is not the one it declares, and the deprecated one
        bytes[4..8].copy_from_slice(&36_u32.to_ne_bytes());
        assert_eq!(
            notification_from_message(&bytes),
            Notification::Unsupported {
                kind: Event::SendFailureEvent as u16,
                data: bytes.clone()
            }
        );
        bytes[..2].copy_from_slice(&(Event::SendFailure as u16).to_ne_bytes());
        assert_eq!(
            notification_from_message(&bytes),
            Notification::Unsupported {
                kind: Event::SendFailure as u16,
                data: bytes.clone()
            }
        );
    }

    #[test]
    fn an_association_change_has_its_cause_as_a_number() {
        let mut bytes = notification(Event::Association, 24);
        bytes[8..10].copy_from_slice(&1_u16.to_ne_bytes());
        bytes[10..12].copy_from_slice(&12_u16.to_be().to_ne_bytes());
        bytes[12..14].copy_from_slice(&10_u16.to_ne_bytes());
        bytes[14..16].copy_from_slice(&5_u16.to_ne_bytes());
        bytes[16..20].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[20..].copy_from_slice(&[0, 12, 0, 4]);
        assert_eq!(
            notification_from_message(&bytes),
            Notification::AssociationChange(AssociationChange {
                state: AssocChangeState::CommLost,
                cause: 12,
                outbound_streams: 10,
                inbound_streams: 5,
                assoc_id: 7,
                info: vec![0, 12, 0, 4],
            })
        );

        bytes[8..10].copy_from_slice(&9_u16.to_ne_bytes());
        assert!(matches!(
            notification_from_message(&bytes),
            Notification::AssociationChange(AssociationChange {
                state: AssocChangeState::Unknown(9),
                ..
            })
        ));
    }

    #[test]
    fn remote_errors_partial_deliveries_and_sender_dry_are_typed() {
        let mut bytes = notification(Event::PeerError, 20);
        bytes[8..10].copy_from_slice(&9_u16.to_be().to_ne_bytes());
        bytes[12..16].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[16..].copy_from_slice(&[0, 9, 0, 4]);
        assert_eq!(
            notification_from_message(&bytes),
            Notification::RemoteError(RemoteError {
                cause: 9,
                assoc_id: 7,
                data: vec![0, 9, 0, 4],
            })
        );

        let mut bytes = notification(Event::PartialDelivery, 24);
        bytes[12..16].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[16..20].copy_from_slice(&3_u32.to_ne_bytes());
        bytes[20..24].copy_from_slice(&41_u32.to_ne_bytes());
        assert_eq!(
            notification_from_message(&bytes),
            Notification::PartialDelivery(PartialDelivery {
                indication: 0,
                assoc_id: 7,
                stream: 3,
                sequence: 41,
            })
        );

        let mut bytes = notification(Event::SenderDry, 12);
        bytes[8..12].copy_from_slice(&7_i32.to_ne_bytes());
        assert_eq!(
            notification_from_message(&bytes),
            Notification::SenderDry { assoc_id: 7 }
        );
    }

    #[test]
    fn send_options_give_the_send_information_of_a_message() {
        let info = crate::SendOptions::new()
            .stream(5)
            .ppid(60)
            .unordered(true)
            .context(42)
            .association(7)
            .info();
        let mut expected = SendInfo::default();
        (expected.sid, expected.ppid, expected.flags) = (5, 60, 1);
        (expected.context, expected.assoc_id) = (42, 7);
        assert_eq!(info, expected);
    }

    #[test]
    fn short_and_unparsed_notifications_keep_their_type_and_octets() {
        let assoc_change = (Event::Association as u16).to_ne_bytes();
        let shutdown = (Event::Shutdown as u16).to_ne_bytes();
        let peer_address_change = (Event::Address as u16).to_ne_bytes();
        for (data, kind) in [
            (&[][..], 0),
            (&[0x80][..], 0),
            (&assoc_change[..], Event::Association as u16),
            (
                &[&assoc_change[..], &[0; 17]].concat()[..],
                Event::Association as u16,
            ),
            (
                &[&shutdown[..], &[0; 9]].concat()[..],
                Event::Shutdown as u16,
            ),
            (
                &[&peer_address_change[..], &[0; 146]].concat()[..],
                Event::Address as u16,
            ),
            // A type that the crate does not know keeps its number
            (&[0x0e, 0x80, 0, 0, 8, 0, 0, 0][..], 0x800e),
        ] {
            let data = if cfg!(target_endian = "big") && data.len() == 8 {
                continue;
            } else {
                data
            };
            assert_eq!(
                notification_from_message(data),
                Notification::Unsupported {
                    kind,
                    data: data.to_vec()
                }
            );
        }

        let mut shutdown_event = notification(Event::Shutdown, 12);
        shutdown_event[8..12].copy_from_slice(&7_i32.to_ne_bytes());
        assert_eq!(
            notification_from_message(&shutdown_event),
            Notification::Shutdown(Shutdown { assoc_id: 7 })
        );
    }
}
