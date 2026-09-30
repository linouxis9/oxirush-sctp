//! Notification decoding from initialized native-format records; no socket operations.
use crate::consts::{SCTP_ASSOC_CHANGE, SCTP_SHUTDOWN};
use crate::{
    AssocChangeState, AssociationChange, Event, Notification, PeerAddressChange, PeerAddressState,
    SendFailure, SendInfo, Shutdown,
};
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
use std::net::SocketAddr;

pub(crate) fn notification_from_message(data: &[u8]) -> Notification {
    // `struct sctp_assoc_change` without `sac_info` and `struct sctp_shutdown_event`.
    const ASSOC_CHANGE_LEN: usize = 20;
    const SHUTDOWN_LEN: usize = 12;

    let unsupported = || Notification::Unsupported {
        ev_type: data.get(0..2).map_or(Event::Unknown, |ev_type| {
            Event::from_u16(u16::from_ne_bytes(ev_type.try_into().unwrap()))
        }),
        data: data.to_vec(),
    };
    if data.len() < 2 {
        log::warn!("Notification of {} octets.", data.len());
        return unsupported();
    }
    let notification_type = u16::from_ne_bytes(data[0..2].try_into().unwrap());
    if data.len() < 8 || u32::from_ne_bytes(data[4..8].try_into().unwrap()) as usize != data.len() {
        return unsupported();
    }
    log::trace!(
        "notification_type: {:x}, SCTP_ASSOC_CHANGE: {:x}",
        notification_type,
        SCTP_ASSOC_CHANGE
    );
    match notification_type {
        value if value == Event::Address as u16 && data.len() == 148 => {
            let Some(address) = socket_address(&data[8..136]) else {
                return unsupported();
            };
            Notification::PeerAddressChange(PeerAddressChange {
                flags: u16::from_ne_bytes(data[2..4].try_into().unwrap()),
                address,
                state: PeerAddressState::from_i32(i32::from_ne_bytes(
                    data[136..140].try_into().unwrap(),
                )),
                error: i32::from_ne_bytes(data[140..144].try_into().unwrap()),
                assoc_id: i32::from_ne_bytes(data[144..148].try_into().unwrap()),
            })
        }
        value if value == Event::SendFailureEvent as u16 && data.len() >= 32 => {
            Notification::SendFailure(SendFailure {
                flags: u16::from_ne_bytes(data[2..4].try_into().unwrap()),
                error: u32::from_ne_bytes(data[8..12].try_into().unwrap()),
                snd_info: SendInfo {
                    sid: u16::from_ne_bytes(data[12..14].try_into().unwrap()),
                    flags: u16::from_ne_bytes(data[14..16].try_into().unwrap()),
                    ppid: u32::from_ne_bytes(data[16..20].try_into().unwrap()),
                    context: u32::from_ne_bytes(data[20..24].try_into().unwrap()),
                    assoc_id: i32::from_ne_bytes(data[24..28].try_into().unwrap()),
                },
                assoc_id: i32::from_ne_bytes(data[28..32].try_into().unwrap()),
                payload: data[32..].to_vec(),
            })
        }
        SCTP_ASSOC_CHANGE if data.len() >= ASSOC_CHANGE_LEN => {
            log::debug!("SCTP_ASSOC_CHANGE Notification Received.");
            let assoc_change = AssociationChange {
                ev_type: Event::from_u16(u16::from_ne_bytes(data[0..2].try_into().unwrap())),
                flags: u16::from_ne_bytes(data[2..4].try_into().unwrap()),
                length: u32::from_ne_bytes(data[4..8].try_into().unwrap()),
                state: AssocChangeState::from_u16(u16::from_ne_bytes(
                    data[8..10].try_into().unwrap(),
                )),
                error: u16::from_ne_bytes(data[10..12].try_into().unwrap()),
                ob_streams: u16::from_ne_bytes(data[12..14].try_into().unwrap()),
                ib_streams: u16::from_ne_bytes(data[14..16].try_into().unwrap()),
                assoc_id: i32::from_ne_bytes(data[16..20].try_into().unwrap()),
                info: data[20..].into(),
            };
            Notification::AssociationChange(assoc_change)
        }
        SCTP_SHUTDOWN if data.len() == SHUTDOWN_LEN => {
            log::debug!("SCTP_SHUTDOWN Notification Received.");
            let shutdown = Shutdown {
                ev_type: Event::from_u16(u16::from_ne_bytes(data[0..2].try_into().unwrap())),
                flags: u16::from_ne_bytes(data[2..4].try_into().unwrap()),
                length: u32::from_ne_bytes(data[4..8].try_into().unwrap()),
                assoc_id: i32::from_ne_bytes(data[8..12].try_into().unwrap()),
            };
            Notification::Shutdown(shutdown)
        }
        _ => {
            log::debug!(
                "Unsupported notification received: type {:x}, {} octets.",
                notification_type,
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

    #[test]
    fn declared_notification_lengths_and_fixed_shutdown_size_are_enforced() {
        for (event, actual, declared) in [
            (Event::Association, 20, 19),
            (Event::Association, 23, 20),
            (Event::Shutdown, 12, 13),
            (Event::Shutdown, 15, 15),
        ] {
            let mut bytes = vec![0; actual];
            bytes[..2].copy_from_slice(&(event.clone() as u16).to_ne_bytes());
            bytes[4..8].copy_from_slice(&(declared as u32).to_ne_bytes());
            assert_eq!(
                notification_from_message(&bytes),
                Notification::Unsupported {
                    ev_type: event,
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
            let mut bytes = vec![0_u8; 148];
            bytes[..2].copy_from_slice(&(Event::Address as u16).to_ne_bytes());
            bytes[4..8].copy_from_slice(&148_u32.to_ne_bytes());
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
                    ev_type: Event::Address,
                    data: bytes.clone()
                }
            );
        }
    }
    #[test]
    fn malformed_and_deprecated_send_failures_keep_all_original_bytes() {
        let mut bytes = vec![0_u8; 35];
        bytes[..2].copy_from_slice(&(Event::SendFailureEvent as u16).to_ne_bytes());
        bytes[4..8].copy_from_slice(&35_u32.to_ne_bytes());
        bytes[8..12].copy_from_slice(&(libc::ECONNREFUSED as u32).to_ne_bytes());
        bytes[12..14].copy_from_slice(&3_u16.to_ne_bytes());
        bytes[16..20].copy_from_slice(&60_u32.to_be().to_ne_bytes());
        bytes[20..24].copy_from_slice(&42_u32.to_ne_bytes());
        bytes[24..28].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[28..32].copy_from_slice(&7_i32.to_ne_bytes());
        bytes[32..].copy_from_slice(b"pdu");
        assert_eq!(
            notification_from_message(&bytes),
            Notification::SendFailure(SendFailure {
                flags: 0,
                error: libc::ECONNREFUSED as u32,
                snd_info: SendInfo {
                    sid: 3,
                    ppid: 60_u32.to_be(),
                    context: 42,
                    assoc_id: 7,
                    ..Default::default()
                },
                assoc_id: 7,
                payload: b"pdu".to_vec(),
            })
        );
        bytes[4..8].copy_from_slice(&36_u32.to_ne_bytes());
        assert_eq!(
            notification_from_message(&bytes),
            Notification::Unsupported {
                ev_type: Event::SendFailureEvent,
                data: bytes.clone()
            }
        );
        bytes[..2].copy_from_slice(&(Event::SendFailure as u16).to_ne_bytes());
        assert_eq!(
            notification_from_message(&bytes),
            Notification::Unsupported {
                ev_type: Event::SendFailure,
                data: bytes.clone()
            }
        );
    }
    #[test]
    fn send_options_convert_only_the_logical_ppid() {
        let info = crate::SendOptions {
            stream_id: 5,
            ppid: 60,
            unordered: true,
            context: 42,
            assoc_id: 7,
        }
        .wire_info();
        assert_eq!(
            info,
            SendInfo {
                sid: 5,
                ppid: 60_u32.to_be(),
                flags: 1,
                context: 42,
                assoc_id: 7
            }
        );
    }
    #[test]
    fn short_and_unparsed_notifications_keep_their_type_and_octets() {
        let assoc_change = SCTP_ASSOC_CHANGE.to_ne_bytes();
        let shutdown = SCTP_SHUTDOWN.to_ne_bytes();
        let peer_address_change = (Event::Address as u16).to_ne_bytes();
        for (data, ev_type) in [
            (&[][..], Event::Unknown),
            (&[0x80][..], Event::Unknown),
            (&assoc_change[..], Event::Association),
            (
                &[&assoc_change[..], &[0; 17]].concat()[..],
                Event::Association,
            ),
            (&[&shutdown[..], &[0; 9]].concat()[..], Event::Shutdown),
            (
                &[&peer_address_change[..], &[0; 146]].concat()[..],
                Event::Address,
            ),
        ] {
            assert_eq!(
                notification_from_message(data),
                Notification::Unsupported {
                    ev_type,
                    data: data.to_vec()
                }
            );
        }

        let mut shutdown_event = shutdown.to_vec();
        shutdown_event.extend_from_slice(&0_u16.to_ne_bytes());
        shutdown_event.extend_from_slice(&12_u32.to_ne_bytes());
        shutdown_event.extend_from_slice(&7_i32.to_ne_bytes());
        assert!(matches!(
            notification_from_message(&shutdown_event),
            Notification::Shutdown(Shutdown { assoc_id: 7, .. })
        ));
    }
}
