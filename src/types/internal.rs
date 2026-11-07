//! Types used by different SCTP Internal APIs
//!
//! Most of these types are 'C' like `struct`s that are passed as parameters as a part of
//! performing certain SCTP related functionality using `libc::getsockopt` or `libc::setsockopt`.
//! Structures below are used by the implementation details and are not part of the public API.

use crate::{AssociationId, Event};

// Structure used by `sctp_peeloff` (Section 9.2)
#[repr(C)]
#[derive(Default, Debug)]
pub(crate) struct PeeloffArg {
    pub(crate) assoc_id: AssociationId,
    pub(crate) sd: libc::c_int,
}

impl PeeloffArg {
    pub(crate) fn from_assoc_id(assoc_id: AssociationId) -> Self {
        Self { assoc_id, sd: 0 }
    }
}

// Structure used by `sctp_peeloff` with flags for the new descriptor
#[repr(C)]
#[derive(Default, Debug)]
pub(crate) struct PeeloffFlagsArg {
    pub(crate) p_arg: PeeloffArg,
    pub(crate) flags: libc::c_uint,
}

// Structure used for Subscribing to SCTP Events
#[repr(C)]
#[derive(Debug)]
pub(crate) struct SubscribeEvent {
    pub(crate) assoc_id: AssociationId,
    pub(crate) event: Event,
    pub(crate) on: bool,
}

// SCTP Initiation Structure (See Section 5.3.1 of RFC 6458)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct InitMsg {
    pub(crate) ostreams: u16,
    pub(crate) istreams: u16,
    pub(crate) retries: u16,
    pub(crate) timeout: u16, // in miliseconds
}

// Structure used by connectx (using SCTP_SOCKOPT_CONNECTX3). This is required to get the
// `assoc_id` in the case of non blocking sockets.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct ConnectxParam {
    pub(crate) assoc_id: AssociationId,
    pub(crate) addrs_size: libc::c_int,
    pub(crate) addrs: *mut u8,
}

// PeerAddress: Structure representing SCTP Peer Address.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct PeerAddrInternal {
    pub assoc_id: AssociationId,
    pub address: libc::sockaddr_storage,
    pub state: i32,
    pub cwnd: u32,
    pub srtt: u32,
    pub rto: u32,
    pub mtu: u32,
}

// ConnStatusInternal: Status of an SCTP Connection
#[repr(C)]
#[derive(Clone)]
pub struct ConnStatusInternal {
    pub assoc_id: AssociationId,
    pub state: i32,
    pub rwnd: u32,
    pub unacked_data: u16,
    pub pending_data: u16,
    pub instreams: u16,
    pub outstreams: u16,
    pub fragmentation_pt: u32,
    pub peer_primary: PeerAddrInternal,
}

// Linux sctp_paddrparams is packed and aligned to 4. Byte fields avoid references to its
// unaligned u32 members; size/offset tests below protect the handwritten UAPI layout.
#[repr(C, align(4))]
pub(crate) struct PeerAddressParamsInternal {
    pub(crate) bytes: [u8; 156],
}

// `struct sctp_sndinfo`, with the payload protocol identifier as the kernel carries it, in
// network byte order.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct SndInfoWire {
    pub(crate) sid: u16,
    pub(crate) flags: u16,
    pub(crate) ppid: u32,
    pub(crate) context: u32,
    pub(crate) assoc_id: AssociationId,
}

impl SndInfoWire {
    // A message with `options` to association `assoc_id`, which a one-to-one socket ignores.
    pub(crate) fn new(options: crate::SendOptions, assoc_id: AssociationId) -> Self {
        Self {
            sid: options.stream_id,
            flags: if options.unordered {
                crate::consts::SCTP_UNORDERED
            } else {
                0
            },
            ppid: options.ppid.to_be(),
            context: options.context,
            assoc_id,
        }
    }

    // A send of no message that only has `flags`: `SCTP_ABORT` or `SCTP_EOF`.
    pub(crate) fn control(flags: u16, assoc_id: AssociationId) -> Self {
        Self {
            flags,
            assoc_id,
            ..Self::default()
        }
    }
}

// `struct sctp_rcvinfo`, with the payload protocol identifier in network byte order.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RcvInfoWire {
    pub(crate) sid: u16,
    pub(crate) ssn: u16,
    pub(crate) flags: u16,
    pub(crate) ppid: u32,
    pub(crate) tsn: u32,
    pub(crate) cumtsn: u32,
    pub(crate) context: u32,
    pub(crate) assoc_id: AssociationId,
}

// `struct sctp_rtoinfo`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct RtoInfoWire {
    pub(crate) assoc_id: AssociationId,
    pub(crate) initial: u32,
    pub(crate) max: u32,
    pub(crate) min: u32,
}

// `struct sctp_assocparams`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct AssocParamsWire {
    pub(crate) assoc_id: AssociationId,
    pub(crate) max_retransmissions: u16,
    pub(crate) peer_addresses: u16,
    pub(crate) peer_rwnd: u32,
    pub(crate) local_rwnd: u32,
    pub(crate) cookie_life: u32,
}

// `struct sctp_assoc_stats`. The observed-RTO address is kept opaque, as
// a `sockaddr_storage`; the counters follow it, each a `__u64`.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct AssocStatsWire {
    pub(crate) assoc_id: AssociationId,
    pub(crate) _pad: u32,
    pub(crate) _obs_rto_ipaddr: [u8; 128],
    pub(crate) max_rto: u64,
    pub(crate) in_sacks: u64,
    pub(crate) out_sacks: u64,
    pub(crate) out_packets: u64,
    pub(crate) in_packets: u64,
    pub(crate) rtx_chunks: u64,
    pub(crate) out_of_sequence_tsns: u64,
    pub(crate) in_dup_chunks: u64,
    pub(crate) gap_count: u64,
    pub(crate) out_unordered_chunks: u64,
    pub(crate) in_unordered_chunks: u64,
    pub(crate) out_ordered_chunks: u64,
    pub(crate) in_ordered_chunks: u64,
    pub(crate) out_control_chunks: u64,
    pub(crate) in_control_chunks: u64,
}

impl Default for AssocStatsWire {
    fn default() -> Self {
        // Safety: the structure is integers and opaque octets; zero is a
        // valid value of every field.
        unsafe { std::mem::zeroed() }
    }
}

// `struct sctp_sack_info`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct SackInfoWire {
    pub(crate) assoc_id: AssociationId,
    pub(crate) delay: u32,
    pub(crate) frequency: u32,
}

// `struct sctp_prim`, which Linux packs and aligns to 4: an association and an address.
#[repr(C, align(4))]
pub(crate) struct PrimaryAddressWire {
    pub(crate) bytes: [u8; 132],
}

use std::convert::{TryFrom, TryInto};

use os_socketaddr::OsSocketAddr;

use crate::types::{ConnState, ConnStatus, PeerAddress};

impl TryFrom<PeerAddrInternal> for PeerAddress {
    type Error = std::io::Error;

    fn try_from(val: PeerAddrInternal) -> Result<Self, Self::Error> {
        let sa_family = val.address.ss_family;
        // Safety: address is valid and hence getting reference to it is valid.
        let address = unsafe {
            if sa_family as i32 == libc::AF_INET {
                let address = val.address;
                let os_socketaddr = OsSocketAddr::copy_from_raw(
                    &address as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>().try_into().unwrap(),
                );
                os_socketaddr.into_addr().unwrap()
            } else if sa_family as i32 == libc::AF_INET6 {
                let address = val.address;
                let os_socketaddr = OsSocketAddr::copy_from_raw(
                    &address as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>()
                        .try_into()
                        .unwrap(),
                );
                os_socketaddr.into_addr().unwrap()
            } else {
                return Err(std::io::Error::from_raw_os_error(22));
            }
        };
        Ok(Self {
            assoc_id: val.assoc_id,
            address,
            state: crate::PathState::from_i32(val.state),
            cwnd: val.cwnd,
            srtt: val.srtt,
            rto: val.rto,
            mtu: val.mtu,
        })
    }
}

impl TryFrom<ConnStatusInternal> for ConnStatus {
    type Error = std::io::Error;

    fn try_from(val: ConnStatusInternal) -> Result<Self, Self::Error> {
        Ok(Self {
            assoc_id: val.assoc_id,
            state: ConnState::from_i32(val.state),
            rwnd: val.rwnd,
            unacked_data: val.unacked_data,
            pending_data: val.pending_data,
            instreams: val.instreams,
            outstreams: val.outstreams,
            fragmentation_pt: val.fragmentation_pt,
            peer_primary: val.peer_primary.try_into()?,
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::mem::{align_of, offset_of, size_of};

    #[test]
    fn linux_sctp_abi_sizes_and_offsets_match_uapi() {
        assert_eq!(size_of::<SubscribeEvent>(), 8);
        assert_eq!(offset_of!(SubscribeEvent, event), 4);
        assert_eq!(offset_of!(SubscribeEvent, on), 6);
        assert_eq!(size_of::<InitMsg>(), 8);
        assert_eq!(size_of::<PeeloffArg>(), 8);
        assert_eq!(size_of::<PeeloffFlagsArg>(), 12);
        assert_eq!(offset_of!(ConnectxParam, addrs), 8);
        assert_eq!(size_of::<ConnectxParam>(), 8 + size_of::<*mut u8>());
        assert_eq!(size_of::<SndInfoWire>(), 16);
        assert_eq!(offset_of!(SndInfoWire, ppid), 4);
        assert_eq!(size_of::<RcvInfoWire>(), 28);
        assert_eq!(offset_of!(RcvInfoWire, ppid), 8);
        assert_eq!(offset_of!(RcvInfoWire, assoc_id), 24);
        assert_eq!(size_of::<RtoInfoWire>(), 16);
        assert_eq!(size_of::<AssocParamsWire>(), 20);
        assert_eq!(offset_of!(AssocParamsWire, peer_rwnd), 8);
        // __s32 and padding, a sockaddr_storage of 128, then fifteen __u64.
        assert_eq!(size_of::<AssocStatsWire>(), 4 + 4 + 128 + 15 * 8);
        assert_eq!(align_of::<AssocStatsWire>(), 8);
        assert_eq!(offset_of!(AssocStatsWire, max_rto), 136);
        assert_eq!(size_of::<SackInfoWire>(), 12);
        assert_eq!(size_of::<PrimaryAddressWire>(), 132);
        assert_eq!(size_of::<PeerAddrInternal>(), 152);
        assert_eq!(offset_of!(PeerAddrInternal, state), 132);
        assert_eq!(size_of::<ConnStatusInternal>(), 176);
        assert_eq!(offset_of!(ConnStatusInternal, peer_primary), 24);
        assert_eq!(size_of::<PeerAddressParamsInternal>(), 156);
        assert_eq!(align_of::<PeerAddressParamsInternal>(), 4);
    }

    /// The options of a send go in their wire form with the payload protocol identifier in
    /// network byte order, and unordered as the flag `SCTP_UNORDERED`, 1.
    #[test]
    fn the_options_of_a_send_go_in_their_wire_form() {
        let options = crate::SendOptions::new()
            .stream(2)
            .ppid(60)
            .unordered(true)
            .context(7);
        let wire = SndInfoWire::new(options, 9);
        assert_eq!(
            (wire.sid, wire.flags, wire.ppid, wire.context, wire.assoc_id),
            (2, 1, 60_u32.to_be(), 7, 9)
        );
        assert_eq!(SndInfoWire::new(crate::SendOptions::new(), 0).flags, 0);
        let abort = SndInfoWire::control(crate::consts::SCTP_ABORT, 9);
        assert_eq!((abort.sid, abort.flags, abort.assoc_id), (0, 4, 9));
    }
}
