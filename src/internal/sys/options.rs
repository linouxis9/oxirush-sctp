//! Linux socket options, with native ABI types confined to this boundary.
use super::SOL_SCTP;
use crate::consts::*;
use crate::types::internal::{
    AssocParamsWire, ConnStatusInternal, InitMsg, PeerAddrInternal, PeerAddressParamsInternal,
    PrimaryAddressWire, RtoInfoWire, SackInfoWire, SndInfoWire, SubscribeEvent,
};
use crate::{
    AssociationId, AssociationParams, ConnStatus, Event, EventSubscriptionError, InitParams,
    PeerAddress, PeerAddressParams, RtoInfo, SackInfo, SendInfo, SubscribeEventAssocId,
};
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd};

pub(crate) fn socket_error(fd: BorrowedFd<'_>) -> std::io::Result<libc::c_int> {
    // Safety: every bit pattern is a valid c_int.
    unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_ERROR, 0) }
}

pub(crate) fn sctp_set_default_sendinfo_internal(
    fd: BorrowedFd<'_>,
    sendinfo: SendInfo,
) -> std::io::Result<()> {
    setsockopt_internal(
        fd,
        SOL_SCTP,
        SCTP_DEFAULT_SNDINFO,
        &SndInfoWire::from(&sendinfo),
    )
}

pub(crate) fn sctp_subscribe_event_internal(
    fd: BorrowedFd<'_>,
    event: Event,
    assoc_id: SubscribeEventAssocId,
    on: bool,
) -> std::io::Result<()> {
    let subscriber = SubscribeEvent {
        event,
        assoc_id: assoc_id.into(),
        on,
    };

    unsafe {
        let result = libc::setsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            SCTP_EVENT,
            &subscriber as *const _ as *const libc::c_void,
            std::mem::size_of::<SubscribeEvent>().try_into().unwrap(),
        );
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

// Whether the socket, or the association of a one-to-one socket, has `event` subscribed.
pub(crate) fn sctp_event_subscribed_internal(
    fd: BorrowedFd<'_>,
    event: Event,
) -> std::io::Result<bool> {
    // `struct sctp_event` as integers alone, for the kernel to write over.
    #[repr(C)]
    struct Query {
        assoc_id: AssociationId,
        event: u16,
        on: u8,
    }
    let query = Query {
        assoc_id: SubscribeEventAssocId::Future.into(),
        event: event as u16,
        on: 0,
    };
    // Safety: `getsockopt` writes a `struct sctp_event` over `query`, which has its layout
    // and takes any octets.
    let answer = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_EVENT, query)? };
    Ok(answer.on != 0)
}

pub(crate) fn sctp_subscribe_events_internal(
    fd: BorrowedFd<'_>,
    events: &[Event],
    assoc_id: SubscribeEventAssocId,
    on: bool,
) -> std::io::Result<()> {
    let mut failures = Vec::new();
    for event in events {
        if let Err(error) = sctp_subscribe_event_internal(fd, *event, assoc_id, on) {
            failures.push((*event, error));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::other(EventSubscriptionError { failures }))
    }
}

fn path_params(assoc_id: AssociationId, address: SocketAddr) -> PeerAddressParamsInternal {
    let mut value = PeerAddressParamsInternal { bytes: [0; 156] };
    value.bytes[..4].copy_from_slice(&assoc_id.to_ne_bytes());
    let address = OsSocketAddr::from(address);
    let address = address.as_ref();
    value.bytes[4..4 + address.len()].copy_from_slice(address);
    value
}

pub(crate) fn peer_address_params_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
    address: SocketAddr,
) -> std::io::Result<PeerAddressParams> {
    // Safety: every byte pattern is valid for the byte buffer.
    let value = unsafe {
        getsockopt_internal(
            fd,
            SOL_SCTP,
            SCTP_PEER_ADDR_PARAMS,
            path_params(assoc_id, address),
        )?
    };
    let bytes = value.bytes;
    Ok(PeerAddressParams {
        assoc_id,
        address,
        heartbeat_enabled: u32::from_ne_bytes(bytes[146..150].try_into().unwrap()) & 1 != 0,
        heartbeat_interval: std::time::Duration::from_millis(u32::from_ne_bytes(
            bytes[132..136].try_into().unwrap(),
        ) as u64),
        path_max_retrans: u16::from_ne_bytes(bytes[136..138].try_into().unwrap()),
    })
}

pub(crate) fn set_peer_address_params_internal(
    fd: BorrowedFd<'_>,
    params: PeerAddressParams,
) -> std::io::Result<()> {
    let millis = params.heartbeat_interval.as_millis();
    if !params
        .heartbeat_interval
        .subsec_nanos()
        .is_multiple_of(1_000_000)
        || millis > u32::MAX as u128
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "heartbeat interval must fit whole u32 milliseconds",
        ));
    }
    let mut value = path_params(params.assoc_id, params.address);
    value.bytes[132..136].copy_from_slice(&(millis as u32).to_ne_bytes());
    value.bytes[136..138].copy_from_slice(&params.path_max_retrans.to_ne_bytes());
    let flags: u32 =
        if params.heartbeat_enabled { 1 } else { 2 } | if millis == 0 { 1 << 7 } else { 0 };
    value.bytes[146..150].copy_from_slice(&flags.to_ne_bytes());
    setsockopt_internal(fd, SOL_SCTP, SCTP_PEER_ADDR_PARAMS, &value)
}

pub(crate) fn request_heartbeat_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
    address: SocketAddr,
) -> std::io::Result<()> {
    let mut value = path_params(assoc_id, address);
    value.bytes[146..150].copy_from_slice(&(1_u32 << 2).to_ne_bytes());
    setsockopt_internal(fd, SOL_SCTP, SCTP_PEER_ADDR_PARAMS, &value)
}

pub(crate) fn sctp_init_params_internal(fd: BorrowedFd<'_>) -> std::io::Result<InitParams> {
    let empty = InitMsg {
        ostreams: 0,
        istreams: 0,
        retries: 0,
        timeout: 0,
    };
    // Safety: `InitMsg` holds integers only, so any bytes are a valid value.
    let value = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_INITMSG, empty)? };
    Ok(InitParams {
        outbound_streams: value.ostreams,
        inbound_streams: value.istreams,
        max_attempts: value.retries,
        max_init_timeout: value.timeout,
    })
}

pub(crate) fn sctp_setup_init_params_internal(
    fd: BorrowedFd<'_>,
    params: InitParams,
) -> std::io::Result<()> {
    tracing::debug!("Setting up `init_params` using `setsockopt`");
    let init_params = InitMsg {
        ostreams: params.outbound_streams,
        istreams: params.inbound_streams,
        retries: params.max_attempts,
        timeout: params.max_init_timeout,
    };

    unsafe {
        let result = libc::setsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            SCTP_INITMSG,
            &init_params as *const _ as *const libc::c_void,
            std::mem::size_of::<InitMsg>().try_into().unwrap(),
        );
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// `SCTP_FRAGMENT_INTERLEAVE`: level 1 lets the parts of the messages of several
/// associations come in turns, level 0 not.
pub(crate) fn set_fragment_interleave_internal(
    fd: BorrowedFd<'_>,
    interleave: bool,
) -> std::io::Result<()> {
    setsockopt_internal(
        fd,
        SOL_SCTP,
        crate::consts::SCTP_FRAGMENT_INTERLEAVE,
        &libc::c_int::from(interleave),
    )
}

pub(crate) fn request_rcvinfo_internal(fd: BorrowedFd<'_>, on: bool) -> std::io::Result<()> {
    tracing::debug!("Requesting `rcv_info` along with received data on the socket.");

    let enable: libc::socklen_t = u32::from(on);
    let enable_size = std::mem::size_of::<libc::socklen_t>();

    unsafe {
        let result = libc::setsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            SCTP_RECVRCVINFO,
            &enable as *const _ as *const libc::c_void,
            enable_size.try_into().unwrap(),
        );

        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

pub(crate) fn request_nxtinfo_internal(fd: BorrowedFd<'_>, on: bool) -> std::io::Result<()> {
    tracing::debug!("Requesting `nxt_info` along with received data on the socket.");

    let enable: libc::socklen_t = u32::from(on);
    let enable_size = std::mem::size_of::<libc::socklen_t>();

    unsafe {
        let result = libc::setsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            SCTP_RECVNXTINFO,
            &enable as *const _ as *const libc::c_void,
            enable_size.try_into().unwrap(),
        );

        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

pub(crate) fn sctp_set_nodelay_internal(fd: BorrowedFd<'_>, nodelay: bool) -> std::io::Result<()> {
    tracing::debug!("Setting `SCTP_NODELAY` to {nodelay}.");
    setsockopt_internal(fd, SOL_SCTP, SCTP_NODELAY, &libc::c_int::from(nodelay))
}

pub(crate) fn sctp_nodelay_internal(fd: BorrowedFd<'_>) -> std::io::Result<bool> {
    // Safety: every value is a valid `c_int`.
    let nodelay: libc::c_int = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_NODELAY, 0)? };
    Ok(nodelay != 0)
}

pub(crate) fn set_linger_internal(
    fd: BorrowedFd<'_>,
    linger: Option<std::time::Duration>,
) -> std::io::Result<()> {
    tracing::debug!("Setting `SO_LINGER` to {linger:?}.");
    if linger.is_some_and(|duration| !duration.is_zero()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "positive linger can block the async runtime when the socket is dropped",
        ));
    }
    let value = libc::linger {
        l_onoff: libc::c_int::from(linger.is_some()),
        l_linger: 0,
    };
    setsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_LINGER, &value)
}

pub(crate) fn sctp_set_rto_info_internal(
    fd: BorrowedFd<'_>,
    rto_info: RtoInfo,
) -> std::io::Result<()> {
    tracing::debug!("Setting `SCTP_RTOINFO` to {rto_info:?}.");
    let wire = RtoInfoWire {
        assoc_id: rto_info.assoc_id,
        initial: rto_info.initial,
        max: rto_info.max,
        min: rto_info.min,
    };
    setsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, &wire)
}

pub(crate) fn sctp_get_rto_info_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<RtoInfo> {
    let wire = RtoInfoWire {
        assoc_id,
        ..Default::default()
    };
    // Safety: the structure holds integers only, so any bytes are a valid value.
    let wire = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, wire)? };
    Ok(RtoInfo {
        assoc_id: wire.assoc_id,
        initial: wire.initial,
        max: wire.max,
        min: wire.min,
    })
}

pub(crate) fn set_association_params_internal(
    fd: BorrowedFd<'_>,
    params: &AssociationParams,
) -> std::io::Result<()> {
    // The numbers that Linux only reports are zero: it leaves them as they are
    let wire = AssocParamsWire {
        assoc_id: params.assoc_id,
        max_retransmissions: params.max_retransmissions,
        cookie_life: params.cookie_life,
        ..Default::default()
    };
    setsockopt_internal(fd, SOL_SCTP, SCTP_ASSOCINFO, &wire)
}

pub(crate) fn association_params_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<AssociationParams> {
    let wire = AssocParamsWire {
        assoc_id,
        ..Default::default()
    };
    // Safety: the structure holds integers only, so any bytes are a valid value.
    let wire = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_ASSOCINFO, wire)? };
    Ok(AssociationParams {
        assoc_id: wire.assoc_id,
        max_retransmissions: wire.max_retransmissions,
        peer_addresses: wire.peer_addresses,
        peer_rwnd: wire.peer_rwnd,
        local_rwnd: wire.local_rwnd,
        cookie_life: wire.cookie_life,
    })
}

pub(crate) fn set_sack_info_internal(fd: BorrowedFd<'_>, info: &SackInfo) -> std::io::Result<()> {
    let wire = SackInfoWire {
        assoc_id: info.assoc_id,
        delay: info.delay,
        frequency: info.frequency,
    };
    setsockopt_internal(fd, SOL_SCTP, SCTP_DELAYED_SACK, &wire)
}

pub(crate) fn sack_info_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<SackInfo> {
    let wire = SackInfoWire {
        assoc_id,
        ..Default::default()
    };
    // Safety: the structure holds integers only, so any bytes are a valid value.
    let wire = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_DELAYED_SACK, wire)? };
    Ok(SackInfo {
        assoc_id: wire.assoc_id,
        delay: wire.delay,
        frequency: wire.frequency,
    })
}

// `struct sctp_prim` for an association and one of the addresses of its peer.
fn primary_address(assoc_id: AssociationId, address: SocketAddr) -> PrimaryAddressWire {
    let mut wire = PrimaryAddressWire { bytes: [0; 132] };
    wire.bytes[..4].copy_from_slice(&assoc_id.to_ne_bytes());
    let address = OsSocketAddr::from(address);
    let address = address.as_ref();
    wire.bytes[4..4 + address.len()].copy_from_slice(address);
    wire
}

pub(crate) fn set_primary_address_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
    address: SocketAddr,
) -> std::io::Result<()> {
    setsockopt_internal(
        fd,
        SOL_SCTP,
        SCTP_PRIMARY_ADDR,
        &primary_address(assoc_id, address),
    )
}

// The DSCP of what the socket sends, in the six high bits of the traffic class of IPv4 and
// of IPv6. A socket of IPv6 can carry IPv4 as well, and is given both.
pub(crate) fn set_dscp_internal(fd: BorrowedFd<'_>, dscp: u8) -> std::io::Result<()> {
    if dscp > 63 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("a DSCP is 0 to 63, and this one is {dscp}"),
        ));
    }
    let class = libc::c_int::from(dscp) << 2;
    if socket_domain(fd)? == libc::AF_INET6 {
        setsockopt_internal(fd, libc::IPPROTO_IPV6, libc::IPV6_TCLASS, &class)?;
    }
    setsockopt_internal(fd, libc::IPPROTO_IP, libc::IP_TOS, &class)
}

pub(crate) fn dscp_internal(fd: BorrowedFd<'_>) -> std::io::Result<u8> {
    let (level, name) = if socket_domain(fd)? == libc::AF_INET6 {
        (libc::IPPROTO_IPV6, libc::IPV6_TCLASS)
    } else {
        (libc::IPPROTO_IP, libc::IP_TOS)
    };
    // Safety: every value is a valid `c_int`.
    let class: libc::c_int = unsafe { getsockopt_internal(fd, level, name, 0)? };
    Ok((class >> 2) as u8 & 0x3f)
}

pub(crate) fn socket_domain(fd: BorrowedFd<'_>) -> std::io::Result<libc::c_int> {
    // Safety: every value is a valid `c_int`.
    unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_DOMAIN, 0) }
}

// The size of a buffer of the socket, in octets. Linux doubles the number that it is given,
// for its own bookkeeping, and reports the double.
pub(crate) fn set_buffer_size_internal(
    fd: BorrowedFd<'_>,
    name: libc::c_int,
    octets: usize,
) -> std::io::Result<()> {
    let octets = libc::c_int::try_from(octets).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("a buffer of {octets} octets is more than the system takes"),
        )
    })?;
    setsockopt_internal(fd, libc::SOL_SOCKET, name, &octets)
}

pub(crate) fn buffer_size_internal(
    fd: BorrowedFd<'_>,
    name: libc::c_int,
) -> std::io::Result<usize> {
    // Safety: every value is a valid `c_int`.
    let octets: libc::c_int = unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, name, 0)? };
    Ok(octets.max(0) as usize)
}

pub(crate) fn set_reuseaddr_internal(fd: BorrowedFd<'_>, reuseaddr: bool) -> std::io::Result<()> {
    tracing::debug!("Setting `SO_REUSEADDR` to {reuseaddr}.");
    setsockopt_internal(
        fd,
        libc::SOL_SOCKET,
        libc::SO_REUSEADDR,
        &libc::c_int::from(reuseaddr),
    )
}

pub(crate) fn reuseaddr_internal(fd: BorrowedFd<'_>) -> std::io::Result<bool> {
    // Safety: every value is a valid `c_int`.
    let reuseaddr: libc::c_int =
        unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 0)? };
    Ok(reuseaddr != 0)
}

fn setsockopt_internal<T>(
    fd: BorrowedFd<'_>,
    level: libc::c_int,
    name: libc::c_int,
    value: &T,
) -> std::io::Result<()> {
    // Safety: `value` is valid for reads of its size during the call and is only read.
    let result = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            level,
            name,
            value as *const T as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if result < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) unsafe fn getsockopt_internal<T>(
    fd: BorrowedFd<'_>,
    level: libc::c_int,
    name: libc::c_int,
    mut value: T,
) -> std::io::Result<T> {
    let size = std::mem::size_of::<T>();
    let mut len = size as libc::socklen_t;
    // Safety: the kernel writes at most `len` octets, the size of `value`. That any octets
    // it writes are a valid `T` is what the caller of this function answers for.
    let result = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            level,
            name,
            &mut value as *mut T as *mut libc::c_void,
            &mut len,
        )
    };
    if result < 0 {
        Err(std::io::Error::last_os_error())
    } else if len as usize != size {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("`getsockopt` returned {len} bytes instead of {size}"),
        ))
    } else {
        Ok(value)
    }
}

pub(crate) fn peer_address_info_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
    address: SocketAddr,
) -> std::io::Result<PeerAddress> {
    let address = OsSocketAddr::from(address);
    let address = address.as_ref();
    // Safety: `PeerAddrInternal` holds integers only, so any bytes are a valid value, and the
    // address fits in its storage.
    let value = unsafe {
        let mut storage: libc::sockaddr_storage = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            address.as_ptr(),
            std::ptr::addr_of_mut!(storage).cast::<u8>(),
            address.len(),
        );
        let mut value = std::mem::MaybeUninit::<PeerAddrInternal>::zeroed().assume_init();
        value.assoc_id = assoc_id;
        value.address = storage;
        getsockopt_internal(fd, SOL_SCTP, SCTP_GET_PEER_ADDR_INFO, value)?
    };
    value.try_into()
}

pub(crate) fn sctp_get_status_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<ConnStatus> {
    tracing::debug!("Calling `sctp_get_status_internal`.");

    // Safety: `ConnStatusInternal` holds integers only, so any bytes are a valid value.
    let sctp_status = unsafe {
        let mut sctp_status = std::mem::MaybeUninit::<ConnStatusInternal>::zeroed().assume_init();
        sctp_status.assoc_id = assoc_id;
        getsockopt_internal(fd, SOL_SCTP, SCTP_STATUS, sctp_status)?
    };
    sctp_status.try_into()
}
