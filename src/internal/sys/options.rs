//! Linux socket options, with native ABI types confined to this boundary.
use super::SOL_SCTP;
use crate::consts::*;
use crate::types::internal::{
    ConnStatusInternal, InitMsg, PeerAddressParamsInternal, SubscribeEvent,
};
use crate::{
    AssociationId, ConnStatus, Event, EventSubscriptionError, PeerAddressParams, RtoInfo, SendInfo,
    SubscribeEventAssocId,
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
    unsafe {
        let result = libc::setsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            SCTP_DEFAULT_SNDINFO,
            &sendinfo as *const _ as *const libc::c_void,
            std::mem::size_of::<SendInfo>().try_into().unwrap(),
        );
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
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

pub(crate) fn sctp_subscribe_events_internal(
    fd: BorrowedFd<'_>,
    events: &[Event],
    assoc_id: SubscribeEventAssocId,
    on: bool,
) -> std::io::Result<()> {
    let mut failures = Vec::new();
    for event in events {
        if let Err(error) = sctp_subscribe_event_internal(fd, event.clone(), assoc_id, on) {
            failures.push((event.clone(), error));
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
    if params.heartbeat_interval.subsec_nanos() % 1_000_000 != 0 || millis > u32::MAX as u128 {
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

pub(crate) fn sctp_setup_init_params_internal(
    fd: BorrowedFd<'_>,
    ostreams: u16,
    istreams: u16,
    retries: u16,
    timeout: u16,
) -> std::io::Result<()> {
    log::debug!("Setting up `init_params` using `setsockopt`");
    let init_params = InitMsg {
        ostreams,
        istreams,
        retries,
        timeout,
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

pub(crate) fn request_rcvinfo_internal(fd: BorrowedFd<'_>, on: bool) -> std::io::Result<()> {
    log::debug!("Requesting `rcv_info` along with received data on the socket.");

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
    log::debug!("Requesting `nxt_info` along with received data on the socket.");

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
    log::debug!("Setting `SCTP_NODELAY` to {}.", nodelay);
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
    log::debug!("Setting `SO_LINGER` to {:?}.", linger);
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
    log::debug!("Setting `SCTP_RTOINFO` to {:?}.", rto_info);
    setsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, &rto_info)
}

pub(crate) fn sctp_get_rto_info_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<RtoInfo> {
    let rto_info = RtoInfo {
        assoc_id,
        ..Default::default()
    };
    // Safety: `RtoInfo` holds integers only, so any bytes are a valid value.
    unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, rto_info) }
}

pub(crate) fn set_reuseaddr_internal(fd: BorrowedFd<'_>, reuseaddr: bool) -> std::io::Result<()> {
    log::debug!("Setting `SO_REUSEADDR` to {}.", reuseaddr);
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
    let result = libc::getsockopt(
        fd.as_raw_fd(),
        level,
        name,
        &mut value as *mut T as *mut libc::c_void,
        &mut len,
    );
    if result < 0 {
        Err(std::io::Error::last_os_error())
    } else if len as usize != size {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("`getsockopt` returned {} bytes instead of {}", len, size),
        ))
    } else {
        Ok(value)
    }
}

pub(crate) fn sctp_get_status_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<ConnStatus> {
    log::debug!("Calling `sctp_get_status_internal`.");

    // Safety: `ConnStatusInternal` holds integers only, so any bytes are a valid value.
    let sctp_status = unsafe {
        let mut sctp_status = std::mem::MaybeUninit::<ConnStatusInternal>::zeroed().assume_init();
        sctp_status.assoc_id = assoc_id;
        getsockopt_internal(fd, SOL_SCTP, SCTP_STATUS, sctp_status)?
    };
    sctp_status.try_into()
}
