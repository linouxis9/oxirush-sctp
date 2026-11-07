//! Linux socket options, with native ABI types confined to this boundary.
use super::SOL_SCTP;
use crate::consts::*;
use crate::types::internal::{ConnStatusInternal, SubscribeEvent};
use crate::{AssociationId, ConnStatus, Event};
use std::os::fd::{AsRawFd, BorrowedFd};

pub(crate) fn socket_error(fd: BorrowedFd<'_>) -> std::io::Result<libc::c_int> {
    // Safety: every bit pattern is a valid c_int.
    unsafe { getsockopt(fd, libc::SOL_SOCKET, libc::SO_ERROR, 0) }
}

// The associations that an event subscription is for: those that a socket has and those to
// come, `SCTP_ALL_ASSOC`. A one-to-one socket has its association alone.
const SCTP_ALL_ASSOC: AssociationId = 2;

pub(crate) fn sctp_subscribe_event_internal(
    fd: BorrowedFd<'_>,
    event: Event,
    on: bool,
) -> std::io::Result<()> {
    let subscriber = SubscribeEvent {
        event,
        assoc_id: SCTP_ALL_ASSOC,
        on,
    };
    setsockopt(fd, SOL_SCTP, SCTP_EVENT, &subscriber)
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
        // `SCTP_FUTURE_ASSOC`: the socket's own, which a one-to-one socket reads from its
        // association
        assoc_id: 0,
        event: event as u16,
        on: 0,
    };
    // Safety: `getsockopt` writes a `struct sctp_event` over `query`, which has its layout
    // and takes any octets.
    let answer = unsafe { getsockopt(fd, SOL_SCTP, SCTP_EVENT, query)? };
    Ok(answer.on != 0)
}

// Each of `events` subscribed or not, up to the first that the kernel refuses: the error,
// which names it.
pub(crate) fn sctp_subscribe_events_internal(
    fd: BorrowedFd<'_>,
    events: &[Event],
    on: bool,
) -> std::io::Result<()> {
    for event in events {
        sctp_subscribe_event_internal(fd, *event, on)
            .map_err(|error| std::io::Error::new(error.kind(), format!("{event:?}: {error}")))?;
    }
    Ok(())
}

/// `SCTP_FRAGMENT_INTERLEAVE` level 1: the parts of the messages of several associations
/// come in turns.
pub(crate) fn set_fragment_interleave_internal(fd: BorrowedFd<'_>) -> std::io::Result<()> {
    setsockopt(
        fd,
        SOL_SCTP,
        crate::consts::SCTP_FRAGMENT_INTERLEAVE,
        &libc::c_int::from(true),
    )
}

/// `SCTP_RECVRCVINFO`: the receive information of each message.
pub(crate) fn request_rcvinfo_internal(fd: BorrowedFd<'_>) -> std::io::Result<()> {
    setsockopt(fd, SOL_SCTP, SCTP_RECVRCVINFO, &libc::c_int::from(true))
}

pub(crate) fn socket_domain(fd: BorrowedFd<'_>) -> std::io::Result<libc::c_int> {
    // Safety: every value is a valid `c_int`.
    unsafe { getsockopt(fd, libc::SOL_SOCKET, libc::SO_DOMAIN, 0) }
}

pub(crate) fn setsockopt<T>(
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

pub(crate) unsafe fn getsockopt<T>(
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

pub(crate) fn sctp_get_status_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<ConnStatus> {
    // Safety: `ConnStatusInternal` holds integers only, so any bytes are a valid value.
    let sctp_status = unsafe {
        let mut sctp_status = std::mem::MaybeUninit::<ConnStatusInternal>::zeroed().assume_init();
        sctp_status.assoc_id = assoc_id;
        getsockopt(fd, SOL_SCTP, SCTP_STATUS, sctp_status)?
    };
    sctp_status.try_into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    /// The first event that the kernel refuses is the error, which names it: here on a socket
    /// of UDP, which has no option of SCTP.
    #[test]
    fn a_refused_subscription_names_its_event() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let events = [Event::Shutdown, Event::Association];
        let error = sctp_subscribe_events_internal(socket.as_fd(), &events, true).unwrap_err();
        let refused = std::io::Error::from_raw_os_error(libc::ENOPROTOOPT);
        assert_eq!(error.kind(), refused.kind());
        assert_eq!(error.to_string(), format!("Shutdown: {refused}"));
    }
}
