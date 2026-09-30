//! Actual implementation of the API Calls
//!
//! Nothing in this module should be public API as this module contains `unsafe` code that uses
//! `libc` and internal `libc` structs and function calls.

use tokio::io::{unix::AsyncFd, Interest};

use std::convert::TryInto;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::{Mutex, PoisonError};

use os_socketaddr::OsSocketAddr;

use crate::types::internal::{ConnStatusInternal, ConnectxParam, InitMsg, SubscribeEvent};
use crate::types::ConnState;
use crate::{
    AssocChangeState, AssociationChange, AssociationId, BindxFlags, CmsgType, ConnStatus,
    ConnectedSocket, Event, Listener, Notification, NotificationOrData, NxtInfo, RcvInfo,
    ReceivedData, RtoInfo, SendData, SendInfo, Shutdown, SubscribeEventAssocId,
};

#[allow(unused)]
use super::consts::*;

static SOL_SCTP: libc::c_int = 132;

// Implementation of `sctp_bindx` using `libc::setsockopt`
pub(crate) fn sctp_bindx_internal(
    fd: &AsyncFd<OwnedFd>,
    addrs: &[SocketAddr],
    flags: BindxFlags,
) -> std::io::Result<()> {
    log::debug!("Binding following addresses to socket: {:#?}", addrs);

    let mut addrs_u8: Vec<u8> = vec![];

    for addr in addrs {
        let ossockaddr: OsSocketAddr = (*addr).into();
        let slice = ossockaddr.as_ref();
        addrs_u8.extend(slice);
    }

    let addrs_len = addrs_u8.len();

    let flags = match flags {
        BindxFlags::Add => SCTP_SOCKOPT_BINDX_ADD,
        BindxFlags::Remove => SCTP_SOCKOPT_BINDX_REM,
    };

    log::trace!(
        "addrs_len: {}, addrs_u8: {:?}, flags: {}",
        addrs_len,
        addrs_u8,
        flags
    );

    // Safety: The passed vector is valid during the function call and hence the passed reference
    // to raw data is valid.
    unsafe {
        let result = libc::setsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            flags,
            addrs_u8.as_ptr() as *const _ as *const libc::c_void,
            addrs_len as libc::socklen_t,
        );

        if result < 0 {
            let error = std::io::Error::last_os_error();
            log::error!("Error: {} during `sctp_bindx` using `setsockopt`.", error);
            Err(error)
        } else {
            Ok(())
        }
    }
}

// Implementation of `sctp_peeloff` using `libc::getsockopt`
pub(crate) fn sctp_peeloff_internal(
    fd: &AsyncFd<OwnedFd>,
    assoc_id: AssociationId,
) -> std::io::Result<ConnectedSocket> {
    log::debug!("Peeling off socket for Association ID: {:?}", assoc_id);

    use crate::types::internal::{PeeloffArg, PeeloffFlagsArg};

    // The peeled off socket is non-blocking, and closed on `exec`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let flags = (libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC) as libc::c_uint;

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let flags = 0;

    let mut peeloff_arg = PeeloffFlagsArg {
        p_arg: PeeloffArg::from_assoc_id(assoc_id),
        flags,
    };
    let mut peeloff_size = std::mem::size_of::<PeeloffFlagsArg>() as libc::socklen_t;

    // Safety: Pointer to `peeloff_arg` and `peeloff_size` is valid as the variable is still in the
    // scope
    unsafe {
        let peeloff_arg_ptr = std::ptr::addr_of_mut!(peeloff_arg);
        let peeloff_size_ptr = std::ptr::addr_of_mut!(peeloff_size);
        let result = libc::getsockopt(
            fd.as_raw_fd(),
            SOL_SCTP,
            SCTP_SOCKOPT_PEELOFF_FLAGS,
            peeloff_arg_ptr as *mut _ as *mut libc::c_void,
            peeloff_size_ptr as *mut _ as *mut libc::socklen_t,
        );
        if result < 0 {
            let error = std::io::Error::last_os_error();
            log::error!("Error: {} during `sctp_peeloff` using `getsockopt`.", error);
            Err(error)
        } else {
            // Safety: the kernel returned a new descriptor, which nothing else owns.
            let fd = OwnedFd::from_raw_fd(peeloff_arg.p_arg.sd);

            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            set_fd_non_blocking_cloexec(fd.as_raw_fd())?;

            ConnectedSocket::from_owned_fd(fd)
        }
    }
}

// Implementation of `socket` using `libc::socket`.
//
// Based on the type of the requested socket, we pass different `type` parameter to actual
// `libc::socket` call. See section 3.1.1 and section 4.1.1 of RFC 6458.
pub(crate) fn sctp_socket_internal(
    domain: libc::c_int,
    assoc: crate::SocketToAssociation,
) -> std::io::Result<OwnedFd> {
    let socket_type = match assoc {
        crate::SocketToAssociation::OneToOne => {
            log::debug!("Creating TCP Style Socket.");
            libc::SOCK_STREAM
        }
        crate::SocketToAssociation::OneToMany => {
            log::debug!("Creating UDP Style Socket.");
            libc::SOCK_SEQPACKET
        }
    };
    // The socket is non-blocking, and closed on `exec` so that child processes do not keep its
    // associations open.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let socket_type = socket_type | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;

    unsafe {
        let rawfd = libc::socket(domain, socket_type, libc::IPPROTO_SCTP);
        if rawfd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Safety: `socket` returned a new descriptor, which nothing else owns.
        let fd = OwnedFd::from_raw_fd(rawfd);

        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        set_fd_non_blocking_cloexec(fd.as_raw_fd())?;

        Ok(fd)
    }
}

// Implementation of `listen` using `libc::listen`
pub(crate) fn sctp_listen_internal(
    fd: AsyncFd<OwnedFd>,
    backlog: i32,
) -> std::io::Result<Listener> {
    unsafe {
        let rawfd = fd.as_raw_fd();
        let result = libc::listen(rawfd, backlog);

        if result < 0 {
            let error = std::io::Error::last_os_error();
            log::error!("Error: {} during `sctp_listen`.", error);
            Err(error)
        } else {
            Ok(Listener::from_async_fd(fd))
        }
    }
}

// Implmentation of `sctp_getpaddrs` using `libc::getsockopt`
pub(crate) fn sctp_getpaddrs_internal(
    fd: &AsyncFd<OwnedFd>,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    sctp_getaddrs_internal(fd.as_raw_fd(), SCTP_GET_PEER_ADDRS, assoc_id)
}

// Implmentation of `sctp_getladdrs` using `libc::getsockopt`
pub(crate) fn sctp_getladdrs_internal(
    fd: &AsyncFd<OwnedFd>,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    sctp_getaddrs_internal(fd.as_raw_fd(), SCTP_GET_LOCAL_ADDRS, assoc_id)
}

// Actual function performing `sctp_getpaddrs` or `sctp_getladdrs`
fn sctp_getaddrs_internal(
    fd: RawFd,
    flags: libc::c_int,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    let addr_type = if flags == SCTP_GET_LOCAL_ADDRS {
        "local"
    } else {
        "peer"
    };
    log::debug!(
        "Getting {} Addresses for the SCTP Association {:?}",
        addr_type,
        assoc_id
    );

    // `struct sctp_getaddrs` is the association ID, the number of addresses and the packed
    // addresses. The kernel fails with `ENOMEM` if they do not fit: the buffer then grows.
    const HEADER_SIZE: usize = 8;
    const MAX_BUFFER_SIZE: usize = 1 << 20;
    let mut capacity = 4096_usize;
    loop {
        let mut addrs_buff: Vec<u8> = vec![0; capacity];
        addrs_buff[0..4].copy_from_slice(&assoc_id.to_ne_bytes());
        let mut getaddrs_size = capacity as libc::socklen_t;

        // Safety: `addrs_buff` is valid for writes of `getaddrs_size` octets during the call.
        let result = unsafe {
            libc::getsockopt(
                fd,
                SOL_SCTP,
                flags,
                addrs_buff.as_mut_ptr() as *mut libc::c_void,
                &mut getaddrs_size,
            )
        };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ENOMEM) && capacity < MAX_BUFFER_SIZE {
                capacity *= 2;
                continue;
            }
            log::error!(
                "Error: {} while getting {} addresses using  `getsockopt`.",
                error,
                addr_type
            );
            return Err(error);
        }

        // For the local addresses, the kernel returns the length of the addresses only.
        let returned_size = if flags == SCTP_GET_LOCAL_ADDRS {
            HEADER_SIZE + getaddrs_size as usize
        } else {
            getaddrs_size as usize
        };
        let returned = &addrs_buff[..returned_size.min(capacity)];
        if returned.len() < HEADER_SIZE {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let addr_count = u32::from_ne_bytes(returned[4..8].try_into().unwrap());
        log::trace!("Got {} addresses", addr_count);

        // Now for each of the 'addresses', we get the family and then interpret each of the
        // addresses accordingly, checking that it is within what the kernel returned.
        let mut peeraddrs = vec![];
        let mut offset = HEADER_SIZE;
        for _ in 0..addr_count {
            let rest = &returned[offset..];
            if rest.len() < std::mem::size_of::<libc::sockaddr>() {
                return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
            }
            // Safety: `rest` holds at least a `sockaddr`, read without alignment requirement.
            let sa_family =
                unsafe { std::ptr::read_unaligned(rest.as_ptr() as *const libc::sockaddr) }
                    .sa_family;
            let len = match sa_family as i32 {
                libc::AF_INET => std::mem::size_of::<libc::sockaddr_in>(),
                libc::AF_INET6 => std::mem::size_of::<libc::sockaddr_in6>(),
                // Unsupported Family - should never come here.
                _ => return Err(std::io::Error::from_raw_os_error(libc::EINVAL)),
            };
            if rest.len() < len {
                return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
            }
            // Safety: `rest` holds `len` octets, which `copy_from_raw` copies.
            let os_socketaddr = unsafe {
                OsSocketAddr::copy_from_raw(
                    rest.as_ptr() as *const libc::sockaddr,
                    len as libc::socklen_t,
                )
            };
            let socketaddr = os_socketaddr
                .into_addr()
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
            log::trace!("Got Address: {:#?}", socketaddr);
            peeraddrs.push(socketaddr);
            offset += len;
        }
        return Ok(peeraddrs);
    }
}

// Implementation of `sctp_connectx` using `getsockopt` and new API using `SCTP_SOCKOPT_CONNECTX3`.
pub(crate) async fn sctp_connectx_internal(
    fd: AsyncFd<OwnedFd>,
    addrs: &[SocketAddr],
) -> std::io::Result<(ConnectedSocket, AssociationId)> {
    let mut addrs_u8: Vec<u8> = vec![];

    log::debug!("Connecting to {:?} using `getsockopt`", addrs);

    for addr in addrs {
        let ossockaddr: OsSocketAddr = (*addr).into();
        let slice = ossockaddr.as_ref();
        addrs_u8.extend(slice);
    }

    let addrs_len = addrs_u8.len();

    let raw_fd = fd.as_raw_fd();
    // Safety: The passed vector is valid during the function call and hence the passed reference
    // to raw data is valid. `params` holds a raw pointer, which must not live across the `await`
    // below, or the future would not be `Send`.
    let assoc_id = unsafe {
        let mut params = ConnectxParam {
            assoc_id: 0,
            addrs_size: addrs_len.try_into().unwrap(),
            addrs: addrs_u8.as_mut_ptr(),
        };

        let mut params_size = std::mem::size_of::<ConnectxParam>() as libc::socklen_t;

        let result = libc::getsockopt(
            raw_fd,
            SOL_SCTP,
            SCTP_SOCKOPT_CONNECTX3,
            &mut params as *mut _ as *mut libc::c_void,
            &mut params_size as *mut _ as *mut libc::socklen_t,
        );

        if result < 0 {
            let last_error = std::io::Error::last_os_error();
            if last_error.raw_os_error() != Some(libc::EINPROGRESS) {
                log::error!(
                    "Error: '{}' while connecting using `getsockopt`.",
                    std::io::Error::last_os_error()
                );
                return Err(last_error);
            }
        }
        params.assoc_id
    };

    // Safety: every value is a valid `c_int`.
    let socket_type: libc::c_int =
        unsafe { getsockopt_internal(&fd, libc::SOL_SOCKET, libc::SO_TYPE, 0)? };
    log::trace!("Waiting to connect...");
    loop {
        // With association events enabled, a failed one-to-one association may only become
        // readable. Leave its notifications queued for `sctp_recv` on a successful connection.
        let mut guard = fd.ready(Interest::READABLE | Interest::WRITABLE).await?;
        // One-to-one sockets report the actual failure in `SO_ERROR`, even when an association
        // notification is queued instead of making the socket writable.
        // Safety: every value is a valid `c_int`.
        let so_error: libc::c_int =
            unsafe { getsockopt_internal(&fd, libc::SOL_SOCKET, libc::SO_ERROR, 0)? };
        if so_error != 0 {
            return Err(std::io::Error::from_raw_os_error(so_error));
        }
        let status = sctp_get_status_internal(&fd, assoc_id).map_err(|error| {
            if error.raw_os_error() == Some(libc::EINVAL) {
                std::io::Error::from_raw_os_error(libc::ECONNREFUSED)
            } else {
                error
            }
        })?;
        if socket_type != libc::SOCK_STREAM || status.state == ConnState::Established {
            break;
        }
        // A notification can arrive while the association is still being established. Clearing
        // this readiness waits for the next state change instead of spinning on that event.
        guard.clear_ready();
    }
    log::trace!("Connected...");

    // The `ConnectedSocket` takes over the registered `fd`. Also, since this `fd` is the
    // 'original' created with `socket` call, no need to set it to non-blocking again.
    Ok((ConnectedSocket::from_async_fd(fd), assoc_id))
}

// Implementation of `accept` - we just call the `libc::accept` allowing it to fail if the socket
// type is not the right one (UDP Style `SOCK_SEQPACKET`).
pub(crate) async fn accept_internal(
    fd: &AsyncFd<OwnedFd>,
) -> std::io::Result<(ConnectedSocket, SocketAddr)> {
    // Safety: Both `addrs_buff` and `addrs_len` are in the scope and hence are valid pointers.
    unsafe {
        let raw_fd = fd.as_raw_fd();

        // This is ugly for the following reasons - On the `SEQPACKET` sockets, we do not get
        // `readable` ready at all for the `accept`.  (Why not sure? Even when tried after sending
        // some dummy data to make sure we can recv on it.) Thus we try `accept` first for
        // `SEQPACKET` sockets, this `accept` would fail with `EINVAL` and for `STREAM` sockets,
        // this 'may' fail with `EWOULDBLOCK`. If it does, we wait for `readable` event again, in
        // the next iteration of the `loop`, we won't get `EWOULDBLOCK` and will actually `accept`.
        loop {
            // this should be enough to `accept` a connection normally `sockaddr`s maximum size is
            // 28 for the `sa_family` we care about.
            let mut addrs_buff: Vec<u8> = vec![0; 32];
            let mut addrs_len = addrs_buff.len() as libc::socklen_t;

            let result = {
                let addrs_len_ptr = std::ptr::addr_of_mut!(addrs_len);
                let addrs_buff_ptr = addrs_buff.as_mut_ptr();

                // The accepted socket is non-blocking, and closed on `exec`.
                #[cfg(any(target_os = "linux", target_os = "android"))]
                let result = libc::accept4(
                    raw_fd,
                    addrs_buff_ptr as *mut _ as *mut libc::sockaddr,
                    addrs_len_ptr as *mut _ as *mut libc::socklen_t,
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                );

                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                let result = libc::accept(
                    raw_fd,
                    addrs_buff_ptr as *mut _ as *mut libc::sockaddr,
                    addrs_len_ptr as *mut _ as *mut libc::socklen_t,
                );

                result
            };

            if result < 0 {
                let last_error = std::io::Error::last_os_error();
                if last_error.raw_os_error() != Some(libc::EWOULDBLOCK) {
                    log::error!(
                        "Error: '{}' while `accept`ing on the socket.",
                        std::io::Error::last_os_error()
                    );
                    return Err(last_error);
                }

                // We got an `EWOULDBLOCK` let's wait.
                fd.readable().await?.clear_ready();
            } else {
                // Safety: `accept` returned a new descriptor, which nothing else owns.
                let accepted = OwnedFd::from_raw_fd(result);
                let os_socketaddr = OsSocketAddr::copy_from_raw(
                    addrs_buff.as_ptr() as *const _ as *const libc::sockaddr,
                    addrs_len,
                );
                log::trace!(
                    "fd: {}, result: {},  addrs_len: {}, addrs_u8: {:?}",
                    raw_fd,
                    result,
                    addrs_len,
                    addrs_buff,
                );
                let socketaddr = os_socketaddr.into_addr().unwrap();

                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                set_fd_non_blocking_cloexec(accepted.as_raw_fd())?;

                return Ok((ConnectedSocket::from_owned_fd(accepted)?, socketaddr));
            }
        }
    }
}

// Shutdown implementation for `Listener` and `ConnectedSocket`.
pub(crate) fn shutdown_internal(
    fd: &AsyncFd<OwnedFd>,
    how: std::net::Shutdown,
) -> std::io::Result<()> {
    use std::net::Shutdown;

    log::debug!("Calling 'shutdown' on socket with flags: {:?}", how);
    let flags = match how {
        Shutdown::Read => libc::SHUT_RD,
        Shutdown::Write => libc::SHUT_WR,
        Shutdown::Both => libc::SHUT_RDWR,
    };

    // Safety: No real undefined behavior as long as fd is a valid fd and if fd is not a valid fd
    // the underlying systemcall will error.
    unsafe {
        let result = libc::shutdown(fd.as_raw_fd(), flags);
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

// Implementation for the receive side for SCTP.
//
// Returns whole messages: `recvmsg` returns at most the size of its buffer and the kernel delivers
// messages longer than the receive window in several parts. The part received so far stays in
// `partial` when there is nothing more to read yet, including when the returned future is dropped.
pub(crate) async fn sctp_recvmsg_internal(
    fd: &AsyncFd<OwnedFd>,
    partial: &Mutex<Option<PartialMessage>>,
    max_message_size: usize,
) -> std::io::Result<NotificationOrData> {
    log::debug!("Receiving Message on the socket.");

    loop {
        // `try_io` clears the readiness when `recvmsg` fails with `EWOULDBLOCK`, so that we wait
        // for the socket to become readable again. A peer abort can signal only error readiness.
        let mut guard = fd.ready(Interest::READABLE | Interest::ERROR).await?;
        if let Ok(result) = guard.try_io(|inner| {
            let mut partial = partial.lock().unwrap_or_else(PoisonError::into_inner);
            sctp_recvmsg_whole(inner.as_raw_fd(), &mut partial, max_message_size)
        }) {
            return result;
        }
    }
}

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
struct Piece {
    len: usize,
    notification: bool,
    end_of_record: bool,
    rcv_info: Option<RcvInfo>,
    nxt_info: Option<NxtInfo>,
    from: Option<SocketAddr>,
}

// Reads the next message up to its end (`MSG_EOR`), or fails with `EWOULDBLOCK` after keeping the
// part received so far in `partial`.
//
// With the default `SCTP_FRAGMENT_INTERLEAVE` level 0, the parts of a message are not interleaved
// with other messages. The kernel only delivers something else after a part when it aborts the
// delivery of the message, e.g. because the association is aborted.
fn sctp_recvmsg_whole(
    rawfd: RawFd,
    partial: &mut Option<PartialMessage>,
    max_message_size: usize,
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

        let piece = sctp_recvmsg_once(rawfd, &mut message.payload[start..]);
        let piece = match piece {
            Ok(piece) => piece,
            Err(e) => {
                message.payload.truncate(start);
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

// A single `recvmsg` call into `buffer`.
fn sctp_recvmsg_once(rawfd: RawFd, buffer: &mut [u8]) -> std::io::Result<Piece> {
    // Safety: all the pointers in `recvmsg_header` point to buffers valid during the call, of the
    // lengths given with them.
    unsafe {
        let mut recv_iov = libc::iovec {
            iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
            iov_len: buffer.len(),
        };
        let mut msg_control = CmsgBuffer::new();
        let mut from: libc::sockaddr_storage = std::mem::zeroed();

        // Some platforms have private fields in `msghdr`.
        let mut recvmsg_header: libc::msghdr = std::mem::zeroed();
        recvmsg_header.msg_name = std::ptr::addr_of_mut!(from) as *mut libc::c_void;
        recvmsg_header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
        recvmsg_header.msg_iov = &mut recv_iov;
        recvmsg_header.msg_iovlen = 1;
        recvmsg_header.msg_control = msg_control.as_mut_ptr();
        recvmsg_header.msg_controllen = CMSG_BUFFER_SIZE as _;

        let flags = 0 as libc::c_int;
        let result = libc::recvmsg(rawfd, &mut recvmsg_header as *mut libc::msghdr, flags);
        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let (rcv_info, nxt_info) = rcvinfo_nxtinfo_from_cmsgs(&recvmsg_header);
        let from = OsSocketAddr::copy_from_raw(
            std::ptr::addr_of!(from) as *const libc::sockaddr,
            recvmsg_header.msg_namelen,
        )
        .into_addr();
        Ok(Piece {
            len: result as usize,
            notification: recvmsg_header.msg_flags as u32 & MSG_NOTIFICATION != 0,
            end_of_record: recvmsg_header.msg_flags & libc::MSG_EOR != 0,
            rcv_info,
            nxt_info,
            from,
        })
    }
}

// Size of the buffer for control messages. It has room for those `recvmsg` can return for SCTP,
// `SCTP_NXTINFO`, `SCTP_RCVINFO` and `SCTP_SNDRCV` (with the data I/O event), after `SOL_SOCKET`
// ones enabled on the socket, such as timestamps.
const CMSG_BUFFER_SIZE: usize = 256;

// A buffer for control messages, aligned for `cmsghdr`.
#[repr(C)]
struct CmsgBuffer {
    _align: [libc::cmsghdr; 0],
    bytes: [u8; CMSG_BUFFER_SIZE],
}

impl CmsgBuffer {
    fn new() -> Self {
        Self {
            _align: [],
            bytes: [0; CMSG_BUFFER_SIZE],
        }
    }

    fn as_mut_ptr(&mut self) -> *mut libc::c_void {
        self.bytes.as_mut_ptr() as *mut libc::c_void
    }
}

// Gets the `RcvInfo` and `NxtInfo` from the control messages `recvmsg` returned in `msghdr`.
//
// Safety: `msghdr.msg_control` must point to `msghdr.msg_controllen` initialized bytes, aligned
// for `cmsghdr`, as after a successful `recvmsg` into a `CmsgBuffer`.
unsafe fn rcvinfo_nxtinfo_from_cmsgs(msghdr: &libc::msghdr) -> (Option<RcvInfo>, Option<NxtInfo>) {
    if msghdr.msg_flags & libc::MSG_CTRUNC != 0 {
        log::warn!("Control messages truncated, `RcvInfo` or `NxtInfo` may be missing.");
    }

    let mut rcv_info = None;
    let mut nxt_info = None;
    let mut cmsghdr = libc::CMSG_FIRSTHDR(msghdr);
    while !cmsghdr.is_null() {
        // The kernel shortens the last control message if the buffer is too small for it.
        let data_len = ((*cmsghdr).cmsg_len as usize).saturating_sub(libc::CMSG_LEN(0) as usize);
        let cmsg_data = libc::CMSG_DATA(cmsghdr);
        if (*cmsghdr).cmsg_level != libc::IPPROTO_SCTP {
            log::trace!(
                "Skipping a control message of level {}.",
                (*cmsghdr).cmsg_level
            );
        } else if (*cmsghdr).cmsg_type == CmsgType::RcvInfo as i32
            && data_len >= std::mem::size_of::<RcvInfo>()
        {
            let recv_info_internal = std::ptr::read_unaligned(cmsg_data as *const RcvInfo);
            log::debug!("Received: RcvInfo: {:#?}", recv_info_internal);
            rcv_info = Some(recv_info_internal);
        } else if (*cmsghdr).cmsg_type == CmsgType::NxtInfo as i32
            && data_len >= std::mem::size_of::<NxtInfo>()
        {
            let nxt_info_internal = std::ptr::read_unaligned(cmsg_data as *const NxtInfo);
            log::debug!("Received: NxtInfo: {:#?}", nxt_info_internal);
            nxt_info = Some(nxt_info_internal);
        }

        cmsghdr = libc::CMSG_NXTHDR(msghdr, cmsghdr);
    }
    (rcv_info, nxt_info)
}

// Implementation of the Send side for SCTP.
pub(crate) async fn sctp_sendmsg_internal(
    fd: &AsyncFd<OwnedFd>,
    to: Option<SocketAddr>,
    data: SendData,
) -> std::io::Result<()> {
    loop {
        // `try_io` clears the readiness when `sendmsg` fails with `EWOULDBLOCK`, so that we wait
        // for the socket to become writable again instead of failing.
        let mut guard = fd.writable().await?;
        if let Ok(result) = guard.try_io(|inner| sctp_sendmsg_once(inner.as_raw_fd(), to, &data)) {
            return result;
        }
    }
}

// A single `sendmsg` call.
fn sctp_sendmsg_once(rawfd: RawFd, to: Option<SocketAddr>, data: &SendData) -> std::io::Result<()> {
    // Safety: All the pointers are valid because they are within the current scope.
    // Also, this is just a wrapper over `libc` call.
    unsafe {
        let mut send_iov = libc::iovec {
            iov_base: data.payload.as_ptr() as *mut libc::c_void,
            iov_len: data.payload.len(),
        };

        // We have to create this `os_sockaddr` outside the `if let ...`
        // Else it will go out of scope and we'll be using it's raw pointer.
        let os_sockaddr: OsSocketAddr;
        let (to_buffer, to_buffer_len) = if let Some(addr) = to {
            os_sockaddr = addr.into();
            let slice: &[u8] = os_sockaddr.as_ref();
            (slice.as_ptr() as *mut _, os_sockaddr.len())
        } else {
            (std::ptr::null::<OsSocketAddr>() as *mut libc::c_void, 0)
        };
        // TODO: Support copy and other send info as well.
        let mut msg_control_buffer = CmsgBuffer::new();

        let (msg_control, msg_control_size) = if data.snd_info.is_some() {
            // Safety: wrapper over `libc` call. the size of the structures are wellknown.

            (
                msg_control_buffer.as_mut_ptr(),
                libc::CMSG_SPACE(std::mem::size_of::<SendInfo>() as u32) as usize,
            )
        } else {
            (
                std::ptr::null::<libc::cmsghdr>() as *mut libc::c_void,
                0_usize,
            )
        };

        // Some platforms have private fields in `msghdr`.
        let mut sendmsg_header: libc::msghdr = std::mem::zeroed();
        sendmsg_header.msg_name = to_buffer;
        sendmsg_header.msg_namelen = to_buffer_len;
        sendmsg_header.msg_iov = &mut send_iov;
        sendmsg_header.msg_iovlen = 1;
        sendmsg_header.msg_control = msg_control;
        sendmsg_header.msg_controllen = msg_control_size as _;

        let cmsg_hdr = libc::CMSG_FIRSTHDR(&sendmsg_header);
        if !cmsg_hdr.is_null() {
            (*cmsg_hdr).cmsg_level = libc::IPPROTO_SCTP;
            (*cmsg_hdr).cmsg_type = CmsgType::SndInfo as i32;
            (*cmsg_hdr).cmsg_len =
                libc::CMSG_LEN(std::mem::size_of::<SendInfo>().try_into().unwrap())
                    .try_into()
                    .unwrap();

            let snd_info = data.snd_info.as_ref().unwrap();
            std::ptr::copy(
                snd_info as *const SendInfo as *const u8,
                libc::CMSG_DATA(cmsg_hdr),
                std::mem::size_of::<SendInfo>(),
            );
        }

        // Report a closed association as `EPIPE` without raising `SIGPIPE`.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let flags = libc::MSG_NOSIGNAL;

        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let flags = 0 as libc::c_int;

        let result = libc::sendmsg(rawfd, &mut sendmsg_header as *mut libc::msghdr, flags);
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

pub(crate) fn sctp_set_default_sendinfo_internal(
    fd: &AsyncFd<OwnedFd>,
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

fn notification_from_message(data: &[u8]) -> Notification {
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
    log::trace!(
        "notification_type: {:x}, SCTP_ASSOC_CHANGE: {:x}",
        notification_type,
        SCTP_ASSOC_CHANGE
    );
    match notification_type {
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
        SCTP_SHUTDOWN if data.len() >= SHUTDOWN_LEN => {
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

// Implementation of Event Subscription
pub(crate) fn sctp_subscribe_event_internal(
    fd: &AsyncFd<OwnedFd>,
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

// Setup initiation parameters
pub(crate) fn sctp_setup_init_params_internal(
    fd: &AsyncFd<OwnedFd>,
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

// Enable/Disable reception of `RcvInfo` actual call.
pub(crate) fn request_rcvinfo_internal(fd: &AsyncFd<OwnedFd>, on: bool) -> std::io::Result<()> {
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

// Enable/Disable reception of `NxtInfo` actual call.
pub(crate) fn request_nxtinfo_internal(fd: &AsyncFd<OwnedFd>, on: bool) -> std::io::Result<()> {
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

// Enable/Disable `SCTP_NODELAY` actual call.
pub(crate) fn sctp_set_nodelay_internal(
    fd: &AsyncFd<OwnedFd>,
    nodelay: bool,
) -> std::io::Result<()> {
    log::debug!("Setting `SCTP_NODELAY` to {}.", nodelay);
    setsockopt_internal(fd, SOL_SCTP, SCTP_NODELAY, &libc::c_int::from(nodelay))
}

// Get `SCTP_NODELAY` actual call.
pub(crate) fn sctp_nodelay_internal(fd: &AsyncFd<OwnedFd>) -> std::io::Result<bool> {
    // Safety: every value is a valid `c_int`.
    let nodelay: libc::c_int = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_NODELAY, 0)? };
    Ok(nodelay != 0)
}

// Set `SO_LINGER` actual call.
pub(crate) fn set_linger_internal(
    fd: &AsyncFd<OwnedFd>,
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

// Set `SCTP_RTOINFO` actual call.
pub(crate) fn sctp_set_rto_info_internal(
    fd: &AsyncFd<OwnedFd>,
    rto_info: RtoInfo,
) -> std::io::Result<()> {
    log::debug!("Setting `SCTP_RTOINFO` to {:?}.", rto_info);
    setsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, &rto_info)
}

// Get `SCTP_RTOINFO` actual call.
pub(crate) fn sctp_get_rto_info_internal(
    fd: &AsyncFd<OwnedFd>,
    assoc_id: AssociationId,
) -> std::io::Result<RtoInfo> {
    let rto_info = RtoInfo {
        assoc_id,
        ..Default::default()
    };
    // Safety: `RtoInfo` holds integers only, so any bytes are a valid value.
    unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, rto_info) }
}

// Enable/Disable `SO_REUSEADDR` actual call.
pub(crate) fn set_reuseaddr_internal(
    fd: &AsyncFd<OwnedFd>,
    reuseaddr: bool,
) -> std::io::Result<()> {
    log::debug!("Setting `SO_REUSEADDR` to {}.", reuseaddr);
    setsockopt_internal(
        fd,
        libc::SOL_SOCKET,
        libc::SO_REUSEADDR,
        &libc::c_int::from(reuseaddr),
    )
}

// Get `SO_REUSEADDR` actual call.
pub(crate) fn reuseaddr_internal(fd: &AsyncFd<OwnedFd>) -> std::io::Result<bool> {
    // Safety: every value is a valid `c_int`.
    let reuseaddr: libc::c_int =
        unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 0)? };
    Ok(reuseaddr != 0)
}

// Sets the option `name` at `level` to `value` using `libc::setsockopt`.
fn setsockopt_internal<T>(
    fd: &AsyncFd<OwnedFd>,
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

// Gets the option `name` at `level` using `libc::getsockopt`. `value` holds the input of the
// options that take one (such as an association ID); the kernel must fill all of it.
//
// Safety: any bytes must make a valid `T`.
unsafe fn getsockopt_internal<T>(
    fd: &AsyncFd<OwnedFd>,
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

// Get the status for the given Assoc ID
pub(crate) fn sctp_get_status_internal(
    fd: &AsyncFd<OwnedFd>,
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

// Where the descriptor cannot be created non-blocking and closed on `exec` at once.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_fd_non_blocking_cloexec(fd: RawFd) -> std::io::Result<()> {
    // Set Non Blocking
    unsafe {
        let result = libc::fcntl(fd, libc::F_GETFL, 0);
        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let flags = result | libc::O_NONBLOCK;
        let result = libc::fcntl(fd, libc::F_SETFL, flags);
        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let result = libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

// Takes ownership of `fd`, after checking that it is open.
pub(crate) fn owned_fd_from_raw(fd: RawFd) -> std::io::Result<OwnedFd> {
    // Safety: `F_GETFD` only queries `fd`. Whether the caller may give up `fd` is part of the
    // contract of the public API that calls this function.
    unsafe {
        if libc::fcntl(fd, libc::F_GETFD) < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                sctp_recvmsg_whole(socket.as_raw_fd(), &mut partial, limit)
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

    #[test]
    fn cmsg_buffer_holds_the_sctp_control_messages() {
        // Safety: `CMSG_SPACE` only computes a size.
        let sctp_cmsgs = unsafe {
            libc::CMSG_SPACE(std::mem::size_of::<NxtInfo>() as u32)
                + libc::CMSG_SPACE(std::mem::size_of::<RcvInfo>() as u32)
                // `struct sctp_sndrcvinfo`
                + libc::CMSG_SPACE(32)
        };
        // Half of the buffer is left for `SOL_SOCKET` control messages.
        assert!((sctp_cmsgs as usize) <= CMSG_BUFFER_SIZE / 2);
        assert_eq!(
            std::mem::align_of::<CmsgBuffer>(),
            std::mem::align_of::<libc::cmsghdr>()
        );
    }
}
