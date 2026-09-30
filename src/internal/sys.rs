//! Synchronous Linux SCTP socket operations and their ABI boundary.
use super::receive::Piece;
use crate::consts::*;
use crate::types::internal::{
    ConnStatusInternal, ConnectxParam, InitMsg, PeerAddressParamsInternal, SubscribeEvent,
};
use crate::{
    AssocChangeState, AssociationChange, AssociationId, BindxFlags, CmsgType, ConnStatus, Event,
    EventSubscriptionError, Notification, NxtInfo, PeerAddressChange, PeerAddressParams,
    PeerAddressState, RcvInfo, RtoInfo, SendFailure, SendInfo, Shutdown, SubscribeEventAssocId,
};
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
static SOL_SCTP: libc::c_int = libc::IPPROTO_SCTP;

// Implementation of `sctp_bindx` using `libc::setsockopt`
pub(crate) fn sctp_bindx_internal(
    fd: BorrowedFd<'_>,
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
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<OwnedFd> {
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

            Ok(fd)
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

// Start listening without changing descriptor ownership or registration.
pub(crate) fn sctp_listen_internal(fd: BorrowedFd<'_>, backlog: i32) -> std::io::Result<()> {
    // Safety: listen only uses the borrowed, live descriptor.
    let result = unsafe { libc::listen(fd.as_raw_fd(), backlog) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        log::error!("Error: {} during `sctp_listen`.", error);
        Err(error)
    } else {
        Ok(())
    }
}

// Implmentation of `sctp_getpaddrs` using `libc::getsockopt`
pub(crate) fn sctp_getpaddrs_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    sctp_getaddrs_internal(fd, SCTP_GET_PEER_ADDRS, assoc_id)
}

// Implmentation of `sctp_getladdrs` using `libc::getsockopt`
pub(crate) fn sctp_getladdrs_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    sctp_getaddrs_internal(fd, SCTP_GET_LOCAL_ADDRS, assoc_id)
}

// Actual function performing `sctp_getpaddrs` or `sctp_getladdrs`
fn sctp_getaddrs_internal(
    fd: BorrowedFd<'_>,
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
                fd.as_raw_fd(),
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
// Start an association; one-to-many completion arrives as an association notification.
pub(crate) fn initiate_connect(
    fd: BorrowedFd<'_>,
    addrs: &[SocketAddr],
) -> std::io::Result<AssociationId> {
    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no peer addresses",
        ));
    }
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
    // to raw data is valid. `params` holds a raw pointer, valid for this synchronous call only.
    let assoc_id = unsafe {
        let mut params = ConnectxParam {
            assoc_id: 0,
            addrs_size: addrs_len.try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "too many peer addresses")
            })?,
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
                    last_error
                );
                return Err(last_error);
            }
        }
        params.assoc_id
    };

    Ok(assoc_id)
}

pub(crate) fn socket_type(fd: BorrowedFd<'_>) -> std::io::Result<libc::c_int> {
    // Safety: every bit pattern is a valid c_int.
    unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_TYPE, 0) }
}

pub(crate) fn connect_association(
    fd: BorrowedFd<'_>,
    addrs: &[SocketAddr],
) -> std::io::Result<AssociationId> {
    if socket_type(fd)? != libc::SOCK_SEQPACKET {
        return Err(std::io::Error::from_raw_os_error(libc::EOPNOTSUPP));
    }
    initiate_connect(fd, addrs)
}

// Accept without awaiting or registering the new descriptor.
pub(crate) fn accept_once(fd: BorrowedFd<'_>) -> std::io::Result<(OwnedFd, SocketAddr)> {
    // Safety: sockaddr_storage accepts the address of either supported family.
    let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&address) as libc::socklen_t;
    // Safety: both output buffers outlive the call and have their declared sizes.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let result = unsafe {
        libc::accept4(
            fd.as_raw_fd(),
            &mut address as *mut _ as *mut libc::sockaddr,
            &mut len,
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let result = unsafe {
        libc::accept(
            fd.as_raw_fd(),
            &mut address as *mut _ as *mut libc::sockaddr,
            &mut len,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // Safety: accept returned a new descriptor owned only here.
    let accepted = unsafe { OwnedFd::from_raw_fd(result) };
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    set_fd_non_blocking_cloexec(accepted.as_raw_fd())?;
    // Safety: accept initialized the address bytes indicated by len.
    let address =
        unsafe { OsSocketAddr::copy_from_raw(&address as *const _ as *const libc::sockaddr, len) }
            .into_addr()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unsupported peer address")
            })?;
    Ok((accepted, address))
}

// Shutdown implementation for `Listener` and `ConnectedSocket`.
pub(crate) fn shutdown_internal(
    fd: BorrowedFd<'_>,
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

// A single `recvmsg` call into `buffer`.
pub(super) fn sctp_recvmsg_once(fd: BorrowedFd<'_>, buffer: &mut [u8]) -> std::io::Result<Piece> {
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
        let result = libc::recvmsg(
            fd.as_raw_fd(),
            &mut recvmsg_header as *mut libc::msghdr,
            flags,
        );
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

// A single `sendmsg` call.
pub(super) fn sctp_sendmsg_once(
    fd: BorrowedFd<'_>,
    to: Option<SocketAddr>,
    payload: &[u8],
    snd_info: Option<&SendInfo>,
) -> std::io::Result<()> {
    // Safety: All the pointers are valid because they are within the current scope.
    // Also, this is just a wrapper over `libc` call.
    unsafe {
        let mut send_iov = libc::iovec {
            iov_base: payload.as_ptr() as *mut libc::c_void,
            iov_len: payload.len(),
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

        let (msg_control, msg_control_size) = if snd_info.is_some() {
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

            let snd_info = snd_info.unwrap();
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

        let result = libc::sendmsg(
            fd.as_raw_fd(),
            &mut sendmsg_header as *mut libc::msghdr,
            flags,
        );
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
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

pub(super) fn notification_from_message(data: &[u8]) -> Notification {
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
        value
            if value == Event::Address as u16
                && data.len() == 148
                && u32::from_ne_bytes(data[4..8].try_into().unwrap()) as usize == data.len() =>
        {
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
        value
            if value == Event::SendFailureEvent as u16
                && data.len() >= 32
                && u32::from_ne_bytes(data[4..8].try_into().unwrap()) as usize == data.len() =>
        {
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

fn socket_address(storage: &[u8]) -> Option<SocketAddr> {
    let family = u16::from_ne_bytes(storage.get(..2)?.try_into().ok()?) as i32;
    let len = match family {
        libc::AF_INET => std::mem::size_of::<libc::sockaddr_in>(),
        libc::AF_INET6 => std::mem::size_of::<libc::sockaddr_in6>(),
        _ => return None,
    };
    let bytes = storage.get(..len)?;
    // Safety: the recognized family determines the initialized length copied into OsSocketAddr.
    unsafe { OsSocketAddr::copy_from_raw(bytes.as_ptr().cast(), len as libc::socklen_t) }
        .into_addr()
}

// Implementation of Event Subscription
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

// Setup initiation parameters
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

// Enable/Disable reception of `RcvInfo` actual call.
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

// Enable/Disable reception of `NxtInfo` actual call.
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

// Enable/Disable `SCTP_NODELAY` actual call.
pub(crate) fn sctp_set_nodelay_internal(fd: BorrowedFd<'_>, nodelay: bool) -> std::io::Result<()> {
    log::debug!("Setting `SCTP_NODELAY` to {}.", nodelay);
    setsockopt_internal(fd, SOL_SCTP, SCTP_NODELAY, &libc::c_int::from(nodelay))
}

// Get `SCTP_NODELAY` actual call.
pub(crate) fn sctp_nodelay_internal(fd: BorrowedFd<'_>) -> std::io::Result<bool> {
    // Safety: every value is a valid `c_int`.
    let nodelay: libc::c_int = unsafe { getsockopt_internal(fd, SOL_SCTP, SCTP_NODELAY, 0)? };
    Ok(nodelay != 0)
}

// Set `SO_LINGER` actual call.
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

// Set `SCTP_RTOINFO` actual call.
pub(crate) fn sctp_set_rto_info_internal(
    fd: BorrowedFd<'_>,
    rto_info: RtoInfo,
) -> std::io::Result<()> {
    log::debug!("Setting `SCTP_RTOINFO` to {:?}.", rto_info);
    setsockopt_internal(fd, SOL_SCTP, SCTP_RTOINFO, &rto_info)
}

// Get `SCTP_RTOINFO` actual call.
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

// Enable/Disable `SO_REUSEADDR` actual call.
pub(crate) fn set_reuseaddr_internal(fd: BorrowedFd<'_>, reuseaddr: bool) -> std::io::Result<()> {
    log::debug!("Setting `SO_REUSEADDR` to {}.", reuseaddr);
    setsockopt_internal(
        fd,
        libc::SOL_SOCKET,
        libc::SO_REUSEADDR,
        &libc::c_int::from(reuseaddr),
    )
}

// Get `SO_REUSEADDR` actual call.
pub(crate) fn reuseaddr_internal(fd: BorrowedFd<'_>) -> std::io::Result<bool> {
    // Safety: every value is a valid `c_int`.
    let reuseaddr: libc::c_int =
        unsafe { getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 0)? };
    Ok(reuseaddr != 0)
}

// Sets the option `name` at `level` to `value` using `libc::setsockopt`.
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

// Gets the option `name` at `level` using `libc::getsockopt`. `value` holds the input of the
// options that take one (such as an association ID); the kernel must fill all of it.
//
// Safety: any bytes must make a valid `T`.
pub(super) unsafe fn getsockopt_internal<T>(
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

// Get the status for the given Assoc ID
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
