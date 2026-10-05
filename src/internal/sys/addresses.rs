//! Binding and association address queries using packed native socket addresses.
use super::SOL_SCTP;
use crate::consts::*;
use crate::{AssociationId, BindxFlags};
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd};

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

pub(crate) fn local_addr(fd: BorrowedFd<'_>) -> std::io::Result<SocketAddr> {
    let mut address = OsSocketAddr::new();
    let mut len = address.capacity();
    // Safety: `address` is valid for writes of `len` octets during the call.
    if unsafe { libc::getsockname(fd.as_raw_fd(), address.as_mut_ptr(), &mut len) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    address
        .into_addr()
        .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))
}

pub(crate) fn sctp_getpaddrs_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    sctp_getaddrs_internal(fd, SCTP_GET_PEER_ADDRS, assoc_id)
}

pub(crate) fn sctp_getladdrs_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<Vec<SocketAddr>> {
    sctp_getaddrs_internal(fd, SCTP_GET_LOCAL_ADDRS, assoc_id)
}

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
