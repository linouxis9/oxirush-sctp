//! Descriptor creation, ownership and association lifecycle operations.
use super::{SOL_SCTP, getsockopt_internal};
use crate::AssociationId;
use crate::consts::*;
use crate::types::internal::ConnectxParam;
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

pub(crate) fn sctp_peeloff_internal(
    fd: BorrowedFd<'_>,
    assoc_id: AssociationId,
) -> std::io::Result<OwnedFd> {
    use crate::types::internal::{PeeloffArg, PeeloffFlagsArg};

    // The peeled off socket is non-blocking, and closed on `exec`.
    let flags = (libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC) as libc::c_uint;

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
            Err(error)
        } else {
            // Safety: the kernel returned a new descriptor, which nothing else owns.
            let fd = OwnedFd::from_raw_fd(peeloff_arg.p_arg.sd);

            Ok(fd)
        }
    }
}

pub(crate) fn sctp_socket_internal(
    domain: libc::c_int,
    assoc: crate::SocketToAssociation,
) -> std::io::Result<OwnedFd> {
    let socket_type = match assoc {
        crate::SocketToAssociation::OneToOne => libc::SOCK_STREAM,
        crate::SocketToAssociation::OneToMany => libc::SOCK_SEQPACKET,
    };
    // The socket is non-blocking, and closed on `exec` so that child processes do not keep its
    // associations open.
    let socket_type = socket_type | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;

    unsafe {
        let rawfd = libc::socket(domain, socket_type, libc::IPPROTO_SCTP);
        if rawfd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Safety: `socket` returned a new descriptor, which nothing else owns.
        let fd = OwnedFd::from_raw_fd(rawfd);

        Ok(fd)
    }
}

pub(crate) fn sctp_listen_internal(fd: BorrowedFd<'_>, backlog: i32) -> std::io::Result<()> {
    // Safety: listen only uses the borrowed, live descriptor.
    let result = unsafe { libc::listen(fd.as_raw_fd(), backlog) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        Err(error)
    } else {
        Ok(())
    }
}

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
    super::check_addresses(fd, addrs)?;
    let mut addrs_u8: Vec<u8> = vec![];

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

pub(crate) fn accept_once(fd: BorrowedFd<'_>) -> std::io::Result<(OwnedFd, SocketAddr)> {
    // Safety: sockaddr_storage accepts the address of either supported family.
    let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&address) as libc::socklen_t;
    // Safety: both output buffers outlive the call and have their declared sizes.
    let result = unsafe {
        libc::accept4(
            fd.as_raw_fd(),
            &mut address as *mut _ as *mut libc::sockaddr,
            &mut len,
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // Safety: accept returned a new descriptor owned only here.
    let accepted = unsafe { OwnedFd::from_raw_fd(result) };
    // Safety: accept initialized the address bytes indicated by len.
    let address =
        unsafe { OsSocketAddr::copy_from_raw(&address as *const _ as *const libc::sockaddr, len) }
            .into_addr()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unsupported peer address")
            })?;
    Ok((accepted, address))
}

pub(crate) fn shutdown_internal(
    fd: BorrowedFd<'_>,
    how: std::net::Shutdown,
) -> std::io::Result<()> {
    use std::net::Shutdown;

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
