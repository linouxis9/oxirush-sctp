//! Tokio readiness and descriptor ownership around the synchronous SCTP operations.
use crate::{AssociationId, ConnState, ConnectedSocket, NotificationOrData, SendInfo};
use std::net::SocketAddr;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::{Mutex, PoisonError};
use tokio::io::{unix::AsyncFd, Interest};
mod receive;
mod sys;
pub(crate) use receive::{PartialMessage, DEFAULT_MAX_MESSAGE_SIZE};
pub(crate) use sys::*;

pub(crate) async fn sctp_connectx_internal(
    fd: AsyncFd<OwnedFd>,
    addrs: &[SocketAddr],
) -> std::io::Result<(ConnectedSocket, AssociationId)> {
    let assoc_id = initiate_connect(fd.as_fd(), addrs)?;
    // Safety: every value is a valid `c_int`.
    let socket_type: libc::c_int = socket_type(fd.as_fd())?;
    log::trace!("Waiting to connect...");
    loop {
        // With association events enabled, a failed one-to-one association may only become
        // readable. Leave its notifications queued for `sctp_recv` on a successful connection.
        let mut guard = fd.ready(Interest::READABLE | Interest::WRITABLE).await?;
        // One-to-one sockets report the actual failure in `SO_ERROR`, even when an association
        // notification is queued instead of making the socket writable.
        // Safety: every value is a valid `c_int`.
        let so_error: libc::c_int =
            unsafe { getsockopt_internal(fd.as_fd(), libc::SOL_SOCKET, libc::SO_ERROR, 0)? };
        if so_error != 0 {
            return Err(std::io::Error::from_raw_os_error(so_error));
        }
        let status = sctp_get_status_internal(fd.as_fd(), assoc_id).map_err(|error| {
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

pub(crate) async fn accept_internal(
    fd: &AsyncFd<OwnedFd>,
) -> std::io::Result<(ConnectedSocket, SocketAddr)> {
    // Try first so the invalid one-to-many operation reports its error without waiting.
    match accept_once(fd.as_fd()) {
        Ok((accepted, address)) => return Ok((ConnectedSocket::from_owned_fd(accepted)?, address)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    loop {
        let mut guard = fd.readable().await?;
        if let Ok(result) = guard.try_io(|inner| accept_once(inner.as_fd())) {
            let (accepted, address) = result?;
            return Ok((ConnectedSocket::from_owned_fd(accepted)?, address));
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
            receive::receive_message(&mut partial, max_message_size, |buffer| {
                sctp_recvmsg_once(inner.as_fd(), buffer)
            })
        }) {
            return result;
        }
    }
}

// Implementation of the Send side for SCTP.
pub(crate) async fn sctp_sendmsg_internal(
    fd: &AsyncFd<OwnedFd>,
    to: Option<SocketAddr>,
    payload: &[u8],
    snd_info: Option<&SendInfo>,
) -> std::io::Result<()> {
    loop {
        // `try_io` clears the readiness when `sendmsg` fails with `EWOULDBLOCK`, so that we wait
        // for the socket to become writable again instead of failing.
        let mut guard = fd.writable().await?;
        if let Ok(result) =
            guard.try_io(|inner| sctp_sendmsg_once(inner.as_fd(), to, payload, snd_info))
        {
            return result;
        }
    }
}
