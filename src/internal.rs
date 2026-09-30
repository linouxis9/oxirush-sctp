//! One registered descriptor and its record state, shared by the public socket roles.
use crate::{AssociationId, ConnState, NotificationOrData, SendInfo, SocketToAssociation};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use tokio::io::{unix::AsyncFd, Interest};

mod notifications;
mod receive;
pub(crate) mod sys;
use receive::{PartialMessage, DEFAULT_MAX_MESSAGE_SIZE};

#[derive(Debug)]
pub(crate) struct SocketCore {
    fd: AsyncFd<OwnedFd>,
    style: SocketToAssociation,
    partial: Mutex<Option<PartialMessage>>,
    max_message_size: AtomicUsize,
}

impl SocketCore {
    pub(crate) fn new(domain: libc::c_int, style: SocketToAssociation) -> std::io::Result<Self> {
        let fd = sys::sctp_socket_internal(domain, style.clone())?;
        Self::register(fd, style)
    }

    fn register(fd: OwnedFd, style: SocketToAssociation) -> std::io::Result<Self> {
        Ok(Self {
            fd: AsyncFd::new(fd)?,
            style,
            partial: Mutex::new(None),
            max_message_size: AtomicUsize::new(DEFAULT_MAX_MESSAGE_SIZE),
        })
    }

    pub(crate) fn from_association(fd: OwnedFd) -> std::io::Result<Self> {
        // accept and peeloff return a single-association descriptor. Linux keeps SO_TYPE as
        // SOCK_SEQPACKET on peeled-off associations, so their role cannot be inferred from it.
        Self::register(fd, SocketToAssociation::OneToOne)
    }

    pub(crate) fn require_style(&self, style: SocketToAssociation) -> std::io::Result<()> {
        if self.style == style {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "operation is incompatible with the SCTP socket style",
            ))
        }
    }

    pub(crate) fn max_message_size(&self) -> usize {
        self.max_message_size.load(Ordering::Relaxed)
    }

    pub(crate) fn set_max_message_size(&self, octets: usize) {
        self.max_message_size.store(octets, Ordering::Relaxed);
    }

    pub(crate) async fn connect(
        self,
        addrs: &[SocketAddr],
    ) -> std::io::Result<(Self, AssociationId)> {
        self.require_style(SocketToAssociation::OneToOne)?;
        let assoc_id = sys::initiate_connect(self.as_fd(), addrs)?;
        loop {
            // Failure can make a socket readable when an association event is queued. Successful
            // connection leaves that event queued for the first receive.
            let mut guard = self
                .fd
                .ready(Interest::READABLE | Interest::WRITABLE)
                .await?;
            let error = sys::socket_error(self.as_fd())?;
            if error != 0 {
                return Err(std::io::Error::from_raw_os_error(error));
            }
            let status =
                sys::sctp_get_status_internal(self.as_fd(), assoc_id).map_err(|error| {
                    if error.raw_os_error() == Some(libc::EINVAL) {
                        std::io::Error::from_raw_os_error(libc::ECONNREFUSED)
                    } else {
                        error
                    }
                })?;
            if status.state == ConnState::Established {
                return Ok((self, assoc_id));
            }
            guard.clear_ready();
        }
    }

    pub(crate) async fn accept(&self) -> std::io::Result<(Self, SocketAddr)> {
        self.require_style(SocketToAssociation::OneToOne)?;
        let (fd, address) = loop {
            let mut guard = self.fd.readable().await?;
            if let Ok(result) = guard.try_io(|inner| sys::accept_once(inner.as_fd())) {
                break result?;
            }
        };
        let accepted = Self::from_association(fd)?;
        accepted.set_max_message_size(self.max_message_size());
        Ok((accepted, address))
    }

    pub(crate) fn peeloff(&self, assoc_id: AssociationId) -> std::io::Result<Self> {
        self.require_style(SocketToAssociation::OneToMany)?;
        let partial = self.partial.lock().unwrap_or_else(PoisonError::into_inner);
        if partial
            .as_ref()
            .is_some_and(|record| record.belongs_to(assoc_id))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot peel off an association during partial record delivery",
            ));
        }
        let peeled = Self::from_association(sys::sctp_peeloff_internal(self.as_fd(), assoc_id)?)?;
        peeled.set_max_message_size(self.max_message_size());
        Ok(peeled)
    }

    pub(crate) async fn recv(&self) -> std::io::Result<NotificationOrData> {
        loop {
            let mut guard = self.fd.ready(Interest::READABLE | Interest::ERROR).await?;
            if let Ok(result) = guard.try_io(|inner| {
                let mut partial = self.partial.lock().unwrap_or_else(PoisonError::into_inner);
                receive::receive_message(&mut partial, self.max_message_size(), |buffer| {
                    sys::sctp_recvmsg_once(inner.as_fd(), buffer)
                })
            }) {
                return result;
            }
        }
    }

    pub(crate) async fn send(
        &self,
        to: Option<SocketAddr>,
        payload: &[u8],
        info: Option<&SendInfo>,
    ) -> std::io::Result<()> {
        loop {
            let mut guard = self.fd.writable().await?;
            if let Ok(result) =
                guard.try_io(|inner| sys::sctp_sendmsg_once(inner.as_fd(), to, payload, info))
            {
                return result;
            }
        }
    }
}

impl AsFd for SocketCore {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for SocketCore {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn peeloff_rejects_partial_record_ownership_before_touching_the_kernel() {
        for identity in [Some(7), None] {
            let core = SocketCore::new(libc::AF_INET, SocketToAssociation::OneToMany).unwrap();
            let mut partial = None;
            let mut first = true;
            let error = receive::receive_message(&mut partial, 100, |buffer| {
                if !first {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                }
                first = false;
                buffer[..7].copy_from_slice(b"partial");
                Ok(receive::Piece {
                    len: 7,
                    notification: false,
                    end_of_record: false,
                    rcv_info: identity.map(|assoc_id| crate::RcvInfo {
                        assoc_id,
                        ..Default::default()
                    }),
                    nxt_info: None,
                    from: None,
                })
            })
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            *core.partial.lock().unwrap() = partial;
            let error = core.peeloff(7).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(error.raw_os_error(), None, "peeloff reached the kernel");
            assert!(core.partial.lock().unwrap().is_some());
            if identity.is_some() {
                let error = core.peeloff(8).unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
                assert!(core.partial.lock().unwrap().is_some());
            }
        }
    }
}
