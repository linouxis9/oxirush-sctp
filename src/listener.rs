//! One-to-one association listener.
use crate::internal::{sys, SocketCore};
use crate::{AssociationId, BindxFlags, ConnectedSocket, SocketOptions};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

/// A one-to-one listener created by [`Socket::listen`][crate::Socket::listen].
///
/// Accepting creates a separate [`ConnectedSocket`] for each association. For shared
/// multi-association I/O, use [`OneToManyEndpoint`][crate::OneToManyEndpoint].
///
/// ```no_run
/// # async fn serve(listener: oxirush_sctp::Listener) -> std::io::Result<()> {
/// loop {
///     let (association, peer) = listener.accept().await?;
///     tokio::spawn(async move {
///         while let Ok(received) = association.recv().await {
///             println!("{peer}: {received:?}");
///         }
///     });
/// }
/// # }
/// ```
///
/// ```compile_fail
/// fn receive_on_listener(listener: &oxirush_sctp::Listener) {
///     let _ = listener.recv();
/// }
/// ```
#[derive(Debug)]
pub struct Listener {
    core: SocketCore,
}

impl Listener {
    /// Accept the next association. Its receive limit starts at the listener's current limit,
    /// and it inherits the listener's kernel options and event subscriptions.
    pub async fn accept(&self) -> std::io::Result<(ConnectedSocket, SocketAddr)> {
        let (core, address) = self.core.accept().await?;
        Ok((ConnectedSocket::from_core(core), address))
    }

    /// Borrow shared socket configuration.
    pub fn options(&self) -> SocketOptions<'_> {
        SocketOptions { core: &self.core }
    }

    /// The local address as `getsockname` reports it, with the port the kernel chose for port 0:
    /// one of several bound addresses, or the wildcard address when bound to it or not bound.
    /// [`local_addrs`][Self::local_addrs] lists them all.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        sys::local_addr(self.as_fd())
    }

    /// Add or remove local addresses.
    pub fn bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sys::sctp_bindx_internal(self.as_fd(), addrs, flags)
    }

    /// Query listening addresses; use association ID zero.
    pub fn local_addrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getladdrs_internal(self.as_fd(), assoc_id)
    }

    pub(crate) fn from_core(core: SocketCore) -> Self {
        Self { core }
    }
}

/// The owned descriptor must remain nonblocking; additional options can be set through it.
impl AsRawFd for Listener {
    fn as_raw_fd(&self) -> RawFd {
        self.core.as_raw_fd()
    }
}
impl AsFd for Listener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.core.as_fd()
    }
}
