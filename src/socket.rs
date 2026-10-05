//! SCTP socket creation and consuming role transitions.
use crate::internal::{sys, SocketCore};
use crate::{
    AssociationId, BindxFlags, ConnectedSocket, Listener, OneToManyEndpoint, SocketOptions,
    SocketToAssociation,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

/// An SCTP socket before it becomes a listener, association or one-to-many endpoint.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use oxirush_sctp::{Socket, SocketToAssociation};
///
/// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// socket.bind("127.0.0.1:0".parse().unwrap())?;
/// let listener = socket.listen(5)?;
///
/// let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let (association, _id) = client.connect(listener.local_addr()?).await?;
/// let (_accepted, peer) = listener.accept().await?;
/// assert_eq!(association.local_addr()?, peer);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Socket {
    core: SocketCore,
}

impl Socket {
    /// Create a nonblocking, close-on-exec IPv4 socket registered with Tokio.
    ///
    /// Without SCTP in the kernel (`modprobe sctp`) this fails with `EPROTONOSUPPORT`, or
    /// `ESOCKTNOSUPPORT` for a one-to-many socket.
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime with the I/O driver enabled.
    pub fn new_v4(style: SocketToAssociation) -> std::io::Result<Self> {
        Ok(Self {
            core: SocketCore::new(libc::AF_INET, style)?,
        })
    }

    /// Create a nonblocking, close-on-exec IPv6 socket registered with Tokio.
    /// Linux IPv6 sockets also support IPv4 addresses unless `IPV6_V6ONLY` is enabled.
    /// It fails and panics as [`new_v4`][Self::new_v4] does.
    pub fn new_v6(style: SocketToAssociation) -> std::io::Result<Self> {
        Ok(Self {
            core: SocketCore::new(libc::AF_INET6, style)?,
        })
    }

    /// Borrow configuration of this socket. Settings survive consuming role transitions.
    pub fn options(&self) -> SocketOptions<'_> {
        SocketOptions { core: &self.core }
    }

    /// Bind one local address.
    pub fn bind(&self, addr: SocketAddr) -> std::io::Result<()> {
        self.bindx(&[addr], BindxFlags::Add)
    }

    /// The local address as `getsockname` reports it, with the port the kernel chose for port 0:
    /// one of several bound addresses, or the wildcard address when bound to it or not bound.
    /// [`local_addrs`][Self::local_addrs] lists them all.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        sys::local_addr(self.as_fd())
    }

    /// Add or remove local addresses (`sctp_bindx`, RFC 6458 section 9.1).
    pub fn bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sys::sctp_bindx_internal(self.as_fd(), addrs, flags)
    }

    /// Query the local addresses. Before establishment, use association ID zero.
    pub fn local_addrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getladdrs_internal(self.as_fd(), assoc_id)
    }

    /// Become a one-to-one accepting listener. A one-to-many socket returns `InvalidInput`
    /// before listening; use [`into_endpoint`][Self::into_endpoint] for that role.
    pub fn listen(self, backlog: i32) -> std::io::Result<Listener> {
        self.core.require_style(SocketToAssociation::OneToOne)?;
        sys::sctp_listen_internal(self.as_fd(), backlog)?;
        Ok(Listener::from_core(self.core))
    }

    /// Become a one-to-many endpoint supporting incoming and outgoing associations.
    ///
    /// A one-to-one socket returns `InvalidInput`. This listens with the supplied backlog. The
    /// endpoint keeps this socket's registered descriptor and receive limit. A failure closes
    /// it.
    pub fn into_endpoint(self, backlog: i32) -> std::io::Result<OneToManyEndpoint> {
        self.core.require_style(SocketToAssociation::OneToMany)?;
        sys::sctp_listen_internal(self.as_fd(), backlog)?;
        Ok(OneToManyEndpoint::from_core(self.core))
    }

    /// Establish one association, consuming a one-to-one socket. Dropping the returned future
    /// closes it. Success leaves any association notification queued for `recv`.
    pub async fn connect(
        self,
        addr: SocketAddr,
    ) -> std::io::Result<(ConnectedSocket, AssociationId)> {
        self.connectx(&[addr]).await
    }

    /// Establish one multihomed association. Completes only once established, or returns the
    /// kernel failure reason. One-to-many sockets return `InvalidInput`; their endpoint's
    /// [`connect`][OneToManyEndpoint::connect] initiates associations without consuming it.
    pub async fn connectx(
        self,
        addrs: &[SocketAddr],
    ) -> std::io::Result<(ConnectedSocket, AssociationId)> {
        let (core, assoc_id) = self.core.connect(addrs).await?;
        Ok((ConnectedSocket::from_core(core), assoc_id))
    }
}

/// The owned descriptor must remain nonblocking; additional options can be set through it.
impl AsRawFd for Socket {
    fn as_raw_fd(&self) -> RawFd {
        self.core.as_raw_fd()
    }
}
impl AsFd for Socket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.core.as_fd()
    }
}
