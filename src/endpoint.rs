//! One-to-many SCTP association endpoint.
use crate::internal::{sys, SocketCore};
use crate::{
    AssociationId, BindxFlags, ConnStatus, ConnectedSocket, NotificationOrData, SendData,
    SendOptions, SocketOptions,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

/// A socket sharing I/O across multiple incoming and outgoing associations.
///
/// Created by [`Socket::into_endpoint`][crate::Socket::into_endpoint]. Receive metadata is
/// enabled by default so messages identify their association, including multihomed peers.
/// Subscribe to association events before connecting to observe establishment and failure.
/// Each association can be peeled into its own [`ConnectedSocket`]; this type never accepts.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
///
/// let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
/// socket.bind("127.0.0.1:0".parse().unwrap())?;
/// let endpoint = socket.into_endpoint(5)?;
/// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// # let (client, _) = client.connect(endpoint.local_addr()?).await?;
/// # client.send(b"request", SendOptions::default()).await?;
///
/// // Answer a message on the association it came from.
/// if let NotificationOrData::Data(message) = endpoint.recv().await? {
///     let options = SendOptions {
///         assoc_id: message.assoc_id().unwrap(),
///         ..Default::default()
///     };
///     endpoint.send(b"answer", options).await?;
/// }
/// # Ok(())
/// # }
/// ```
///
/// ```compile_fail
/// fn accept_endpoint(endpoint: &oxirush_sctp::OneToManyEndpoint) {
///     let _ = endpoint.accept();
/// }
/// ```
#[derive(Debug)]
pub struct OneToManyEndpoint {
    core: SocketCore,
}

impl OneToManyEndpoint {
    /// Borrow shared socket configuration.
    pub fn options(&self) -> SocketOptions<'_> {
        SocketOptions { core: &self.core }
    }

    /// Initiate an outgoing, possibly multihomed association without consuming the endpoint.
    /// Returns before establishment; [`Event::Association`][crate::Event::Association]
    /// notifications report the result. The returned ID selects a subsequent send.
    pub fn connect(&self, addrs: &[SocketAddr]) -> std::io::Result<AssociationId> {
        sys::initiate_connect(self.as_fd(), addrs)
    }

    /// Receive a complete record or notification. Cancellation retains partial delivery.
    ///
    /// Assembly uses `MSG_EOR` and assumes `SCTP_FRAGMENT_INTERLEAVE` level zero. Records over
    /// `options().max_message_size()` are drained before reporting `InvalidData`.
    pub async fn recv(&self) -> std::io::Result<NotificationOrData> {
        self.core.recv().await
    }

    /// Send a complete borrowed record to `options.assoc_id`. A zero association ID without a
    /// destination is rejected. PPID is in host byte order; waits for send-buffer capacity.
    pub async fn send(&self, payload: &[u8], options: SendOptions) -> std::io::Result<()> {
        if options.assoc_id == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a destination association is required",
            ));
        }
        let info = options.wire_info();
        self.core.send(None, payload, Some(&info)).await
    }

    /// Send a borrowed record to a peer address. A new association may be created by the kernel
    /// when no existing association matches. Cancellation before completion sends no fragment.
    pub async fn send_to(
        &self,
        address: SocketAddr,
        payload: &[u8],
        options: SendOptions,
    ) -> std::io::Result<()> {
        let info = options.wire_info();
        self.core.send(Some(address), payload, Some(&info)).await
    }

    /// Send an owned record with wire-order low-level ancillary data. `to` may be `None` when
    /// `data.snd_info` selects an association (including SCTP EOF/ABORT controls).
    pub async fn send_data(&self, to: Option<SocketAddr>, data: SendData) -> std::io::Result<()> {
        self.core
            .send(to, &data.payload, data.snd_info.as_ref())
            .await
    }

    /// Move an association and its queued messages into a separate one-to-one socket.
    ///
    /// A partially delivered record already belongs to this endpoint, not to the kernel.
    /// Peeling that association during partial delivery returns `InvalidInput`; finish or drain
    /// its record first. The returned association inherits this endpoint's receive limit.
    pub fn peeloff(&self, assoc_id: AssociationId) -> std::io::Result<ConnectedSocket> {
        Ok(ConnectedSocket::from_core(self.core.peeloff(assoc_id)?))
    }

    /// The local address as `getsockname` reports it, with the port the kernel chose for port 0:
    /// one of several bound addresses, or the wildcard address when bound to it or not bound.
    /// [`sctp_getladdrs`][Self::sctp_getladdrs] lists them all.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        sys::local_addr(self.as_fd())
    }

    /// Add or remove local addresses across associations, subject to kernel ASCONF policy.
    pub fn sctp_bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sys::sctp_bindx_internal(self.as_fd(), addrs, flags)
    }

    /// Query an association's peer addresses.
    pub fn sctp_getpaddrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getpaddrs_internal(self.as_fd(), assoc_id)
    }

    /// Query association local addresses; zero selects the endpoint's bound addresses.
    pub fn sctp_getladdrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getladdrs_internal(self.as_fd(), assoc_id)
    }

    /// Query one association's connection status.
    pub fn sctp_get_status(&self, assoc_id: AssociationId) -> std::io::Result<ConnStatus> {
        sys::sctp_get_status_internal(self.as_fd(), assoc_id)
    }

    pub(crate) fn from_core(core: SocketCore) -> Self {
        Self { core }
    }
}

/// The owned descriptor must remain nonblocking; additional options can be set through it.
impl AsRawFd for OneToManyEndpoint {
    fn as_raw_fd(&self) -> RawFd {
        self.core.as_raw_fd()
    }
}
impl AsFd for OneToManyEndpoint {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.core.as_fd()
    }
}
