//! SCTP Socket: An unconnected SCTP Socket

use std::net::SocketAddr;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

use tokio::io::unix::AsyncFd;

use crate::{
    AssociationId, BindxFlags, ConnStatus, ConnectedSocket, Event, Listener, PeerAddressParams,
    RtoInfo, SocketToAssociation, SubscribeEventAssocId,
};

#[allow(unused)]
use super::internal::*;

/// A structure representing an unconnected SCTP Socket.
///
/// When we `listen` on this socket, we get an [`Listener`] on which we can `accept` to
/// get a [`ConnectedSocket`] (This is like `TCPStream` but since this can have multiple
/// associations, we are calling it a 'connected' socket).
pub struct Socket {
    inner: AsyncFd<OwnedFd>,
}

impl Socket {
    /// Create a New IPv4 family socket.
    ///
    /// [`SocketToAssociation`] determines the type of the socket created. For a TCP style
    /// socket use [`OneToOne`][`SocketToAssociation::OneToOne`] and for a UDP style socket use
    /// [`OneToMany`][`SocketToAssociation::OneToMany`]. The socket created is set to a
    /// non-blocking socket and is registered for polling for read-write events.
    /// For any potentially blocking I/O operations, whether the socket is 'readable' or
    /// 'writable' is handled internally.
    pub fn new_v4(assoc: SocketToAssociation) -> std::io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(sctp_socket_internal(libc::AF_INET, assoc)?)?,
        })
    }

    /// Create a New IPv6 family socket.
    ///
    /// [`SocketToAssociation`] determines the type of the socket created. For a TCP style
    /// socket use [`SocketToAssociation::OneToOne`] and for a UDP style socket use
    /// [`SocketToAssociation::OneToMany`]. The socket created is set to a non-blocking
    /// socket and is registered for polling for read-write events. For any potentially blocking
    /// I/O operations, whether the socket is 'readable' or 'writable' is handled internally.
    pub fn new_v6(assoc: SocketToAssociation) -> std::io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(sctp_socket_internal(libc::AF_INET6, assoc)?)?,
        })
    }

    /// Bind a socket to a given IP Address.
    ///
    /// The passed IP address can be an IPv4 or an IPv6, IP address. For the IPv6 family sockets,
    /// it is possible to bind to both IPv4 and IPv6 addresses. IPv4 family sockets can be bound
    /// only to IPv4 addresses only.
    pub fn bind(&self, addr: SocketAddr) -> std::io::Result<()> {
        self.sctp_bindx(&[addr], BindxFlags::Add)
    }

    /// Listen on a given socket.
    ///
    /// This successful operation  returns [`Listener`] consuming this structure. The `backlog`
    /// parameter determines the length of the listen queue.
    pub fn listen(self, backlog: i32) -> std::io::Result<Listener> {
        sctp_listen_internal(self.inner.as_fd(), backlog)?;
        Ok(Listener::from_async_fd(self.inner))
    }

    /// Connect to SCTP Server.
    ///
    /// The successful operation returns [`ConnectedSocket`] consuming this structure. See
    /// [`sctp_connectx`][Self::sctp_connectx] for when it completes.
    pub async fn connect(
        self,
        addr: SocketAddr,
    ) -> std::io::Result<(ConnectedSocket, AssociationId)> {
        sctp_connectx_internal(self.inner, &[addr]).await
    }

    /// SCTP Specific extension for binding to multiple addresses on a given socket. See Section
    /// 9.1 RFC 6458.
    ///
    /// `sctp_bindx` API can be used to add or remove additional addresses to an unbound (ie newly
    /// created socket) or a socket that is already bound to address(es) (flag
    /// [`Add`][`BindxFlags::Add`]).  It is also possible to 'remove' bound addresses from the
    /// socket using the same API (flag [`Remove`][`BindxFlags::Remove`]). See the section 9.1
    /// for more details about the semantics of which addresses are acceptable for addition or
    /// removoal using the `sctp_bindx` API.
    pub fn sctp_bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sctp_bindx_internal(self.inner.as_fd(), addrs, flags)
    }

    /// Connect to a multi-homed Peer. See Section 9.9 RFC 6458
    ///
    /// An Unbound socket when connected to a remote end would return a tuple containing a
    /// [connected socket][`ConnectedSocket`] and an [associaton ID][`AssociationId`]. In
    /// the case of One-to-many sockets, this association ID can be used for subscribing to SCTP
    /// events and requesting additional anciliary control data on the socket.
    ///
    /// For a One-to-one socket, this completes when the association is established, or fails
    /// with the reason it could not be, e.g. `ETIMEDOUT` when the INIT was not answered or
    /// `ECONNREFUSED` when the peer aborted it. For a One-to-many socket, this completes while
    /// the association is being set up: an [`AssociationChange`][crate::AssociationChange]
    /// notification tells how that ends. Dropping the returned future closes the socket.
    pub async fn sctp_connectx(
        self,
        addrs: &[SocketAddr],
    ) -> std::io::Result<(ConnectedSocket, AssociationId)> {
        sctp_connectx_internal(self.inner, addrs).await
    }

    /// Subscribe to a given SCTP Event on the given socket. See section 6.2.1 of RFC6458.
    ///
    /// SCTP allows receiving notifications about the changes to SCTP associations etc from the
    /// user space. For these notification events to be received, this API is used to subsribe for
    /// the events while receiving the data on the SCTP Socket.
    #[deprecated(since = "0.2.2", note = "use sctp_subscribe_events instead.")]
    pub fn sctp_subscribe_event(
        &self,
        event: Event,
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        sctp_subscribe_event_internal(self.inner.as_fd(), event, assoc_id, true)
    }

    /// Unsubscribe from a given SCTP Event on the given socket. See section 6.2.1 of RFC6458.
    ///
    /// See [`sctp_subscribe_event`][`Self::sctp_subscribe_event`] for further details.
    #[deprecated(since = "0.2.2", note = "use sctp_unsubscribe_events instead.")]
    pub fn sctp_unsubscribe_event(
        &self,
        event: Event,
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        sctp_subscribe_event_internal(self.inner.as_fd(), event, assoc_id, false)
    }

    /// Subscribe to SCTP Events. See section 6.2.1 of RFC6458.
    ///
    /// SCTP allows receiving notifications about the changes to SCTP associations etc from the
    /// user space. Every event is attempted; on failure the error contains
    /// [`EventSubscriptionError`][crate::EventSubscriptionError] with the failed events and errno values.
    pub fn sctp_subscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        sctp_subscribe_events_internal(self.inner.as_fd(), events, assoc_id, true)
    }

    /// Unsubscribe from a given SCTP Event on the given socket. See section 6.2.1 of RFC6458.
    ///
    /// See [`sctp_subscribe_events`][`Self::sctp_subscribe_events`] for further details.
    pub fn sctp_unsubscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        sctp_subscribe_events_internal(self.inner.as_fd(), events, assoc_id, false)
    }

    /// Setup parameters for a new association.
    ///
    /// To specify custom parameters for a new association this API is used.
    pub fn sctp_setup_init_params(
        &self,
        ostreams: u16,
        istreams: u16,
        retries: u16,
        timeout: u16,
    ) -> std::io::Result<()> {
        sctp_setup_init_params_internal(self.inner.as_fd(), ostreams, istreams, retries, timeout)
    }

    /// Request to receive `RcvInfo` ancillary data.
    ///
    /// SCTP allows receiving ancillary data about the curent data received on the given socket.
    /// This API is used to obtain receive side additional info when the data is to be received.
    pub fn sctp_request_rcvinfo(&self, on: bool) -> std::io::Result<()> {
        request_rcvinfo_internal(self.inner.as_fd(), on)
    }

    /// Request to receive `NxtInfo` ancillary data.
    ///
    /// SCTP allows receiving ancillary data about the curent data received on the given socket.
    /// This API is used to obtain information about the next datagram that will be received.
    pub fn sctp_request_nxtinfo(&self, on: bool) -> std::io::Result<()> {
        request_nxtinfo_internal(self.inner.as_fd(), on)
    }

    /// Get the status of the connection associated with the association ID.
    pub fn sctp_get_status(&self, assoc_id: AssociationId) -> std::io::Result<ConnStatus> {
        sctp_get_status_internal(self.inner.as_fd(), assoc_id)
    }

    /// Enables or disables `SCTP_NODELAY` (Section 8.1.5 of RFC 6458).
    ///
    /// Like Nagle's algorithm in TCP, the Linux SCTP stack holds back a small message while data
    /// sent earlier is unacknowledged, to bundle it with the next ones. As the peer delays its
    /// acknowledgements (by up to 200 ms by default), a request can wait that long for no gain.
    /// With `nodelay` set, messages are sent as soon as the congestion window allows.
    ///
    /// On Linux, sockets accepted from a listening socket or peeled off it inherit its setting.
    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        sctp_set_nodelay_internal(self.inner.as_fd(), nodelay)
    }

    /// Whether `SCTP_NODELAY` is set. See [`set_nodelay`][Self::set_nodelay].
    pub fn nodelay(&self) -> std::io::Result<bool> {
        sctp_nodelay_internal(self.inner.as_fd())
    }

    /// Set the retransmission timeout parameters (`SCTP_RTOINFO`, Section 8.1.1 of RFC 6458) of
    /// the association `rto_info.assoc_id`, or with 0 the defaults of the socket.
    pub fn sctp_set_rto_info(&self, rto_info: RtoInfo) -> std::io::Result<()> {
        sctp_set_rto_info_internal(self.inner.as_fd(), rto_info)
    }

    /// Get the retransmission timeout parameters of the association `assoc_id`, or with 0 the
    /// defaults of the socket. See [`sctp_set_rto_info`][Self::sctp_set_rto_info].
    pub fn sctp_get_rto_info(&self, assoc_id: AssociationId) -> std::io::Result<RtoInfo> {
        sctp_get_rto_info_internal(self.inner.as_fd(), assoc_id)
    }

    /// Enables or disables `SO_REUSEADDR`, before [`bind`][Self::bind].
    ///
    /// With it, the socket can bind an address that sockets which also have it set are bound to,
    /// as long as none of them listens, e.g. while the associations of a previous instance of a
    /// server are still shutting down.
    pub fn set_reuseaddr(&self, reuseaddr: bool) -> std::io::Result<()> {
        set_reuseaddr_internal(self.inner.as_fd(), reuseaddr)
    }

    /// Whether `SO_REUSEADDR` is set. See [`set_reuseaddr`][Self::set_reuseaddr].
    pub fn reuseaddr(&self) -> std::io::Result<bool> {
        reuseaddr_internal(self.inner.as_fd())
    }
    /// Get heartbeat and retransmission settings for a peer path or socket defaults.
    pub fn peer_address_params(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<PeerAddressParams> {
        peer_address_params_internal(self.inner.as_fd(), assoc_id, address)
    }

    /// Set heartbeat and path retransmission settings, leaving other path settings unchanged.
    pub fn set_peer_address_params(&self, params: PeerAddressParams) -> std::io::Result<()> {
        set_peer_address_params_internal(self.inner.as_fd(), params)
    }

    /// Request an immediate heartbeat on the specified path.
    pub fn request_heartbeat(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<()> {
        request_heartbeat_internal(self.inner.as_fd(), assoc_id, address)
    }
}

/// The descriptor stays owned by the socket and must stay non-blocking. It allows setting socket
/// options that are not wrapped here, e.g. with `socket2::SockRef` or `libc::setsockopt`.
impl AsRawFd for Socket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Borrows the descriptor, under the same conditions as [`AsRawFd`].
impl AsFd for Socket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.get_ref().as_fd()
    }
}
