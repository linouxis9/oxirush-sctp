//! A Connected SCTP Socket. This is similar to `TCPStream`.

use tokio::io::unix::AsyncFd;

use std::net::SocketAddr;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[allow(unused)]
use crate::internal::*;
use crate::{
    AssociationId, BindxFlags, ConnStatus, Event, NotificationOrData, PeerAddressParams, RtoInfo,
    SendData, SendInfo, SendOptions, SubscribeEventAssocId,
};

/// A structure representing a Connected SCTP socket.
///
/// A Connected SCTP Socket is associated with one or more Peer associations (each of which is
/// identified by an Association ID). A Connected SCTP Socket will be created by an
/// [`Listener`][crate::Listener] when it calls an `accept` (in the case of One to One style
/// sockets) or upon receiving a `SCTP_COMM_UP` event in `SCTP_ASSOC_CHANGE` notification.
///
/// It is also possible to [`peeloff`][crate::Listener::sctp_peeloff] a socket from One to Many
/// listening socket and the peeled socket is an [`ConnectedSocket`].
#[derive(Debug)]
pub struct ConnectedSocket {
    inner: AsyncFd<OwnedFd>,
    // The part of a message `sctp_recv` has received so far.
    partial: Mutex<Option<PartialMessage>>,
    // The length of the longest message `sctp_recv` returns.
    max_message_size: AtomicUsize,
}

impl ConnectedSocket {
    /// Creates new [`ConnectedSocket`] from a [`RawFd`][std::os::unix::io::RawFd].
    ///
    /// TODO: Remove this from Public API
    /// Although, this is available as public API as of now, likely the users are not required to
    /// use this. Mostly [`accept`][`crate::Listener::accept`] (in the case of One to One
    /// Socket to Association) or [`peeloff`][`crate::Listener::sctp_peeloff`] (in the case of
    /// One to Many Association) would use this API to create new [`ConnectedSocket`].
    ///
    /// The [`ConnectedSocket`] takes ownership of `rawfd` and closes it when dropped, also when
    /// this function fails after checking that `rawfd` is open.
    ///
    /// # Safety
    ///
    /// `rawfd` must be a non-blocking SCTP socket that nothing else owns: nothing else may close
    /// it or use it after this call, as with [`OwnedFd::from_raw_fd`][std::os::fd::FromRawFd].
    pub unsafe fn from_rawfd(rawfd: RawFd) -> std::io::Result<Self> {
        Self::from_owned_fd(owned_fd_from_raw(rawfd)?)
    }

    /// Perform a TCP like half close.
    ///
    /// Note: however that the semantics for TCP and SCTP half close are different. See section
    /// 4.1.7 of RFC 6458 for details.
    pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
        shutdown_internal(self.inner.as_fd(), how)
    }

    /// Bind to addresses on the given socket. See Section 9.1 RFC 6458.
    ///
    /// Add or remove local addresses on an established association. Kernel ASCONF policy
    /// determines whether the change is also advertised to the peer.
    pub fn sctp_bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sctp_bindx_internal(self.inner.as_fd(), addrs, flags)
    }

    /// Get Peer addresses for the association. See Section 9.3 RFC 6458.
    pub fn sctp_getpaddrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sctp_getpaddrs_internal(self.inner.as_fd(), assoc_id)
    }

    /// Get Local addresses for the association. See section 9.5 RFC 6458.
    pub fn sctp_getladdrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sctp_getladdrs_internal(self.inner.as_fd(), assoc_id)
    }

    /// Receive Data or Notification from the connected socket.
    ///
    /// The internal API used to receive the data is also the API used to receive notifications.
    /// This function returns either the notification (which the user should have subscribed for)
    /// or the data. Data with an empty payload means the peer has shut the association down.
    ///
    /// Each call returns a whole message (or notification), however many reads it takes: the
    /// kernel returns a message longer than the read buffer, or than the receive window, in
    /// several parts. A message longer than [`max_message_size`][Self::max_message_size] (4 MiB
    /// by default) is discarded and reported as an
    /// [`InvalidData`][std::io::ErrorKind::InvalidData] error, and the next call returns the next
    /// message. The `RcvInfo` is that of the first part of the message and the `NxtInfo` that of
    /// the last. This assumes the default `SCTP_FRAGMENT_INTERLEAVE` level 0, where the parts of
    /// a message are not interleaved with other messages.
    ///
    /// Cancel safe: if the returned future is dropped before it completes, the part of a message
    /// received so far is kept for the next call.
    pub async fn sctp_recv(&self) -> std::io::Result<NotificationOrData> {
        sctp_recvmsg_internal(&self.inner, &self.partial, self.max_message_size()).await
    }

    /// Sets the length of the longest message [`sctp_recv`][Self::sctp_recv] returns, 4 MiB by
    /// default. A longer message is read and discarded, so that a peer cannot make the socket
    /// buffer without bound.
    pub fn set_max_message_size(&self, octets: usize) {
        self.max_message_size.store(octets, Ordering::Relaxed);
    }

    /// The length of the longest message [`sctp_recv`][Self::sctp_recv] returns.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size.load(Ordering::Relaxed)
    }

    /// Send Data and Anciliary data if any on the SCTP Socket.
    ///
    /// SCTP supports sending the actual SCTP message together with sending any anciliary data on
    /// the SCTP association. The anciliary data is optional.
    ///
    /// Waits while the send buffer is full. The message is sent whole, or not at all if the
    /// returned future is dropped before it completes.
    pub async fn sctp_send(&self, data: SendData) -> std::io::Result<()> {
        sctp_sendmsg_internal(&self.inner, None, &data.payload, data.snd_info.as_ref()).await
    }

    /// Send a borrowed message. PPID in `options` is in host byte order.
    ///
    /// Waits for send-buffer capacity without taking ownership of the payload. Dropping the
    /// future before completion sends no part of the message, as with [`sctp_send`][Self::sctp_send].
    pub async fn send(&self, payload: &[u8], options: SendOptions) -> std::io::Result<()> {
        let info = options.wire_info();
        sctp_sendmsg_internal(&self.inner, None, payload, Some(&info)).await
    }

    /// Initiate another outgoing association on a one-to-many socket without consuming it.
    ///
    /// Returns the association ID after initiation. Subscribe to [`Event::Association`] to
    /// learn whether establishment succeeds. One-to-one sockets return `EOPNOTSUPP`.
    /// The returned ID can be used with [`SendOptions::assoc_id`].
    pub fn sctp_connectx_association(
        &self,
        addrs: &[SocketAddr],
    ) -> std::io::Result<AssociationId> {
        connect_association(self.inner.as_fd(), addrs)
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

    /// Set Default `SendInfo` values for this socket.
    ///
    /// In the [`sctp_send`][Self::sctp_send] API, an optional `SendInfo` is present, which can be
    /// used to specify the ancillary data along with the payload. Instead, a sender can chose to
    /// use this API to set the default `SendInfo` to be used while sending the data for this
    /// 'connected' socket.
    /// Note: This API is provided only for the [`ConnectedSocket`].
    pub fn sctp_set_default_sendinfo(&self, sendinfo: SendInfo) -> std::io::Result<()> {
        sctp_set_default_sendinfo_internal(self.inner.as_fd(), sendinfo)
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

    /// Sets `SO_LINGER` (Section 8.1.4 of RFC 6458): how closing (dropping) the socket ends its
    /// association. With `Some(Duration::ZERO)` the close aborts it (the ABORT primitive, with
    /// an ABORT chunk to the peer) instead of shutting it down gracefully, as with `None`, the
    /// default. Positive durations return [`InvalidInput`][std::io::ErrorKind::InvalidInput]:
    /// Linux can block even a non-blocking socket's close for that duration, stalling the async
    /// runtime when the socket is dropped.
    pub fn set_linger(&self, linger: Option<std::time::Duration>) -> std::io::Result<()> {
        set_linger_internal(self.inner.as_fd(), linger)
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

    /// Get heartbeat and retransmission settings for a peer path.
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

    // functions not part of public APIs
    pub(crate) fn from_owned_fd(fd: OwnedFd) -> std::io::Result<Self> {
        Ok(Self::from_async_fd(AsyncFd::new(fd)?))
    }

    pub(crate) fn from_async_fd(inner: AsyncFd<OwnedFd>) -> Self {
        Self {
            inner,
            partial: Mutex::new(None),
            max_message_size: AtomicUsize::new(DEFAULT_MAX_MESSAGE_SIZE),
        }
    }
}

/// The descriptor stays owned by the socket and must stay non-blocking. It allows setting socket
/// options that are not wrapped here, e.g. with `socket2::SockRef` or `libc::setsockopt`.
impl AsRawFd for ConnectedSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Borrows the descriptor, under the same conditions as [`AsRawFd`].
impl AsFd for ConnectedSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.get_ref().as_fd()
    }
}
