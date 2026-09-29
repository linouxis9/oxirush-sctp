//! Listening SCTP Socket

use std::net::SocketAddr;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use tokio::io::unix::AsyncFd;

#[allow(unused)]
use crate::internal::*;
use crate::{
    types::AssociationId, BindxFlags, ConnStatus, ConnectedSocket, Event, NotificationOrData,
    RtoInfo, SendData, SubscribeEventAssocId,
};

/// A structure representing a socket that is listening for incoming SCTP Connections.
///
/// This structure is created by an [`Socket`][crate::Socket] when it is bound to local address(es)
/// and is waiting for incoming connections by calling the `listen` on the socket. The original
/// [`Socket`][crate::Socket] is consumed when this structure is created. See
/// [`Socket::listen`][crate::Socket::listen] for more details.
pub struct Listener {
    inner: AsyncFd<OwnedFd>,
    // The part of a message `sctp_recv` has received so far.
    partial: Mutex<Option<PartialMessage>>,
    // The length of the longest message `sctp_recv` returns.
    max_message_size: AtomicUsize,
}

impl Listener {
    /// Accept on a given socket (valid only for `OneToOne` type sockets).
    pub async fn accept(&self) -> std::io::Result<(ConnectedSocket, SocketAddr)> {
        let (accepted, address) = accept_internal(&self.inner).await?;
        accepted.set_max_message_size(self.max_message_size());
        Ok((accepted, address))
    }

    /// Shutdown on the socket
    pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
        shutdown_internal(&self.inner, how)
    }

    /// Binds to one or more local addresses. See: Section 9.1 RFC 6458
    ///
    /// It is possible to call `sctp_bindx` on an already 'bound' (that is 'listen'ing socket.)
    pub fn sctp_bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sctp_bindx_internal(&self.inner, addrs, flags)
    }

    /// Peels off a connected SCTP association from the listening socket. See: Section 9.2 RFC 6458
    ///
    /// This call is successful only for UDP style one to many sockets. This is like
    /// `[Listener::accept`] where peeled off socket behaves like a stand alone
    /// one-to-one socket.
    ///
    /// The kernel moves the association's queued messages to the new socket, but not the part of
    /// a message this socket's [`sctp_recv`][Self::sctp_recv] has already received: peel off an
    /// association between its messages, e.g. on its `SCTP_COMM_UP` notification.
    pub fn sctp_peeloff(&self, assoc_id: AssociationId) -> std::io::Result<ConnectedSocket> {
        let peeled_off = sctp_peeloff_internal(&self.inner, assoc_id)?;
        peeled_off.set_max_message_size(self.max_message_size());
        Ok(peeled_off)
    }

    /// Get Peer Address(es) for the given Association ID. See: Section 9.3 RFC 6458
    ///
    /// This function is supported on the [`Listener`] because in the case of One to Many
    /// associations that are not peeled off, we are performing IO operations on the listening
    /// socket itself.
    pub fn sctp_getpaddrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sctp_getpaddrs_internal(&self.inner, assoc_id)
    }

    /// Get's the Local Addresses for the association. See: Section 9.4 RFC 6458
    pub fn sctp_getladdrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sctp_getladdrs_internal(&self.inner, assoc_id)
    }

    /// Receive Data or Notification from the listening socket.
    ///
    /// In the case of One-to-many sockets, it is possible to receive on the listening socket,
    /// without explicitly 'accept'ing or 'peeling off' the socket. The internal API used to
    /// receive the data is also the API used to receive notifications. This function returns
    /// either the notification (which the user should have subscribed for) or the data.
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
    ///
    /// When the delivery of a message is aborted (e.g. its association is aborted), the part
    /// received so far is dropped if a notification follows (subscribe to
    /// [`Event::PartialDelivery`] for one) or, with `RcvInfo` requested (see
    /// [`sctp_request_rcvinfo`][Self::sctp_request_rcvinfo]), a different message. Without
    /// `RcvInfo`, a changed sender address also discards the partial message. Request `RcvInfo`
    /// for multi-homed peers, where a continuation can arrive from another address.
    pub async fn sctp_recv(&self) -> std::io::Result<NotificationOrData> {
        sctp_recvmsg_internal(&self.inner, &self.partial, self.max_message_size()).await
    }

    /// Sets the length of the longest message [`sctp_recv`][Self::sctp_recv] returns, 4 MiB by
    /// default. A longer message is read and discarded, so that a peer cannot make the socket
    /// buffer without bound. Sockets accepted from this
    /// one or peeled off it start with its limit.
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
    pub async fn sctp_send(&self, to: SocketAddr, data: SendData) -> std::io::Result<()> {
        sctp_sendmsg_internal(&self.inner, Some(to), data).await
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
        sctp_subscribe_event_internal(&self.inner, event, assoc_id, true)
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
        sctp_subscribe_event_internal(&self.inner, event, assoc_id, false)
    }

    /// Subscribe to SCTP Events. See section 6.2.1 of RFC6458.
    ///
    /// SCTP allows receiving notifications about the changes to SCTP associations etc from the
    /// user space. For these notification events to be received, this API is used to subsribe for
    /// the events while receiving the data on the SCTP Socket.
    pub fn sctp_subscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        let mut failures = vec![];
        for ev in events {
            let result = sctp_subscribe_event_internal(&self.inner, ev.clone(), assoc_id, true);
            if result.is_err() {
                failures.push(result.err().unwrap());
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(std::io::Error::other(format!("{:?}", failures)))
        }
    }

    /// Unsubscribe from a given SCTP Event on the given socket. See section 6.2.1 of RFC6458.
    ///
    /// See [`sctp_subscribe_events`][`Self::sctp_subscribe_events`] for further details.
    pub fn sctp_unsubscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        let mut failures = vec![];
        for ev in events {
            let result = sctp_subscribe_event_internal(&self.inner, ev.clone(), assoc_id, false);
            if result.is_err() {
                failures.push(result.err().unwrap());
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(std::io::Error::other(format!("{:?}", failures)))
        }
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
        sctp_setup_init_params_internal(&self.inner, ostreams, istreams, retries, timeout)
    }

    /// Request to receive `RcvInfo` ancillary data.
    ///
    /// SCTP allows receiving ancillary data about the curent data received on the given socket.
    /// This API is used to obtain receive side additional info when the data is to be received.
    pub fn sctp_request_rcvinfo(&self, on: bool) -> std::io::Result<()> {
        request_rcvinfo_internal(&self.inner, on)
    }

    /// Request to receive `NxtInfo` ancillary data.
    ///
    /// SCTP allows receiving ancillary data about the curent data received on the given socket.
    /// This API is used to obtain information about the next datagram that will be received.
    pub fn sctp_request_nxtinfo(&self, on: bool) -> std::io::Result<()> {
        request_nxtinfo_internal(&self.inner, on)
    }

    /// Get the status of the connection associated with the association ID.
    pub fn sctp_get_status(&self, assoc_id: AssociationId) -> std::io::Result<ConnStatus> {
        sctp_get_status_internal(&self.inner, assoc_id)
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
        sctp_set_nodelay_internal(&self.inner, nodelay)
    }

    /// Whether `SCTP_NODELAY` is set. See [`set_nodelay`][Self::set_nodelay].
    pub fn nodelay(&self) -> std::io::Result<bool> {
        sctp_nodelay_internal(&self.inner)
    }

    /// Set the retransmission timeout parameters (`SCTP_RTOINFO`, Section 8.1.1 of RFC 6458) of
    /// the association `rto_info.assoc_id`, or with 0 the defaults of the socket.
    pub fn sctp_set_rto_info(&self, rto_info: RtoInfo) -> std::io::Result<()> {
        sctp_set_rto_info_internal(&self.inner, rto_info)
    }

    /// Get the retransmission timeout parameters of the association `assoc_id`, or with 0 the
    /// defaults of the socket. See [`sctp_set_rto_info`][Self::sctp_set_rto_info].
    pub fn sctp_get_rto_info(&self, assoc_id: AssociationId) -> std::io::Result<RtoInfo> {
        sctp_get_rto_info_internal(&self.inner, assoc_id)
    }

    // functions not part of public APIs
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
impl AsRawFd for Listener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Borrows the descriptor, under the same conditions as [`AsRawFd`].
impl AsFd for Listener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.get_ref().as_fd()
    }
}
