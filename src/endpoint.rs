//! One-to-many SCTP association endpoint.
use crate::internal::{SocketCore, sys};
use crate::{
    AssociationId, BindxFlags, ConnStatus, ConnectedSocket, NotificationOrData, ReceivedInto,
    SendData, SendOptions, SocketOptions,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::task::{Context, Poll};

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
///     let options = SendOptions::new().association(message.assoc_id().unwrap());
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
pub struct OneToManyEndpoint {
    core: SocketCore,
}

/// The local address and the descriptor.
impl std::fmt::Debug for OneToManyEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneToManyEndpoint")
            .field("local", &self.local_addr().ok())
            .field("fd", &self.as_raw_fd())
            .finish()
    }
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
    /// The parts of the messages of several associations come in turns
    /// (`SCTP_FRAGMENT_INTERLEAVE` 1), and each is assembled apart until its `MSG_EOR`: a
    /// long message of one association holds back none of the others. The parts of a
    /// message whose delivery the kernel gives up go with the notification of it, which the
    /// endpoint subscribes to for itself and passes on only if
    /// [`Event::PartialDelivery`][crate::Event::PartialDelivery] is subscribed through
    /// [`options`][Self::options]. Records over `options().max_message_size()` are drained
    /// before reporting `InvalidData`.
    pub async fn recv(&self) -> std::io::Result<NotificationOrData> {
        // A one-to-many socket has no end of stream: its associations end in notifications
        self.core.recv().await?.ok_or_else(shut_down)
    }

    /// [`recv`][Self::recv] as a poll, for a `Stream` or a future of one's own: `Pending`
    /// until the socket is readable, and the waker of `cx` is woken then.
    pub fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<std::io::Result<NotificationOrData>> {
        self.core
            .poll_recv(cx)
            .map(|received| received?.ok_or_else(shut_down))
    }

    /// [`recv_into`][Self::recv_into] as a poll, as [`poll_recv`][Self::poll_recv] is.
    pub fn poll_recv_into(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<std::io::Result<ReceivedInto>> {
        self.core
            .poll_recv_into(cx, buffer)
            .map(|received| received?.ok_or_else(shut_down))
    }

    /// [`send`][Self::send] as a poll: `Pending` until the send buffer has room, and the
    /// waker of `cx` is woken then.
    pub fn poll_send(
        &self,
        cx: &mut Context<'_>,
        payload: &[u8],
        options: SendOptions,
    ) -> Poll<std::io::Result<()>> {
        if options.assoc_id() == 0 {
            return Poll::Ready(Err(no_association()));
        }
        let info = options.info();
        self.core.poll_send(cx, None, payload, Some(&info))
    }

    /// [`send_to`][Self::send_to] as a poll, as [`poll_send`][Self::poll_send] is.
    pub fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        address: SocketAddr,
        payload: &[u8],
        options: SendOptions,
    ) -> Poll<std::io::Result<()>> {
        let info = options.info();
        self.core.poll_send(cx, Some(address), payload, Some(&info))
    }

    /// Send a complete borrowed record to `options.assoc_id`. A zero association ID without a
    /// destination is rejected. PPID is in host byte order; waits for send-buffer capacity.
    pub async fn send(&self, payload: &[u8], options: SendOptions) -> std::io::Result<()> {
        if options.assoc_id() == 0 {
            return Err(no_association());
        }
        let info = options.info();
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
        let info = options.info();
        self.core.send(Some(address), payload, Some(&info)).await
    }

    /// Send several messages, each to the association of its options, in a few system calls:
    /// a prefix of `messages`, which is all of them unless the send buffer filled, after
    /// waiting for room for the first. The number sent. Each needs an association, as
    /// [`send`][Self::send] does. The messages of an association that follow one another in
    /// `messages` are bundled, as
    /// [`ConnectedSocket::send_many`][crate::ConnectedSocket::send_many] has it.
    pub async fn send_many(&self, messages: &[(&[u8], SendOptions)]) -> std::io::Result<usize> {
        with_associations(messages)?;
        self.core.send_many(messages).await
    }

    /// [`send_many`][Self::send_many] as a poll.
    pub fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        messages: &[(&[u8], SendOptions)],
    ) -> Poll<std::io::Result<usize>> {
        match with_associations(messages) {
            Ok(()) => self.core.poll_send_many(cx, messages),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// Receive the messages and notifications that the kernel holds of every association,
    /// up to `limit`, in one system call, and append them to `out`, waiting for the first:
    /// how many were appended. They are whole, as [`recv`][Self::recv] returns them.
    pub async fn recv_many(
        &self,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> std::io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_recv_many(cx, out, limit)).await
    }

    /// [`recv_many`][Self::recv_many] as a poll.
    pub fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> Poll<std::io::Result<usize>> {
        match self.core.poll_recv_many(cx, out, limit) {
            // A one-to-many socket has no end of stream: its associations end in
            // notifications
            Poll::Ready(Ok(0)) if limit > 0 => Poll::Ready(Err(shut_down())),
            other => other,
        }
    }

    /// Send an owned record with wire-order low-level ancillary data. `to` may be `None` when
    /// `data.snd_info` selects an association (including SCTP EOF/ABORT controls).
    pub async fn send_data(&self, to: Option<SocketAddr>, data: SendData) -> std::io::Result<()> {
        self.core
            .send(to, &data.payload, data.snd_info.as_ref())
            .await
    }

    /// Receive the next message or notification into `buffer`, without an allocation when
    /// one read gives a message whole. A message longer than the buffer is discarded and
    /// reported as `InvalidData`.
    pub async fn recv_into(&self, buffer: &mut [u8]) -> std::io::Result<ReceivedInto> {
        self.core.recv_into(buffer).await?.ok_or_else(shut_down)
    }

    /// Abort one association: its peer learns of it at once, with `reason` as the reason of
    /// a user-initiated abort, and what was not delivered is lost on both sides.
    pub fn abort(&self, assoc_id: AssociationId, reason: &[u8]) -> std::io::Result<()> {
        let info = crate::SendInfo {
            flags: crate::consts::SCTP_ABORT,
            assoc_id,
            ..Default::default()
        };
        sys::sctp_sendmsg_once(self.as_fd(), None, reason, Some(&info))
    }

    /// Move an association and its queued messages into a separate one-to-one socket.
    ///
    /// A partially delivered record already belongs to this endpoint, not to the kernel.
    /// Peeling that association during partial delivery returns `InvalidInput`; finish or drain
    /// its record first. The returned association inherits this endpoint's receive limit.
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
    ///
    /// let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    /// socket.bind("127.0.0.1:0".parse().unwrap())?;
    /// let endpoint = socket.into_endpoint(16)?;
    /// let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// let (peer, _) = peer.connect(endpoint.local_addr()?).await?;
    /// peer.send(b"ping", SendOptions::new()).await?;
    ///
    /// // The association of the first message gets a socket of its own
    /// let NotificationOrData::Data(message) = endpoint.recv().await? else { unreachable!() };
    /// let association = endpoint.peeloff(message.assoc_id().unwrap())?;
    /// association.send(b"pong", SendOptions::new()).await?;
    /// let Some(NotificationOrData::Data(answer)) = peer.recv().await? else { unreachable!() };
    /// assert_eq!(answer.payload, b"pong");
    /// # Ok(())
    /// # }
    /// ```
    pub fn peeloff(&self, assoc_id: AssociationId) -> std::io::Result<ConnectedSocket> {
        Ok(ConnectedSocket::from_core(self.core.peeloff(assoc_id)?))
    }

    /// The local address as `getsockname` reports it, with the port the kernel chose for port 0:
    /// one of several bound addresses, or the wildcard address when bound to it or not bound.
    /// [`local_addrs`][Self::local_addrs] lists them all.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        sys::local_addr(self.as_fd())
    }

    /// Add or remove local addresses across associations, subject to kernel ASCONF policy.
    pub fn bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sys::sctp_bindx_internal(self.as_fd(), addrs, flags)
    }

    /// Query an association's peer addresses.
    pub fn peer_addrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getpaddrs_internal(self.as_fd(), assoc_id)
    }

    /// Query association local addresses; zero selects the endpoint's bound addresses.
    pub fn local_addrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getladdrs_internal(self.as_fd(), assoc_id)
    }

    /// Query one association's connection status.
    pub fn status(&self, assoc_id: AssociationId) -> std::io::Result<ConnStatus> {
        sys::sctp_get_status_internal(self.as_fd(), assoc_id)
    }

    /// Each path of association `assoc_id`, as
    /// [`ConnectedSocket::paths`][crate::ConnectedSocket::paths] reads them.
    pub fn paths(&self, assoc_id: AssociationId) -> std::io::Result<Vec<crate::PeerAddress>> {
        self.peer_addrs(assoc_id)?
            .into_iter()
            .map(|address| sys::peer_address_info_internal(self.as_fd(), assoc_id, address))
            .collect()
    }

    pub(crate) fn from_core(core: SocketCore) -> Self {
        Self { core }
    }
}

// What a receive of a one-to-many socket that was shut down for reading returns.
fn shut_down() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "the socket was shut down for reading",
    )
}

// The messages of a batch, each with its association.
// The options of each message of a batch of an endpoint name its association.
fn with_associations(messages: &[(&[u8], SendOptions)]) -> std::io::Result<()> {
    if messages.iter().any(|(_, options)| options.assoc_id() == 0) {
        return Err(no_association());
    }
    Ok(())
}

// What a send without an association returns.
fn no_association() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "a destination association is required",
    )
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
