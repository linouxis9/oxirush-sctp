//! A socket owning one SCTP association.
use crate::internal::{SocketCore, sys};
use crate::{
    BindxFlags, ConnStatus, NotificationOrData, PeerAddress, ReceivedData, ReceivedInto, SendData,
    SendOptions, SocketOptions,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::task::{Context, Poll};

/// One association established by [`Socket::connect`][crate::Socket::connect],
/// accepted by a [`Listener`][crate::Listener], or peeled off a
/// [`OneToManyEndpoint`][crate::OneToManyEndpoint].
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
/// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// # socket.bind("127.0.0.1:0".parse().unwrap())?;
/// # let listener = socket.listen(5)?;
/// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// # let (association, _) = client.connect(listener.local_addr()?).await?;
/// # let (peer, _) = listener.accept().await?;
///
/// let options = SendOptions::new().stream(1).ppid(60);
/// association.send(b"request", options).await?;
/// match peer.recv().await? {
///     None => println!("closed"),
///     Some(NotificationOrData::Data(message)) => {
///         assert_eq!((message.stream_id(), message.ppid()), (Some(1), Some(60)));
///     }
///     Some(other) => println!("{other:?}"),
/// }
/// # Ok(())
/// # }
/// ```
pub struct ConnectedSocket {
    core: SocketCore,
}

/// The local address, the first address of the peer and the descriptor.
impl std::fmt::Debug for ConnectedSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectedSocket")
            .field("local", &self.local_addr().ok())
            .field(
                "peer",
                &self
                    .peer_addrs()
                    .ok()
                    .and_then(|peer| peer.into_iter().next()),
            )
            .field("fd", &self.as_raw_fd())
            .finish()
    }
}

impl ConnectedSocket {
    /// Take ownership of an existing established nonblocking `SOCK_STREAM` SCTP descriptor.
    /// Raw Linux peeled-off `SOCK_SEQPACKET` descriptors are not accepted by this import;
    /// use [`OneToManyEndpoint::peeloff`][crate::OneToManyEndpoint::peeloff] to obtain those.
    ///
    /// A valid descriptor is closed on failure, including rejection of a one-to-many socket.
    /// Its options stay as they are: receive metadata is off unless the descriptor has it on.
    ///
    /// # Safety
    ///
    /// `rawfd` must be an established nonblocking SCTP socket that nobody else owns. Nothing
    /// else may close or use it after this call, as with `OwnedFd::from_raw_fd`.
    pub unsafe fn from_rawfd(rawfd: RawFd) -> std::io::Result<Self> {
        let fd = sys::owned_fd_from_raw(rawfd)?;
        if sys::socket_type(fd.as_fd())? != libc::SOCK_STREAM {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "raw import requires a SOCK_STREAM SCTP association",
            ));
        }
        let core = SocketCore::import(fd)?;
        Ok(Self { core })
    }

    /// Borrow shared socket configuration.
    pub fn options(&self) -> SocketOptions<'_> {
        SocketOptions { core: &self.core }
    }

    /// Initiate shutdown. SCTP shutdown is association-wide and does not provide TCP half-close
    /// semantics; see RFC 6458 section 4.1.7.
    ///
    /// After `Shutdown::Write`, [`recv`][Self::recv] returns what the peer had still sent, then
    /// the empty message that ends an association, once the peer has answered the shutdown.
    /// Linux says so in an association change notification alone, which this call subscribes
    /// to for `recv`. `recv` passes these notifications on only if
    /// [`Event::Association`][crate::Event::Association] is subscribed through
    /// [`options`][Self::options].
    pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
        self.core.shutdown(how)
    }

    /// The local address as `getsockname` reports it, with the port the kernel chose for port 0:
    /// one of several bound addresses, or the wildcard address when bound to it or not bound.
    /// [`local_addrs`][Self::local_addrs] lists them all.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        sys::local_addr(self.as_fd())
    }

    /// Add or remove local addresses. Kernel ASCONF policy governs advertising changes.
    pub fn bindx(&self, addrs: &[SocketAddr], flags: BindxFlags) -> std::io::Result<()> {
        sys::sctp_bindx_internal(self.as_fd(), addrs, flags)
    }

    /// The addresses of the peer.
    pub fn peer_addrs(&self) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getpaddrs_internal(self.as_fd(), 0)
    }

    /// The local addresses of the association.
    pub fn local_addrs(&self) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getladdrs_internal(self.as_fd(), 0)
    }

    /// The status of the association.
    pub fn status(&self) -> std::io::Result<ConnStatus> {
        sys::sctp_get_status_internal(self.as_fd(), 0)
    }

    /// Each path to the peer, one an address of the peer, with its state, its congestion
    /// window, its smoothed round trip, its retransmission timeout and its MTU
    /// (`SCTP_GET_PEER_ADDR_INFO`). [`status`][Self::status] has the primary path alone.
    pub fn paths(&self) -> std::io::Result<Vec<PeerAddress>> {
        self.peer_addrs()?
            .into_iter()
            .map(|address| sys::peer_address_info_internal(self.as_fd(), 0, address))
            .collect()
    }

    /// Receive the next message or notification. `None` is the end of an association that
    /// was shut down, by the peer or by [`shutdown`][Self::shutdown] on this side: a loop
    /// `while let Some(received) = association.recv().await?` ends there. An association
    /// that was aborted or lost is an error such as `ConnectionReset`. Either end stays:
    /// every later call returns it again, and none waits. A [`send`][Self::send] that learns
    /// of the end first records it for this call, which then returns what the kernel still
    /// holds of the association, and the end after it.
    ///
    /// Records split across reads are assembled until `MSG_EOR`. Cancellation retains the
    /// partial record for the next call; concurrent readers share one assembler. Records over
    /// `options().max_message_size()` are drained and reported as `InvalidData`, leaving the
    /// next record intact; a notification is never discarded.
    pub async fn recv(&self) -> std::io::Result<Option<NotificationOrData>> {
        self.core.recv().await
    }

    /// [`recv`][Self::recv] as a poll, for a `Stream` or a future of one's own: `Pending`
    /// until the socket is readable, and the waker of `cx` is woken then. It ends and fails
    /// as `recv` does: the crate subscribes to the association changes of a one-to-one socket
    /// for itself, so that the end of the association makes it readable, and passes them on
    /// only if [`Event::Association`][crate::Event::Association] is subscribed through
    /// [`options`][Self::options].
    pub fn poll_recv(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<Option<NotificationOrData>>> {
        self.core.poll_recv(cx)
    }

    /// [`recv_into`][Self::recv_into] as a poll, as [`poll_recv`][Self::poll_recv] is.
    pub fn poll_recv_into(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<std::io::Result<Option<ReceivedInto>>> {
        self.core.poll_recv_into(cx, buffer)
    }

    /// [`send`][Self::send] as a poll: `Pending` until the send buffer has room, and the
    /// waker of `cx` is woken then. A message is sent whole or not at all.
    pub fn poll_send(
        &self,
        cx: &mut Context<'_>,
        payload: &[u8],
        options: SendOptions,
    ) -> Poll<std::io::Result<()>> {
        if options.assoc_id() != 0 {
            return Poll::Ready(Err(Self::not_one_to_many()));
        }
        let info = options.info();
        self.core.poll_send(cx, None, payload, Some(&info))
    }

    fn not_one_to_many() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a connected socket has one association: `SendOptions::association` is for a \
             one-to-many endpoint",
        )
    }

    /// Send a message, waiting for room in the send buffer. Dropping the future before
    /// completion sends no part of it.
    ///
    /// An empty payload, a stream the association does not have ([`ConnStatus::outstreams`]),
    /// [`SendOptions::association`] with another value than 0 and a message longer than the
    /// send buffer (`SO_SNDBUF`) are `InvalidInput`, with a text that says which. After the
    /// end of the association, every call returns at once: `BrokenPipe` for an association
    /// that was shut down, and the error of one that was aborted or lost, such as
    /// `ConnectionReset`.
    pub async fn send(&self, payload: &[u8], options: SendOptions) -> std::io::Result<()> {
        if options.assoc_id() != 0 {
            return Err(Self::not_one_to_many());
        }
        let info = options.info();
        self.core.send(None, payload, Some(&info)).await
    }

    /// Send an owned message with optional low-level `SendInfo`, whose flags give the
    /// controls of the kernel that `SendOptions` does not have.
    pub async fn send_data(&self, data: SendData) -> std::io::Result<()> {
        self.core
            .send(None, &data.payload, data.snd_info.as_ref())
            .await
    }

    /// Receive the next message into `buffer`, without an allocation when one read gives it
    /// whole, which is the usual case. It ends and fails as [`recv`][Self::recv] does. A
    /// message longer than the buffer is discarded and reported as `InvalidData`: give a
    /// buffer as long as the longest message that the peer may send.
    pub async fn recv_into(&self, buffer: &mut [u8]) -> std::io::Result<Option<ReceivedInto>> {
        self.core.recv_into(buffer).await
    }

    /// Close the association without losing what was sent: shut it down for writing, then
    /// read until the peer has answered, for `deadline` at most. The messages that the peer
    /// had still sent are returned.
    ///
    /// An association that the peer aborts meanwhile is that error, and a peer that has not
    /// answered in time is `TimedOut`: the socket is then dropped as it is. A dropped
    /// socket is closed by Linux, which shuts its association down in the background when
    /// everything that it received was read, and aborts it when a message or a notification
    /// was not: what this side had sent and the peer had not acknowledged is then lost.
    ///
    /// The deadline needs the time driver of the runtime (`enable_time`, or
    /// `#[tokio::main]`).
    pub async fn close(self, deadline: std::time::Duration) -> std::io::Result<Vec<ReceivedData>> {
        match self.core.shutdown(std::net::Shutdown::Write) {
            // An association that has ended already: `recv` says how
            Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {}
            other => other?,
        }
        let mut unread = Vec::new();
        let ended = tokio::time::timeout(deadline, async {
            loop {
                match self.core.recv().await? {
                    None => return std::io::Result::Ok(()),
                    Some(NotificationOrData::Data(message)) => unread.push(message),
                    Some(_) => {}
                }
            }
        })
        .await;
        match ended {
            Ok(result) => result.map(|()| unread),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("the peer has not answered the shutdown in {deadline:?}"),
            )),
        }
    }

    /// Abort the association: the peer learns of it at once, and what was not delivered is
    /// lost on both sides.
    ///
    /// `reason` goes to the peer in the ABORT, as the reason of a user-initiated abort, on a
    /// socket that was peeled off an endpoint. Linux refuses to carry one on a socket that
    /// was connected or accepted: the association is then aborted without it.
    pub fn abort(self, reason: &[u8]) -> std::io::Result<()> {
        let info = crate::SendInfo {
            flags: crate::consts::SCTP_ABORT,
            ..Default::default()
        };
        match sys::sctp_sendmsg_once(self.as_fd(), None, reason, Some(&info)) {
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
                // Closing a socket with a linger of zero aborts its association
                sys::set_linger_internal(self.as_fd(), Some(std::time::Duration::ZERO))
            }
            result => result,
        }
    }

    pub(crate) fn from_core(core: SocketCore) -> Self {
        Self { core }
    }
}

/// The owned descriptor must remain nonblocking; additional options can be set through it.
impl AsRawFd for ConnectedSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.core.as_raw_fd()
    }
}
impl AsFd for ConnectedSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.core.as_fd()
    }
}
