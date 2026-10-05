//! A socket owning one SCTP association.
use crate::internal::{sys, SocketCore};
use crate::{
    AssociationId, BindxFlags, ConnStatus, NotificationOrData, SendData, SendOptions, SocketOptions,
};
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

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
/// let options = SendOptions {
///     stream_id: 1,
///     ppid: 60,
///     ..Default::default()
/// };
/// association.send(b"request", options).await?;
/// match peer.recv().await? {
///     NotificationOrData::Data(message) if message.payload.is_empty() => println!("closed"),
///     NotificationOrData::Data(message) => {
///         assert_eq!((message.stream_id(), message.ppid()), (Some(1), Some(60)));
///     }
///     NotificationOrData::Notification(notification) => println!("{notification:?}"),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ConnectedSocket {
    core: SocketCore,
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
        let core = SocketCore::from_association(fd)?;
        Ok(Self { core })
    }

    /// Borrow shared socket configuration.
    pub fn options(&self) -> SocketOptions<'_> {
        SocketOptions { core: &self.core }
    }

    /// Initiate shutdown. SCTP shutdown is association-wide and does not provide TCP half-close
    /// semantics; see RFC 6458 section 4.1.7.
    ///
    /// Linux reports the end of a shutdown that this socket started with `Shutdown::Write` as a
    /// `ShutdownComplete` association notification only: without
    /// [`Event::Association`][crate::Event::Association], `recv` then waits forever.
    pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
        sys::shutdown_internal(self.as_fd(), how)
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

    /// Query peer addresses. Linux ignores `assoc_id` for this one-to-one socket.
    pub fn peer_addrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getpaddrs_internal(self.as_fd(), assoc_id)
    }

    /// Query local addresses. Linux ignores `assoc_id` for this one-to-one socket.
    pub fn local_addrs(&self, assoc_id: AssociationId) -> std::io::Result<Vec<SocketAddr>> {
        sys::sctp_getladdrs_internal(self.as_fd(), assoc_id)
    }

    /// Query this association's status. Linux ignores `assoc_id` for a one-to-one socket.
    pub fn status(&self, assoc_id: AssociationId) -> std::io::Result<ConnStatus> {
        sys::sctp_get_status_internal(self.as_fd(), assoc_id)
    }

    /// Receive a complete record or notification. Empty data indicates peer shutdown, and
    /// every later call returns it again. An aborted or lost association is an error such as
    /// `ConnectionReset`, after which nothing more arrives.
    ///
    /// Records split across reads are assembled until `MSG_EOR`. Cancellation retains the
    /// partial record for the next call; concurrent readers share one assembler. Records over
    /// `options().max_message_size()` are drained and reported as `InvalidData`, leaving the
    /// next record intact. Assumes `SCTP_FRAGMENT_INTERLEAVE` level zero.
    pub async fn recv(&self) -> std::io::Result<NotificationOrData> {
        self.core.recv().await
    }

    /// Send a borrowed complete record, waiting for capacity. PPID is in host byte order.
    /// Dropping the future before completion sends no part of this record.
    ///
    /// An empty payload and a stream the association does not have
    /// ([`ConnStatus::outstreams`]) are `EINVAL`, a record longer than the send buffer
    /// (`SO_SNDBUF`) is `EMSGSIZE`, and an association that has ended is `EPIPE`.
    pub async fn send(&self, payload: &[u8], options: SendOptions) -> std::io::Result<()> {
        let info = options.wire_info();
        self.core.send(None, payload, Some(&info)).await
    }

    /// Send an owned record with optional low-level `SendInfo`. Its PPID remains in wire byte
    /// order, and flags support kernel controls unavailable through `SendOptions`.
    pub async fn send_data(&self, data: SendData) -> std::io::Result<()> {
        self.core
            .send(None, &data.payload, data.snd_info.as_ref())
            .await
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
