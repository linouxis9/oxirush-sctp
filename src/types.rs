//! Types used by the Public APIs

/// Octets as `Debug` prints them: their number, then each in hexadecimal.
pub(crate) struct Octets<'a>(pub(crate) &'a [u8]);

impl std::fmt::Debug for Octets<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} octets ", self.0.len())?;
        for octet in self.0 {
            write!(f, "{octet:02x}")?;
        }
        Ok(())
    }
}

/// SCTP Association ID Type
pub type AssociationId = i32;

/// Flags of `bindx`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BindxFlags {
    /// Add the addresses passed (corresponding to `SCTP_BINDX_ADD_ADDR`)
    Add,

    /// Remove the addresses passed (corresponding to `SCTP_BINDX_REM_ADDR`)
    Remove,
}

/// SocketToAssociation: One-to-Many or One-to-One style Socket
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SocketToAssociation {
    /// One Association per Socket (TCP Style Socket.)
    OneToOne,

    /// Many Associations per Socket (UDP Style Socket.)
    OneToMany,
}

/// What a `recv` call returns: a message or a notification.
///
/// The end of an association is neither:
/// [`ConnectedSocket::recv`][crate::ConnectedSocket::recv] returns `None` for an association
/// that was shut down, and an error for one that was aborted or lost.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use oxirush_sctp::{Event, Notification, NotificationOrData, SendOptions, Socket};
/// use oxirush_sctp::{SocketToAssociation, SubscribeEventAssocId};
///
/// // A socket that subscribes to the changes of its associations
/// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// socket.bind("127.0.0.1:0".parse()?)?;
/// socket.options().subscribe_events(&[Event::Association], SubscribeEventAssocId::All)?;
/// let listener = socket.listen(5)?;
///
/// let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let (peer, _) = peer.connect(listener.local_addr()?).await?;
/// peer.send(b"hello", SendOptions::new()).await?;
/// let (association, _) = listener.accept().await?;
///
/// // The association comes up, then the message
/// for _ in 0..2 {
///     match association.recv().await? {
///         Some(NotificationOrData::Notification(Notification::AssociationChange(change))) => {
///             println!("{:?}", change.state)
///         }
///         Some(NotificationOrData::Data(message)) => println!("{} octets", message.payload.len()),
///         other => println!("{other:?}"),
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NotificationOrData {
    /// SCTP Notification received by a `recv` call.
    Notification(Notification),

    /// SCTP Data Received by a `recv` call.
    Data(ReceivedData),
}

/// A message that `recv` returned: its payload, its sender and the ancillary data of the
/// system call that received it. `Debug` prints the payload as its length and its octets in
/// hexadecimal.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
///
/// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// socket.bind("127.0.0.1:0".parse()?)?;
/// let listener = socket.listen(5)?;
/// let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let (peer, _) = peer.connect(listener.local_addr()?).await?;
/// let (association, _) = listener.accept().await?;
///
/// // A message on stream 0, with the payload protocol identifier of NGAP
/// peer.send(b"NG SETUP REQUEST", SendOptions::new().stream(0).ppid(60)).await?;
/// if let Some(NotificationOrData::Data(message)) = association.recv().await? {
///     assert_eq!(message.payload, b"NG SETUP REQUEST");
///     assert_eq!((message.stream_id(), message.ppid()), (Some(0), Some(60)));
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReceivedData {
    /// Received Message Payload.
    pub payload: Vec<u8>,

    /// Address of the peer that sent the message, which tells the associations of a One-to-many
    /// socket apart. `None` when the kernel gives no address.
    pub from: Option<std::net::SocketAddr>,

    /// Ancillary information about the received payload. `None` when
    /// [`request_rcvinfo`][crate::SocketOptions::request_rcvinfo] turned it off.
    pub rcv_info: Option<RcvInfo>,

    /// Optional ancillary information about the next call to `recv`.
    pub nxt_info: Option<NxtInfo>,
}

impl std::fmt::Debug for ReceivedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceivedData")
            .field("payload", &Octets(&self.payload))
            .field("from", &self.from)
            .field("rcv_info", &self.rcv_info)
            .field("nxt_info", &self.nxt_info)
            .finish()
    }
}

impl ReceivedData {
    /// Stream the message arrived on. `None` without receive information.
    pub fn stream_id(&self) -> Option<u16> {
        self.rcv_info.as_ref().map(|info| info.sid)
    }

    /// Payload protocol identifier, as [`SendOptions::ppid`] takes it: 60 for NGAP. `None`
    /// without receive information.
    pub fn ppid(&self) -> Option<u32> {
        self.rcv_info.as_ref().map(|info| info.ppid)
    }

    /// Association the message arrived on, which [`SendOptions::association`] takes to answer
    /// on a one-to-many socket. `None` without receive information.
    pub fn assoc_id(&self) -> Option<AssociationId> {
        self.rcv_info.as_ref().map(|info| info.assoc_id)
    }
}

/// A message received into a buffer of the caller, by
/// [`ConnectedSocket::recv_into`][crate::ConnectedSocket::recv_into] or
/// [`OneToManyEndpoint::recv_into`][crate::OneToManyEndpoint::recv_into].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReceivedInto {
    /// A message, whose octets are the first `len` of the buffer.
    Data {
        /// Octets of the message.
        len: usize,

        /// Address of the peer that sent the message. `None` when the kernel gives no address.
        from: Option<std::net::SocketAddr>,

        /// Ancillary information about the message. `None` when
        /// [`request_rcvinfo`][crate::SocketOptions::request_rcvinfo] turned it off.
        rcv_info: Option<RcvInfo>,
    },

    /// A notification. The buffer is as it was.
    Notification(Notification),
}

/// Structure Representing Data to be Sent.
///
/// This structure contains actual payload and optional ancillary data.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SendData {
    /// Message payload.
    pub payload: Vec<u8>,

    /// Optional ancillary information used to send the data.
    pub snd_info: Option<SendInfo>,
}

impl std::fmt::Debug for SendData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendData")
            .field("payload", &Octets(&self.payload))
            .field("snd_info", &self.snd_info)
            .finish()
    }
}

impl SendData {
    /// A message without ancillary information: the defaults of the socket apply.
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            snd_info: None,
        }
    }

    /// The message with this ancillary information.
    #[must_use]
    pub fn with_info(mut self, info: SendInfo) -> Self {
        self.snd_info = Some(info);
        self
    }
}

/// Options for sending a message with [`ConnectedSocket::send`][crate::ConnectedSocket::send]
/// or [`OneToManyEndpoint::send`][crate::OneToManyEndpoint::send].
///
/// The default is an ordered message on stream 0 with the payload protocol identifier 0.
///
/// ```
/// use oxirush_sctp::SendOptions;
///
/// // Stream 1, and the payload protocol identifier of NGAP
/// let options = SendOptions::new().stream(1).ppid(60);
/// assert_eq!(options, SendOptions::default().ppid(60).stream(1));
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SendOptions {
    stream_id: u16,
    ppid: u32,
    unordered: bool,
    context: u32,
    assoc_id: AssociationId,
}

impl SendOptions {
    /// The default options.
    pub const fn new() -> Self {
        Self {
            stream_id: 0,
            ppid: 0,
            unordered: false,
            context: 0,
            assoc_id: 0,
        }
    }

    /// The stream to send on, below [`ConnStatus::outstreams`].
    #[must_use]
    pub const fn stream(mut self, stream_id: u16) -> Self {
        self.stream_id = stream_id;
        self
    }

    /// The payload protocol identifier, as a number: 60 for NGAP, 18 for S1AP.
    #[must_use]
    pub const fn ppid(mut self, ppid: u32) -> Self {
        self.ppid = ppid;
        self
    }

    /// Deliver without ordering relative to the other messages of the stream.
    #[must_use]
    pub const fn unordered(mut self, unordered: bool) -> Self {
        self.unordered = unordered;
        self
    }

    /// A number of the application, which a send failure notification gives back.
    #[must_use]
    pub const fn context(mut self, context: u32) -> Self {
        self.context = context;
        self
    }

    /// The association to send on, for a one-to-many endpoint. A
    /// [`ConnectedSocket`][crate::ConnectedSocket] has one association, and refuses another
    /// value than 0.
    #[must_use]
    pub const fn association(mut self, assoc_id: AssociationId) -> Self {
        self.assoc_id = assoc_id;
        self
    }

    pub(crate) fn assoc_id(&self) -> AssociationId {
        self.assoc_id
    }

    pub(crate) fn info(self) -> SendInfo {
        SendInfo {
            sid: self.stream_id,
            flags: u16::from(self.unordered),
            ppid: self.ppid,
            context: self.context,
            assoc_id: self.assoc_id,
        }
    }
}

/// Failures from a batch event subscription or unsubscription.
///
/// Every requested event is attempted. The returned [`std::io::Error`] has kind `Other` and
/// contains this value; use `error.get_ref().and_then(|source| source.downcast_ref::<Self>())`
/// to inspect each event and its original kernel error. Its error source is the first failure.
#[derive(Debug)]
pub struct EventSubscriptionError {
    pub(crate) failures: Vec<(Event, std::io::Error)>,
}

impl EventSubscriptionError {
    /// Failed events, in request order, with their original errors and errno values.
    pub fn failures(&self) -> &[(Event, std::io::Error)] {
        &self.failures
    }
}

impl std::fmt::Display for EventSubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, (event, error)) in self.failures.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            write!(f, "{event:?}: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for EventSubscriptionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failures.first().map(|(_, error)| error as _)
    }
}

/// Heartbeat and failure-detection settings for one destination path (`SCTP_PEER_ADDR_PARAMS`).
///
/// Times are in milliseconds. Setting these fields leaves PMTU discovery, SACK delay and other
/// Linux path settings unchanged. Association 0 selects socket defaults; an unspecified address
/// applies to all paths of the selected association (RFC 6458, Section 8.1.12).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeerAddressParams {
    /// Association to query or change; ignored on one-to-one sockets.
    pub assoc_id: AssociationId,

    /// Peer path, or an unspecified address for defaults/all paths.
    pub address: std::net::SocketAddr,

    /// Whether periodic heartbeats are enabled on the path.
    pub heartbeat_enabled: bool,

    /// Interval between heartbeats, applied only when enabling them. Disabling leaves the
    /// previous interval unchanged. Zero explicitly selects zero rather than leaving it unchanged.
    pub heartbeat_interval: std::time::Duration,

    /// Path retransmission limit; zero leaves the kernel value unchanged when setting.
    pub path_max_retrans: u16,
}

impl PeerAddressParams {
    /// The settings of a path, to change with
    /// [`set_peer_address_params`][crate::SocketOptions::set_peer_address_params]: heartbeats
    /// enabled every 30 seconds, and the retransmission limit of the kernel.
    pub fn new(assoc_id: AssociationId, address: std::net::SocketAddr) -> Self {
        Self {
            assoc_id,
            address,
            heartbeat_enabled: true,
            heartbeat_interval: std::time::Duration::from_secs(30),
            path_max_retrans: 0,
        }
    }
}

/// Ancillary send information (Section 5.3.4 of RFC 6458): `struct sctp_sndinfo`, whose
/// fields the RFC fixes.
///
/// ```
/// use oxirush_sctp::SendInfo;
///
/// // An unordered message of NGAP on stream 1
/// let info = SendInfo { sid: 1, ppid: 60, flags: SendInfo::UNORDERED, ..Default::default() };
/// assert_eq!(info.assoc_id, 0);
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SendInfo {
    /// Stream ID of the stream to send the data on.
    pub sid: u16,

    /// Flags of the send, `enum sctp_sinfo_flags`, among them [`UNORDERED`][Self::UNORDERED],
    /// [`ABORT`][Self::ABORT] and [`EOF`][Self::EOF], which can be combined with `|`.
    pub flags: u16,

    /// Payload protocol identifier, as a number: 60 for NGAP. The crate converts it to the
    /// network byte order that the kernel carries.
    pub ppid: u32,

    /// Opaque context to be used while sending the data.
    pub context: u32,

    /// Association ID of the SCTP Association to be used while sending the data.
    pub assoc_id: AssociationId,
}

impl SendInfo {
    /// Deliver without ordering relative to the other messages of the stream
    /// (`SCTP_UNORDERED`).
    pub const UNORDERED: u16 = 1;

    /// Send to the address of the send, a peer address of the association, rather than to its
    /// primary path (`SCTP_ADDR_OVER`).
    pub const ADDR_OVER: u16 = 1 << 1;

    /// Abort the association, with the payload as the reason of a user-initiated abort
    /// (`SCTP_ABORT`). A one-to-many socket alone takes it: `ConnectedSocket::abort` does it
    /// on a one-to-one socket.
    pub const ABORT: u16 = 1 << 2;

    /// Ask the peer to acknowledge this message at once (`SCTP_SACK_IMMEDIATELY`).
    pub const SACK_IMMEDIATELY: u16 = 1 << 3;

    /// Send the message on every association of a one-to-many socket (`SCTP_SENDALL`).
    pub const SENDALL: u16 = 1 << 6;

    /// Shut the association down gracefully, with an empty payload (`SCTP_EOF`). A
    /// one-to-many socket alone takes it: `ConnectedSocket::shutdown` does it on a one-to-one
    /// socket.
    pub const EOF: u16 = 0x200;
}

impl From<SendOptions> for SendInfo {
    /// The send information of these options, to which flags can be added.
    fn from(options: SendOptions) -> Self {
        options.info()
    }
}

/// What the associations of a socket start with (`SCTP_INITMSG`, Section 8.1.3 of RFC 6458).
/// When setting them, a 0 leaves the value unchanged, as `Default` has it.
///
/// ```
/// use oxirush_sctp::InitParams;
///
/// // Four streams each way
/// let init = InitParams::streams(4, 4);
/// assert_eq!(init, InitParams { outbound_streams: 4, inbound_streams: 4, ..Default::default() });
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InitParams {
    /// Outbound streams that this side asks for. Linux opens ten by default.
    pub outbound_streams: u16,

    /// Inbound streams that this side takes at most.
    pub inbound_streams: u16,

    /// Times that the INIT is sent before the association fails to start.
    pub max_attempts: u16,

    /// The longest that the INIT waits before it is sent again, in milliseconds.
    pub max_init_timeout: u16,
}

impl InitParams {
    /// Ask for `outbound` streams, and take `inbound` at most, leaving the rest unchanged.
    pub const fn streams(outbound: u16, inbound: u16) -> Self {
        Self {
            outbound_streams: outbound,
            inbound_streams: inbound,
            max_attempts: 0,
            max_init_timeout: 0,
        }
    }
}

/// Retransmission timeout parameters (`SCTP_RTOINFO`, see Section 8.1.1 of RFC 6458).
///
/// The times are in milliseconds. When setting them, a 0 leaves the value unchanged.
///
/// ```
/// use oxirush_sctp::RtoInfo;
///
/// // The defaults of the socket: 200 ms at first, between 100 ms and 1 s
/// let rto = RtoInfo::new(200, 1000, 100);
/// assert_eq!((rto.assoc_id, rto.initial, rto.max, rto.min), (0, 200, 1000, 100));
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RtoInfo {
    /// Association ID of the association, or 0 for the defaults of the socket, which new
    /// associations start with. It is ignored for the association of a One-to-one socket.
    pub assoc_id: AssociationId,

    /// Initial retransmission timeout, also of the INIT.
    pub initial: u32,

    /// Maximum retransmission timeout.
    pub max: u32,

    /// Minimum retransmission timeout.
    pub min: u32,
}

impl RtoInfo {
    /// The initial, the maximum and the minimum retransmission timeout, in milliseconds,
    /// for the defaults of the socket: `assoc_id` is 0, and names one association of a
    /// one-to-many socket when it is set.
    pub fn new(initial: u32, max: u32, min: u32) -> Self {
        Self {
            assoc_id: 0,
            initial,
            max,
            min,
        }
    }
}

/// Association parameters (`SCTP_ASSOCINFO`, see Section 8.1.2 of RFC 6458).
///
/// When setting them, a 0 leaves the value unchanged.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AssociationParams {
    /// Association ID of the association, or 0 for the defaults of the socket, which new
    /// associations start with. It is ignored for the association of a One-to-one socket.
    pub assoc_id: AssociationId,

    /// Retransmissions over all the paths together after which the association is lost.
    pub max_retransmissions: u16,

    /// Number of addresses of the peer. Read only.
    pub peer_addresses: u16,

    /// The receive window of the peer, in octets. Read only.
    pub peer_rwnd: u32,

    /// The receive window of this side, in octets. Read only.
    pub local_rwnd: u32,

    /// Life of the cookie that this side sends, in milliseconds.
    pub cookie_life: u32,
}

/// Delayed acknowledgement (`SCTP_DELAYED_SACK`, see Section 8.1.19 of RFC 6458).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SackInfo {
    /// Association ID of the association, or 0 for the defaults of the socket. It is ignored
    /// for the association of a One-to-one socket.
    pub assoc_id: AssociationId,

    /// Time that an acknowledgement may wait, in milliseconds. When setting, 0 leaves it
    /// unchanged.
    pub delay: u32,

    /// Number of packets after which an acknowledgement is sent without waiting: 1 turns
    /// the delay off. When setting, 0 leaves it unchanged.
    pub frequency: u32,
}

/// Ancillary receive information (Section 5.3.5 of RFC 6458).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RcvInfo {
    /// Stream ID on which the data is received.
    pub sid: u16,

    /// Stream Sequence Number received in the data.
    pub ssn: u16,

    /// Flags for the received data.
    pub flags: u16,

    /// Payload protocol identifier that the sender gave, as a number: 60 for NGAP.
    pub ppid: u32,

    /// Transaction sequence number.
    pub tsn: u32,

    /// Cumulative sequence number.
    pub cumtsn: u32,

    /// Opaque context.
    pub context: u32,

    /// SCTP Association ID.
    pub assoc_id: AssociationId,
}

/// Ancillary information about the next message (Section 5.3.6 of RFC 6458).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NxtInfo {
    /// Stream ID for the next received data.
    pub sid: u16,

    /// Flags for the next received data.
    pub flags: u16,

    /// Payload protocol identifier of the next message, as a number.
    pub ppid: u32,

    /// Length of the message to be used in the next `recv` call.
    pub length: u32,

    /// SCTP Association ID.
    pub assoc_id: AssociationId,
}

#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
/// An `enum` representing the notifications received on the SCTP Sockets. `Debug` prints
/// octets as their length and each in hexadecimal.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// # socket.bind("127.0.0.1:0".parse().unwrap())?;
/// # let listener = socket.listen(5)?;
/// use oxirush_sctp::{AssocChangeState, Event, Notification, NotificationOrData};
/// use oxirush_sctp::{Socket, SocketToAssociation, SubscribeEventAssocId};
///
/// // A client that subscribes to the changes of its association before connecting
/// let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let events = [Event::Association];
/// client.options().subscribe_events(&events, SubscribeEventAssocId::All)?;
/// let (client, _) = client.connect(listener.local_addr()?).await?;
///
/// // The first that it receives: the association is up, with its streams
/// let received = client.recv().await?;
/// let Some(NotificationOrData::Notification(Notification::AssociationChange(change))) = received
/// else {
///     unreachable!()
/// };
/// assert_eq!(change.state, AssocChangeState::CommUp);
/// assert!(change.outbound_streams > 0);
/// # Ok(())
/// # }
/// ```
pub enum Notification {
    /// Association Change Notification. See Section 6.1.1 of RFC 6458.
    AssociationChange(AssociationChange),

    /// Shutdown Notification. See Section 6.1.5 of RFC 6458.
    Shutdown(Shutdown),

    /// Peer path status changed. Subscribe to [`Event::Address`].
    PeerAddressChange(PeerAddressChange),

    /// A message could not be delivered. Subscribe to [`Event::SendFailureEvent`].
    SendFailure(SendFailure),

    /// The peer sent an ERROR chunk. Subscribe to [`Event::PeerError`].
    RemoteError(RemoteError),

    /// The delivery of a message in several parts was given up. Subscribe to
    /// [`Event::PartialDelivery`].
    PartialDelivery(PartialDelivery),

    /// The association has no more data to send or to retransmit. Subscribe to
    /// [`Event::SenderDry`].
    SenderDry {
        /// Association that has nothing left to send.
        assoc_id: AssociationId,
    },

    /// A notification this crate does not parse, such as the deprecated send-failure format
    /// (Section 6.1 of RFC 6458), or one malformed for its type.
    Unsupported {
        /// Type of the notification as the kernel numbers it (`sn_type`), 0 for a
        /// notification too short to have one.
        kind: u16,

        /// The notification as the kernel delivered it, header included, in host byte order.
        data: Vec<u8>,
    },
}

impl std::fmt::Debug for Notification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AssociationChange(change) => {
                f.debug_tuple("AssociationChange").field(change).finish()
            }
            Self::Shutdown(shutdown) => f.debug_tuple("Shutdown").field(shutdown).finish(),
            Self::PeerAddressChange(change) => {
                f.debug_tuple("PeerAddressChange").field(change).finish()
            }
            Self::SendFailure(failure) => f.debug_tuple("SendFailure").field(failure).finish(),
            Self::RemoteError(error) => f.debug_tuple("RemoteError").field(error).finish(),
            Self::PartialDelivery(delivery) => {
                f.debug_tuple("PartialDelivery").field(delivery).finish()
            }
            Self::SenderDry { assoc_id } => f
                .debug_struct("SenderDry")
                .field("assoc_id", assoc_id)
                .finish(),
            Self::Unsupported { kind, data } => f
                .debug_struct("Unsupported")
                .field("kind", kind)
                .field("data", &Octets(data))
                .finish(),
        }
    }
}

/// Status reported by a peer-address-change notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerAddressState {
    /// The path is reachable.
    Available,

    /// The path is unreachable after its retransmission limit.
    Unreachable,

    /// The address was removed from the association.
    Removed,

    /// The address was added to the association.
    Added,

    /// The address became the primary path.
    MadePrimary,

    /// The address was confirmed reachable.
    Confirmed,

    /// Linux marked the path potentially failed.
    PotentiallyFailed,

    /// A state unknown to this crate; its kernel value is preserved.
    Unknown(i32),
}

impl PeerAddressState {
    pub(crate) fn from_i32(state: i32) -> Self {
        match state {
            0 => Self::Available,
            1 => Self::Unreachable,
            2 => Self::Removed,
            3 => Self::Added,
            4 => Self::MadePrimary,
            5 => Self::Confirmed,
            6 => Self::PotentiallyFailed,
            value => Self::Unknown(value),
        }
    }
}

/// A peer path change (`SCTP_PEER_ADDR_CHANGE`, RFC 6458 Section 6.1.2).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeerAddressChange {
    /// Notification flags as delivered by the kernel.
    pub flags: u16,

    /// The affected peer address.
    pub address: std::net::SocketAddr,

    /// New path state, including unrecognized kernel values.
    pub state: PeerAddressState,

    /// Kernel error code associated with this change.
    pub error: i32,

    /// Association whose path changed.
    pub assoc_id: AssociationId,
}

/// A send failure (`SCTP_SEND_FAILED_EVENT`, RFC 6458 Section 6.1.11).
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SendFailure {
    /// Whether the message was put on the wire, without its delivery being confirmed. A
    /// message that was not is known not to have reached the peer.
    pub sent: bool,

    /// Why the send failed: an error cause of SCTP (RFC 9260, Section 3.3.10), such as 12
    /// after the peer's application aborted the association. 0 when Linux gives none.
    pub cause: u16,

    /// What the message was sent with.
    pub snd_info: SendInfo,

    /// Association whose message failed; can be 0 if establishment failed before an ID was assigned.
    pub assoc_id: AssociationId,

    /// Undelivered message bytes returned by the kernel.
    pub payload: Vec<u8>,
}

impl std::fmt::Debug for SendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFailure")
            .field("sent", &self.sent)
            .field("cause", &self.cause)
            .field("snd_info", &self.snd_info)
            .field("assoc_id", &self.assoc_id)
            .field("payload", &Octets(&self.payload))
            .finish()
    }
}

/// An ERROR chunk of the peer (`SCTP_REMOTE_ERROR`, RFC 6458 Section 6.1.3).
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RemoteError {
    /// The error cause of SCTP that the peer reports (RFC 9260, Section 3.3.10).
    pub cause: u16,

    /// Association whose peer sent the error.
    pub assoc_id: AssociationId,

    /// The error cause as the peer sent it, from its cause code on.
    pub data: Vec<u8>,
}

impl std::fmt::Debug for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteError")
            .field("cause", &self.cause)
            .field("assoc_id", &self.assoc_id)
            .field("data", &Octets(&self.data))
            .finish()
    }
}

/// The delivery of a message in several parts was given up
/// (`SCTP_PARTIAL_DELIVERY_EVENT`, RFC 6458 Section 6.1.7): what was received of the message
/// is to be discarded, which `recv` has done.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartialDelivery {
    /// What happened, as the kernel numbers it: 0 is `SCTP_PARTIAL_DELIVERY_ABORTED`.
    pub indication: u32,

    /// Association of the message.
    pub assoc_id: AssociationId,

    /// Stream of the message.
    pub stream: u32,

    /// Stream sequence number of the message.
    pub sequence: u32,
}

/// AssociationChange: Structure returned as notification for Association Change.
///
/// To subscribe to this notification type, an application should call
/// [`subscribe_events`][crate::SocketOptions::subscribe_events] using the [`Event`]
/// type as [`Event::Association`].
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AssociationChange {
    /// Association Change state. See also [`AssocChangeState`].
    pub state: AssocChangeState,

    /// Error cause of SCTP when the association was lost or could not start (RFC 9260,
    /// Section 3.3.10): 12 after the peer's application aborted. 0 when there is none.
    pub cause: u16,

    /// Maximum number of outbound streams.
    pub outbound_streams: u16,

    /// Maximum number of inbound streams.
    pub inbound_streams: u16,

    /// Association ID for the event.
    pub assoc_id: AssociationId,

    /// Additional data for the event: the error causes of the ABORT when the peer aborted the
    /// association.
    pub info: Vec<u8>,
}

impl std::fmt::Debug for AssociationChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssociationChange")
            .field("state", &self.state)
            .field("cause", &self.cause)
            .field("outbound_streams", &self.outbound_streams)
            .field("inbound_streams", &self.inbound_streams)
            .field("assoc_id", &self.assoc_id)
            .field("info", &Octets(&self.info))
            .finish()
    }
}

/// Shutdown: Structure returned as notification when the peer shuts the association down.
///
/// To subscribe to this notification type, an application should call
/// [`subscribe_events`][crate::SocketOptions::subscribe_events] using the [`Event`]
/// type as [`Event::Shutdown`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Shutdown {
    /// Association ID for the event.
    pub assoc_id: AssociationId,
}

/// Event: Used for Subscribing for SCTP Events
///
/// See [`subscribe_events`][`crate::SocketOptions::subscribe_events`] for the usage.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// The deprecated `SCTP_SNDRCV` ancillary data, which this crate does not parse: see
    /// [`request_rcvinfo`][crate::SocketOptions::request_rcvinfo].
    DataIo = (1 << 15),

    /// Event related to association change.
    Association,

    /// Event related to peer address change.
    Address,

    /// The deprecated send failure notification, delivered as [`Notification::Unsupported`]:
    /// subscribe to [`Event::SendFailureEvent`] instead.
    SendFailure,

    /// An ERROR chunk of the peer: [`Notification::RemoteError`].
    PeerError,

    /// Event related to indicate peer shutdown.
    Shutdown,

    /// A message whose delivery in parts was given up: [`Notification::PartialDelivery`].
    PartialDelivery,

    /// Event related to indicate peer's partial indication.
    AdaptationLayer,

    /// Authentication event.
    Authentication,

    /// Nothing left to send or to retransmit: [`Notification::SenderDry`].
    SenderDry,

    /// Event related to stream reset.
    StreamReset,

    /// Event related to association reset.
    AssociationReset,

    /// Event related to stream change.
    StreamChange,

    /// A message could not be delivered: [`Notification::SendFailure`].
    SendFailureEvent,
}

/// SubscribeEventAssocId: AssociationID Used for Event Subscription
///
/// A one-to-one socket ignores it: a subscription made before `listen` or `connect` applies to
/// the associations accepted or established afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscribeEventAssocId {
    /// Subscribe to Future Association IDs
    Future,

    /// Subscribe to Current Association IDs
    Current,

    /// Subscribe to ALL Association IDs
    All,

    /// Subscribe to Association ID with a given value.
    Value(AssociationId),
}

impl From<SubscribeEventAssocId> for AssociationId {
    fn from(value: SubscribeEventAssocId) -> Self {
        match value {
            SubscribeEventAssocId::Future => 0 as Self,
            SubscribeEventAssocId::Current => 1 as Self,
            SubscribeEventAssocId::All => 2 as Self,
            SubscribeEventAssocId::Value(v) => v,
        }
    }
}

/// Association Change States
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AssocChangeState {
    /// SCTP communication up.
    CommUp,

    /// SCTP communication lost: the peer aborted the association or became unreachable.
    CommLost,

    /// SCTP communication restarted.
    Restart,

    /// Shutdown complete: the association ended gracefully.
    ShutdownComplete,

    /// Cannot start association.
    CannotStartAssoc,

    /// A state unknown to this crate; its kernel value is preserved.
    Unknown(u16),
}

impl AssocChangeState {
    pub(crate) fn from_u16(val: u16) -> Self {
        match val {
            0 => AssocChangeState::CommUp,
            1 => AssocChangeState::CommLost,
            2 => AssocChangeState::Restart,
            3 => AssocChangeState::ShutdownComplete,
            4 => AssocChangeState::CannotStartAssoc,
            value => AssocChangeState::Unknown(value),
        }
    }
}

/// Constants related to `enum sctp_cmsg_type`
#[repr(i32)]
#[allow(dead_code)]
pub(crate) enum CmsgType {
    Init = 0,
    SndRcv,
    SndInfo,
    RcvInfo,
    NxtInfo,
    PrInfo,
    AuthInfo,
    DstAddrV4,
    DstAddrV6,
}

/// State of an association (`enum sctp_sstat_state`, RFC 9260 Section 4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnState {
    /// No association.
    #[default]
    Empty,

    /// Closed.
    Closed,

    /// INIT sent.
    CookieWait,

    /// COOKIE ECHO sent.
    CookieEchoed,

    /// Established: messages can be exchanged.
    Established,

    /// Shutdown requested locally, waiting for outstanding data to be acknowledged.
    ShutdownPending,

    /// SHUTDOWN sent.
    ShutdownSent,

    /// SHUTDOWN received from the peer.
    ShutdownReceived,

    /// SHUTDOWN ACK sent.
    ShutdownAckSent,

    /// A state unknown to this crate; its kernel value is preserved.
    Unknown(i32),
}

impl ConnState {
    fn from_i32(val: i32) -> Self {
        match val {
            0 => Self::Empty,
            1 => Self::Closed,
            2 => Self::CookieWait,
            3 => Self::CookieEchoed,
            4 => Self::Established,
            5 => Self::ShutdownPending,
            6 => Self::ShutdownSent,
            7 => Self::ShutdownReceived,
            8 => Self::ShutdownAckSent,
            value => Self::Unknown(value),
        }
    }
}

/// The state of a path to the peer (`enum sctp_spinfo_state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathState {
    /// Unreachable: its retransmissions went past the limit of the path.
    Inactive,

    /// Potentially failed: retransmissions went past the threshold of RFC 7829, and the
    /// association prefers another path.
    PotentiallyFailed,

    /// Reachable.
    Active,

    /// Not yet confirmed by a heartbeat: the association does not send data on it.
    Unconfirmed,

    /// A state unknown to this crate; its kernel value is preserved.
    Unknown(i32),
}

impl PathState {
    pub(crate) fn from_i32(state: i32) -> Self {
        match state {
            0 => Self::Inactive,
            1 => Self::PotentiallyFailed,
            2 => Self::Active,
            3 => Self::Unconfirmed,
            value => Self::Unknown(value),
        }
    }
}

/// PeerAddress: one path to the peer (`struct sctp_paddrinfo`, RFC 6458 Section 8.2.2), as
/// [`ConnectedSocket::paths`][crate::ConnectedSocket::paths] and
/// [`peer_address_info`][crate::SocketOptions::peer_address_info] read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeerAddress {
    /// Association of the path.
    pub assoc_id: AssociationId,

    /// The peer's address.
    pub address: std::net::SocketAddr,

    /// State of the path.
    pub state: PathState,

    /// Congestion window in octets.
    pub cwnd: u32,

    /// Smoothed round-trip time, in milliseconds.
    pub srtt: u32,

    /// Retransmission timeout in milliseconds.
    pub rto: u32,

    /// Path MTU in octets.
    pub mtu: u32,
}

/// ConnStatus: status of an association (`SCTP_STATUS`, RFC 6458 Section 8.2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnStatus {
    /// Association ID.
    pub assoc_id: AssociationId,

    /// State of the association.
    pub state: ConnState,

    /// The peer's receive window in octets.
    pub rwnd: u32,

    /// Number of DATA chunks sent and not yet acknowledged.
    pub unacked_data: u16,

    /// Number of DATA chunks received and not yet read.
    pub pending_data: u16,

    /// Number of inbound streams: the peer sends on streams below it.
    pub instreams: u16,

    /// Number of outbound streams: [`SendOptions::stream`] must be below it.
    pub outstreams: u16,

    /// Size in octets above which a message is fragmented.
    pub fragmentation_pt: u32,

    /// The primary path to the peer.
    pub peer_primary: PeerAddress,
}

pub(crate) mod internal;

#[cfg(test)]
mod tests {
    use super::*;

    /// Octets are printed as their number and in hexadecimal, as in the other crates of the
    /// family, where they were a list of numbers in decimal.
    #[test]
    fn debug_prints_octets_in_hexadecimal() {
        let message = ReceivedData {
            payload: b"\x00\x15ping".to_vec(),
            from: None,
            rcv_info: None,
            nxt_info: None,
        };
        assert_eq!(
            format!("{message:?}"),
            "ReceivedData { payload: 6 octets 001570696e67, from: None, rcv_info: None, \
             nxt_info: None }"
        );
        let unsupported = Notification::Unsupported {
            kind: 0x8010,
            data: vec![0x10, 0x80],
        };
        assert_eq!(
            format!("{unsupported:?}"),
            "Unsupported { kind: 32784, data: 2 octets 1080 }"
        );
    }

    /// The flags of a send are named, and a value of the structure is written as a literal.
    #[test]
    fn the_flags_of_a_send_have_names() {
        let info = SendInfo {
            flags: SendInfo::UNORDERED | SendInfo::SACK_IMMEDIATELY,
            ..Default::default()
        };
        assert_eq!(info.flags, 9);
        assert_eq!(
            (SendInfo::ABORT, SendInfo::EOF),
            (crate::consts::SCTP_ABORT, crate::consts::SCTP_EOF)
        );
        assert_eq!(
            SendInfo::from(SendOptions::new().unordered(true)).flags,
            SendInfo::UNORDERED
        );
    }
}
