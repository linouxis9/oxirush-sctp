//! Types used by the Public APIs

/// SCTP Association ID Type
pub type AssociationId = i32;

/// Flags used by `sctp_bindx`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindxFlags {
    /// Add the addresses passed (corresponding to `SCTP_BINDX_ADD_ADDR`)
    Add,

    /// Remove the addresses passed (corresponding to `SCTP_BINDX_REM_ADDR`)
    Remove,
}

/// SocketToAssociation: One-to-Many or One-to-One style Socket
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketToAssociation {
    /// One Association per Socket (TCP Style Socket.)
    OneToOne,

    /// Many Associations per Socket (UDP Style Socket.)
    OneToMany,
}

/// NotificationOrData: A type returned by a `recv` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationOrData {
    /// SCTP Notification received by a `recv` call.
    Notification(Notification),

    /// SCTP Data Received by a `recv` call.
    Data(ReceivedData),
}

/// Structure Representing SCTP Received Data.
///
/// This structure is returned by the `recv` API call. This contains in addition to 'received'
/// data, any ancillary data that is received during the underlying system call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedData {
    /// Received Message Payload.
    pub payload: Vec<u8>,

    /// Address of the peer that sent the message, which tells the associations of a One-to-many
    /// socket apart. `None` when the kernel gives no address, as at the end of the stream.
    pub from: Option<std::net::SocketAddr>,

    /// Optional ancillary information about the received payload.
    pub rcv_info: Option<RcvInfo>,

    /// Optional ancillary information about the next call to `recv`.
    pub nxt_info: Option<NxtInfo>,
}

/// Structure Represnting Data to be Sent.
///
/// This structure contains actual paylod and optional ancillary data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendData {
    /// Received Message Payload.
    pub payload: Vec<u8>,

    /// Optional ancillary information used to send the data.
    pub snd_info: Option<SendInfo>,
}

/// Options for sending a borrowed message with [`ConnectedSocket::send`][crate::ConnectedSocket::send]
/// or [`OneToManyEndpoint::send`][crate::OneToManyEndpoint::send].
///
/// Unlike the low-level [`SendInfo`], `ppid` uses host byte order; conversion happens once when
/// the message is sent. Defaults select ordered delivery on stream 0 with PPID 0.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SendOptions {
    /// Outbound stream identifier.
    pub stream_id: u16,
    /// Payload protocol identifier in host byte order, e.g. 60 for NGAP.
    pub ppid: u32,
    /// Deliver without ordering relative to other messages on the stream.
    pub unordered: bool,
    /// Application context returned in send-failure notifications.
    pub context: u32,
    /// Destination association on a one-to-many socket; ignored on one-to-one sockets.
    pub assoc_id: AssociationId,
}

impl SendOptions {
    pub(crate) fn wire_info(self) -> SendInfo {
        SendInfo {
            sid: self.stream_id,
            flags: u16::from(self.unordered),
            ppid: self.ppid.to_be(),
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
            write!(f, "{:?}: {}", event, error)?;
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

/// Structure representing Ancilliary Send Information (See Section 5.3.4 of RFC 6458)
#[repr(C)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SendInfo {
    /// Stream ID of the stream to send the data on.
    pub sid: u16,

    /// Flags to be used while sending the data.
    pub flags: u16,

    /// Application Protocol ID to be used while sending the data.
    ///
    /// It is sent as is, without byte order conversion (RFC 6458, Section 5.3.4): for the
    /// payload protocol identifiers assigned by IANA, use network byte order, e.g.
    /// `60_u32.to_be()` for NGAP.
    pub ppid: u32,

    /// Opaque context to be used while sending the data.
    pub context: u32,

    /// Association ID of the SCTP Association to be used while sending the data.
    pub assoc_id: AssociationId,
}

/// Retransmission timeout parameters (`SCTP_RTOINFO`, see Section 8.1.1 of RFC 6458).
///
/// The times are in milliseconds. When setting them, a 0 leaves the value unchanged.
#[repr(C)]
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

/// Structure Representing Ancillary Receive Information (See Section 5.3.5 of RFC 6458)
#[repr(C)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RcvInfo {
    /// Stream ID on which the data is received.
    pub sid: u16,

    /// Stream Sequence Number received in the data.
    pub ssn: u16,

    /// Flags for the received data.
    pub flags: u16,

    /// Application Protocol ID used by the sender while sending the data, as received, without
    /// byte order conversion (see [`SendInfo::ppid`]).
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

/// Structure representing Ancillary next information (See Section 5.3.5)
#[repr(C)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NxtInfo {
    /// Stream ID for the next received data.
    pub sid: u16,

    /// Flags for the next received data.
    pub flags: u16,

    /// Application protocol ID, without byte order conversion (see [`SendInfo::ppid`]).
    pub ppid: u32,

    /// Length of the message to be used in the next `recv` call.
    pub length: u32,

    /// SCTP Association ID.
    pub assoc_id: AssociationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
/// An `enum` representing the notifications received on the SCTP Sockets.
pub enum Notification {
    /// Association Change Notification. See Section 6.1.1 of RFC 6458.
    AssociationChange(AssociationChange),

    /// Shutdown Notification. See Section 6.1.5 of RFC 6458.
    Shutdown(Shutdown),

    /// Peer path status changed. Subscribe to [`Event::Address`].
    PeerAddressChange(PeerAddressChange),

    /// A message could not be delivered. Subscribe to [`Event::SendFailureEvent`].
    SendFailure(SendFailure),

    /// A notification this crate does not parse, such as the deprecated send-failure format,
    /// a remote error or sender dry (Section 6.1 of RFC 6458), or one malformed for its type.
    Unsupported {
        /// Type of the notification, [`Event::Unknown`] for a type this crate does not know or a
        /// notification too short to have one.
        ev_type: Event,

        /// The notification as the kernel delivered it, header included, in host byte order.
        data: Vec<u8>,
    },
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

/// A modern send failure (`SCTP_SEND_FAILED_EVENT`, RFC 6458 Section 6.1.11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendFailure {
    /// Kernel flags: 0 means never sent, 1 means transmitted without confirmed delivery.
    pub flags: u16,
    /// Kernel error associated with the failed send.
    pub error: u32,
    /// Original send metadata. PPID keeps the wire-order semantics of [`SendInfo`].
    pub snd_info: SendInfo,
    /// Association whose message failed; can be 0 if establishment failed before an ID was assigned.
    pub assoc_id: AssociationId,
    /// Undelivered message bytes returned by the kernel.
    pub payload: Vec<u8>,
}

/// AssociationChange: Structure returned as notification for Association Change.
///
/// To subscribe to this notification type, An application should call `sctp_subscribe_event` using
/// the [`Event`] type as [`Event::Association`].
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationChange {
    /// Type of the Notification always `SCTP_ASSOC_CHAGE`
    pub ev_type: Event,

    /// Notification Flags. Unused currently.
    pub flags: u16,

    /// Length of the notification data.
    pub length: u32,

    /// Association Change state. See also [`AssocChangeState`].
    pub state: AssocChangeState,

    /// Error when state is an error state and error information is available.
    pub error: u16,

    /// Maximum number of outbound streams.
    pub ob_streams: u16,

    /// Maximum number of inbound streams.
    pub ib_streams: u16,

    /// Association ID for the event.
    pub assoc_id: AssociationId,

    /// Additional data for the event.
    pub info: Vec<u8>,
}

/// Shutdown: Structure rreturned as notification for Shutdown Event.
///
///To subscribe to this notification type, An application should call `sctp_subscribe_event` using
///the [`Event`] ty[e as [`Event::Shutdown`]
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shutdown {
    /// Type of the Notification always `SCTP_SHUTDOWN`
    pub ev_type: Event,

    /// Notification Flags. Unused currently.
    pub flags: u16,

    /// Length of the notification data.
    pub length: u32,

    /// Association ID for the event.
    pub assoc_id: AssociationId,
}

/// Event: Used for Subscribing for SCTP Events
///
/// See [`sctp_subscribe_events`][`crate::SocketOptions::sctp_subscribe_events`] for the usage.
#[repr(u16)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Event to receive ancillary information with every `recv`.
    DataIo = (1 << 15),

    /// Event related to association change.
    Association,

    /// Event related to peer address change.
    Address,

    /// Event related to send failure.
    SendFailure,

    /// Event related to error received from the peer.
    PeerError,

    /// Event related to indicate peer shutdown.
    Shutdown,

    /// Event related to indicate partial delivery.
    PartialDelivery,

    /// Event related to indicate peer's partial indication.
    AdaptationLayer,

    /// Authentication event.
    Authentication,

    /// Event related to sender having no outstanding user data.
    SenderDry,

    /// Event related to stream reset.
    StreamReset,

    /// Event related to association reset.
    AssociationReset,

    /// Event related to stream change.
    StreamChange,

    /// Send Failure Event indication. (The actual received information is different from the one
    /// received in the `SendFailed` event.)
    SendFailureEvent,

    /// Unknown Event: Used only when unknwon value is received as a `Notification`.
    Unknown,
}

impl Event {
    pub(crate) fn from_u16(val: u16) -> Self {
        match val {
            0x8000 => Event::DataIo,
            0x8001 => Event::Association,
            0x8002 => Event::Address,
            0x8003 => Event::SendFailure,
            0x8004 => Event::PeerError,
            0x8005 => Event::Shutdown,
            0x8006 => Event::PartialDelivery,
            0x8007 => Event::AdaptationLayer,
            0x8008 => Event::Authentication,
            0x8009 => Event::SenderDry,
            0x800A => Event::StreamReset,
            0x800B => Event::AssociationReset,
            0x800C => Event::StreamChange,
            0x800D => Event::SendFailureEvent,
            _ => Event::Unknown,
        }
    }
}

/// SubscribeEventAssocId: AssociationID Used for Event Subscription
///
/// Note: repr should be same as `AssociationId` (ie. `i32`)
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[repr(u16)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssocChangeState {
    /// SCTP communication up.
    CommUp = 0,

    /// SCTP communication lost.
    CommLost,

    /// SCTP communication restarted.
    Restart,

    /// Shutdown complete.
    ShutdownComplete,

    /// Cannot start association.
    CannotStartAssoc,

    /// Unknown State: This value indicates an error
    Unknown,
}

impl AssocChangeState {
    pub(crate) fn from_u16(val: u16) -> Self {
        match val {
            0 => AssocChangeState::CommUp,
            1 => AssocChangeState::CommLost,
            2 => AssocChangeState::Restart,
            3 => AssocChangeState::ShutdownComplete,
            4 => AssocChangeState::CannotStartAssoc,
            _ => AssocChangeState::Unknown,
        }
    }
}

/// Constants related to `enum sctp_cmsg_type`
#[repr(i32)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CmsgType {
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

/// Constants related to `enum sctp_sstat_state`
#[repr(i32)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ConnState {
    #[default]
    Empty = 0,
    Closed,
    CookieWait,
    CookieEchoed,
    Established,
    ShutdownPending,
    ShutdownSent,
    ShutdownReceived,
    ShutdownAckSent,

    Unknown, // Should never be seen.
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
            _ => Self::Unknown,
        }
    }
}

/// PeerAddress: Structure representing SCTP Peer Address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerAddress {
    pub assoc_id: AssociationId,
    pub address: std::net::SocketAddr,
    pub state: i32,
    pub cwnd: u32,
    pub srtt: u32,
    pub rto: u32,
    pub mtu: u32,
}

/// ConnStatus: Status of an SCTP Connection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnStatus {
    pub assoc_id: AssociationId,
    pub state: ConnState,
    pub rwnd: u32,
    pub unacked_data: u16,
    pub pending_data: u16,
    pub instreams: u16,
    pub outstreams: u16,
    pub fragmentation_pt: u32,
    pub peer_primary: PeerAddress,
}

pub(crate) mod internal;
