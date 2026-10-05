//! Borrowed configuration shared by every SCTP socket role.
use crate::internal::{sys, SocketCore};
use crate::{AssociationId, Event, PeerAddressParams, RtoInfo, SendInfo, SubscribeEventAssocId};
use std::net::SocketAddr;
use std::os::fd::AsFd;

/// A configuration view borrowing its socket, listener, association or endpoint.
///
/// Kernel options and the receive limit belong to the underlying socket. Moving a socket into
/// a listener, association or endpoint preserves them; accepted and peeled-off associations
/// inherit the receive limit. Association IDs are ignored by Linux on one-to-one sockets.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use oxirush_sctp::{Event, Socket, SocketToAssociation, SubscribeEventAssocId};
///
/// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let options = socket.options();
/// options.set_nodelay(true)?;
/// options.set_init_params(4, 4, 0, 0)?; // four streams each way
/// options.subscribe_events(
///     &[Event::Association, Event::Shutdown],
///     SubscribeEventAssocId::All,
/// )?;
/// assert!(options.nodelay()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct SocketOptions<'a> {
    pub(crate) core: &'a SocketCore,
}

impl SocketOptions<'_> {
    /// Subscribe to events. Every event is attempted; errors retain all failed events and errno
    /// values in [`EventSubscriptionError`][crate::EventSubscriptionError]. Associations
    /// accepted from a listener inherit its subscriptions.
    pub fn subscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        sys::sctp_subscribe_events_internal(self.core.as_fd(), events, assoc_id, true)
    }

    /// Unsubscribe from events, with the same attempt-all error behavior as subscription.
    pub fn unsubscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        sys::sctp_subscribe_events_internal(self.core.as_fd(), events, assoc_id, false)
    }

    /// Set outgoing/incoming stream counts, INIT retry count and timeout in milliseconds for
    /// associations established after this call (`SCTP_INITMSG`). A zero leaves that value
    /// unchanged; by default Linux opens ten outgoing streams.
    pub fn set_init_params(
        &self,
        ostreams: u16,
        istreams: u16,
        retries: u16,
        timeout: u16,
    ) -> std::io::Result<()> {
        sys::sctp_setup_init_params_internal(
            self.core.as_fd(),
            ostreams,
            istreams,
            retries,
            timeout,
        )
    }

    /// Enable receive metadata: the stream, payload protocol and association of each message.
    /// Enabled by default. On a [`OneToManyEndpoint`][crate::OneToManyEndpoint], disabling it
    /// loses the association's identity and can make partially delivered records from
    /// multihomed peers ambiguous.
    pub fn request_rcvinfo(&self, on: bool) -> std::io::Result<()> {
        sys::request_rcvinfo_internal(self.core.as_fd(), on)
    }

    /// Enable metadata describing the next queued message (`SCTP_NXTINFO`).
    pub fn request_nxtinfo(&self, on: bool) -> std::io::Result<()> {
        sys::request_nxtinfo_internal(self.core.as_fd(), on)
    }

    /// Set wire-order ancillary defaults used when a send supplies no `SendInfo`.
    pub fn set_default_sendinfo(&self, info: SendInfo) -> std::io::Result<()> {
        sys::sctp_set_default_sendinfo_internal(self.core.as_fd(), info)
    }

    /// Send small messages as soon as congestion permits, without Nagle-like bundling delays.
    /// Linux accepted and peeled-off sockets inherit this setting.
    pub fn set_nodelay(&self, on: bool) -> std::io::Result<()> {
        sys::sctp_set_nodelay_internal(self.core.as_fd(), on)
    }

    /// Whether `SCTP_NODELAY` is enabled.
    pub fn nodelay(&self) -> std::io::Result<bool> {
        sys::sctp_nodelay_internal(self.core.as_fd())
    }

    /// Choose graceful close (`None`) or abort on close (`Some(Duration::ZERO)`). Positive
    /// durations are rejected because Linux can block close even on a nonblocking socket.
    pub fn set_linger(&self, linger: Option<std::time::Duration>) -> std::io::Result<()> {
        sys::set_linger_internal(self.core.as_fd(), linger)
    }

    /// Set association retransmission timeouts, or socket defaults when the ID is zero.
    pub fn set_rto_info(&self, info: RtoInfo) -> std::io::Result<()> {
        sys::sctp_set_rto_info_internal(self.core.as_fd(), info)
    }

    /// Query association retransmission timeouts, or socket defaults when the ID is zero.
    pub fn rto_info(&self, assoc_id: AssociationId) -> std::io::Result<RtoInfo> {
        sys::sctp_get_rto_info_internal(self.core.as_fd(), assoc_id)
    }

    /// Permit rebinding local addresses when Linux SCTP socket rules allow it.
    pub fn set_reuseaddr(&self, on: bool) -> std::io::Result<()> {
        sys::set_reuseaddr_internal(self.core.as_fd(), on)
    }

    /// Whether `SO_REUSEADDR` is enabled.
    pub fn reuseaddr(&self) -> std::io::Result<bool> {
        sys::reuseaddr_internal(self.core.as_fd())
    }

    /// Query heartbeat and retransmission settings for a peer path.
    pub fn peer_address_params(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<PeerAddressParams> {
        sys::peer_address_params_internal(self.core.as_fd(), assoc_id, address)
    }

    /// Change heartbeat and retransmission settings, preserving unrelated path settings.
    pub fn set_peer_address_params(&self, params: PeerAddressParams) -> std::io::Result<()> {
        sys::set_peer_address_params_internal(self.core.as_fd(), params)
    }

    /// Request an immediate heartbeat on a peer path.
    pub fn request_heartbeat(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<()> {
        sys::request_heartbeat_internal(self.core.as_fd(), assoc_id, address)
    }

    /// Set the largest complete message returned by `recv` (4 MiB by default). Longer records
    /// are drained and reported as `InvalidData`; the next call starts at the next record.
    pub fn set_max_message_size(&self, octets: usize) {
        self.core.set_max_message_size(octets);
    }

    /// The current complete-message limit.
    pub fn max_message_size(&self) -> usize {
        self.core.max_message_size()
    }
}
