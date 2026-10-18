//! Borrowed configuration shared by every SCTP socket role.
use crate::internal::{SocketCore, sys};
use crate::{
    AssociationId, AssociationParams, Event, InitParams, PeerAddress, PeerAddressParams, RtoInfo,
    SackInfo, SendInfo, SubscribeEventAssocId,
};
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
/// use oxirush_sctp::{Event, InitParams, Socket, SocketToAssociation, SubscribeEventAssocId};
///
/// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let options = socket.options();
/// options.set_nodelay(true)?;
/// options.set_init_params(InitParams::streams(4, 4))?; // four streams each way
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
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use oxirush_sctp::{Event, Socket, SocketToAssociation, SubscribeEventAssocId};
    ///
    /// // Before listening: the accepted associations inherit it
    /// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// let events = [Event::Association, Event::Shutdown];
    /// socket.options().subscribe_events(&events, SubscribeEventAssocId::All)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        self.core.subscribe_events(events, assoc_id, true)
    }

    /// Unsubscribe from events, with the same attempt-all error behavior as subscription.
    pub fn unsubscribe_events(
        &self,
        events: &[Event],
        assoc_id: SubscribeEventAssocId,
    ) -> std::io::Result<()> {
        self.core.subscribe_events(events, assoc_id, false)
    }

    /// Set the streams, the attempts and the timeout of the INIT of the associations that
    /// start after this call (`SCTP_INITMSG`). A zero leaves that value unchanged; by default
    /// Linux opens ten outgoing streams.
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use oxirush_sctp::{InitParams, Socket, SocketToAssociation};
    ///
    /// // Four streams each way, as an NGAP node with three streams for its UEs asks
    /// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// socket.options().set_init_params(InitParams::streams(4, 4))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_init_params(&self, params: InitParams) -> std::io::Result<()> {
        sys::sctp_setup_init_params_internal(self.core.as_fd(), params)
    }

    /// What the associations that start from now on start with.
    pub fn init_params(&self) -> std::io::Result<InitParams> {
        sys::sctp_init_params_internal(self.core.as_fd())
    }

    /// Enable receive metadata: the stream, payload protocol and association of each message.
    /// Enabled by default. On a [`OneToManyEndpoint`][crate::OneToManyEndpoint], disabling it
    /// loses the association's identity, and the parts of the messages of several
    /// associations, which the endpoint can then not tell apart, come one message after the
    /// other (`SCTP_FRAGMENT_INTERLEAVE` 0): a long message holds back the others. It is
    /// `InvalidInput` to disable it while the endpoint assembles messages, whose parts it
    /// could then not tell apart: do it before the endpoint has associations.
    pub fn request_rcvinfo(&self, on: bool) -> std::io::Result<()> {
        self.core.request_rcvinfo(on)
    }

    /// Enable metadata describing the next queued message (`SCTP_NXTINFO`).
    pub fn request_nxtinfo(&self, on: bool) -> std::io::Result<()> {
        sys::request_nxtinfo_internal(self.core.as_fd(), on)
    }

    /// Set what a message is sent with when a send supplies no `SendInfo`.
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
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// # use oxirush_sctp::{Socket, SocketToAssociation};
    /// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # socket.bind("127.0.0.1:0".parse().unwrap())?;
    /// # let listener = socket.listen(5)?;
    /// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # let (client, id) = client.connect(listener.local_addr()?).await?;
    /// use std::time::Duration;
    ///
    /// // A heartbeat on the path to the peer each second, where Linux sends one each 30
    /// let address = client.peer_addrs()?[0];
    /// let mut path = client.options().peer_address_params(id, address)?;
    /// path.heartbeat_interval = Duration::from_secs(1);
    /// client.options().set_peer_address_params(path)?;
    /// let path = client.options().peer_address_params(id, address)?;
    /// assert_eq!(path.heartbeat_interval, Duration::from_secs(1));
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_peer_address_params(&self, params: PeerAddressParams) -> std::io::Result<()> {
        sys::set_peer_address_params_internal(self.core.as_fd(), params)
    }

    /// The state, the congestion window, the smoothed round trip, the retransmission timeout
    /// and the MTU of the path to `address`, one of the peer's addresses of association
    /// `assoc_id` (`SCTP_GET_PEER_ADDR_INFO`); a one-to-one socket ignores the association.
    /// [`ConnectedSocket::paths`][crate::ConnectedSocket::paths] reads every path.
    pub fn peer_address_info(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<PeerAddress> {
        sys::peer_address_info_internal(self.core.as_fd(), assoc_id, address)
    }

    /// Request an immediate heartbeat on a peer path.
    pub fn request_heartbeat(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<()> {
        sys::request_heartbeat_internal(self.core.as_fd(), assoc_id, address)
    }

    /// The parameters of an association, or the defaults of the socket when the ID is zero
    /// (`SCTP_ASSOCINFO`): the retransmissions after which an association is lost, and the
    /// life of a cookie.
    pub fn association_params(
        &self,
        assoc_id: AssociationId,
    ) -> std::io::Result<AssociationParams> {
        sys::association_params_internal(self.core.as_fd(), assoc_id)
    }

    /// Change the retransmission limit of an association and the life of its cookie. A zero
    /// leaves a value as it is. Linux refuses a limit above the sum of the limits of the
    /// paths ([`PeerAddressParams::path_max_retrans`]).
    pub fn set_association_params(&self, params: AssociationParams) -> std::io::Result<()> {
        sys::set_association_params_internal(self.core.as_fd(), &params)
    }

    /// How long an acknowledgement may wait (`SCTP_DELAYED_SACK`).
    pub fn sack_info(&self, assoc_id: AssociationId) -> std::io::Result<SackInfo> {
        sys::sack_info_internal(self.core.as_fd(), assoc_id)
    }

    /// Change how long an acknowledgement may wait, in milliseconds, and after how many
    /// packets one is sent at once. A zero leaves a value as it is, and a frequency of 1
    /// turns the delay off.
    pub fn set_sack_info(&self, info: SackInfo) -> std::io::Result<()> {
        sys::set_sack_info_internal(self.core.as_fd(), &info)
    }

    /// Send to this address of the peer from now on (`SCTP_PRIMARY_ADDR`). It is one of
    /// [`peer_addrs`][crate::ConnectedSocket::peer_addrs]; a one-to-one socket ignores the
    /// association ID.
    pub fn set_primary_address(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<()> {
        sys::set_primary_address_internal(self.core.as_fd(), assoc_id, address)
    }

    /// Mark what the socket sends with a DSCP, 0 to 63, for IPv4 and for IPv6: 46 is
    /// expedited forwarding. Set it before `connect` or `listen` for the first packets to
    /// have it.
    pub fn set_dscp(&self, dscp: u8) -> std::io::Result<()> {
        sys::set_dscp_internal(self.core.as_fd(), dscp)
    }

    /// The DSCP that the socket sends with.
    pub fn dscp(&self) -> std::io::Result<u8> {
        sys::dscp_internal(self.core.as_fd())
    }

    /// Ask for a send buffer of this many octets (`SO_SNDBUF`). Linux gives twice the
    /// number, up to `net.core.wmem_max`, and [`send_buffer_size`][Self::send_buffer_size]
    /// reports what it gave. A message must fit in it.
    pub fn set_send_buffer_size(&self, octets: usize) -> std::io::Result<()> {
        sys::set_buffer_size_internal(self.core.as_fd(), libc::SO_SNDBUF, octets)
    }

    /// The size of the send buffer, in octets.
    pub fn send_buffer_size(&self) -> std::io::Result<usize> {
        sys::buffer_size_internal(self.core.as_fd(), libc::SO_SNDBUF)
    }

    /// Ask for a receive buffer of this many octets (`SO_RCVBUF`). Linux gives twice the
    /// number, up to `net.core.rmem_max`.
    pub fn set_recv_buffer_size(&self, octets: usize) -> std::io::Result<()> {
        sys::set_buffer_size_internal(self.core.as_fd(), libc::SO_RCVBUF, octets)
    }

    /// The size of the receive buffer, in octets.
    pub fn recv_buffer_size(&self) -> std::io::Result<usize> {
        sys::buffer_size_internal(self.core.as_fd(), libc::SO_RCVBUF)
    }

    /// Set the largest complete message returned by `recv` (4 MiB by default). Longer records
    /// are drained and reported as `InvalidData`; the next call starts at the next record. A
    /// notification is never discarded.
    pub fn set_max_message_size(&self, octets: usize) {
        self.core.set_max_message_size(octets);
    }

    /// The current complete-message limit.
    pub fn max_message_size(&self) -> usize {
        self.core.max_message_size()
    }

    /// Set the most octets that a [`OneToManyEndpoint`][crate::OneToManyEndpoint] holds of
    /// the messages that it assembles, over all its associations: 64 MiB by default, sixteen
    /// messages of the default [`max_message_size`][Self::max_message_size]. Each
    /// association has one message in assembly at most, and past this limit the endpoint
    /// aborts the association whose message is the longest, drops what it has of it, and
    /// `recv` reports it once as `InvalidData`. A one-to-one socket has one message in
    /// assembly, which `max_message_size` bounds.
    pub fn set_max_assembly(&self, octets: usize) {
        self.core.set_max_assembly(octets);
    }

    /// The limit of the octets of the messages in assembly.
    pub fn max_assembly(&self) -> usize {
        self.core.max_assembly()
    }
}
