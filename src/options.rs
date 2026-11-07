//! Borrowed configuration shared by every SCTP socket role.
use crate::consts::*;
use crate::internal::SocketCore;
use crate::internal::sys::{self, SOL_SCTP};
use crate::types::internal::{
    AssocParamsWire, AssocStatsWire, InitMsg, PeerAddrInternal, PeerAddressParamsInternal,
    PrimaryAddressWire, RtoInfoWire, SackInfoWire,
};
use crate::{
    AssociationId, AssociationParams, AssociationStats, Event, InitParams, PeerAddress,
    PeerAddressParams, RtoInfo, SackInfo,
};
use os_socketaddr::OsSocketAddr;
use std::net::SocketAddr;
use std::os::fd::{AsFd, BorrowedFd};

/// A configuration view borrowing its socket, listener, association or endpoint.
///
/// Kernel options and the receive limit belong to the underlying socket. Moving a socket into
/// a listener, association or endpoint preserves them; accepted and peeled-off associations
/// inherit the receive limit. Association IDs are ignored by Linux on one-to-one sockets.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use oxirush_sctp::{Event, InitParams, Socket, SocketToAssociation};
///
/// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// let options = socket.options();
/// options.set_nodelay(true)?;
/// options.set_init_params(InitParams::streams(4, 4))?; // four streams each way
/// options.subscribe_events(
///     &[Event::Association, Event::Shutdown],
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
    /// Subscribe to events, on every association of the socket and on those to come: the
    /// associations that a listener accepts inherit its subscriptions. The first event that
    /// the kernel refuses is the error, which names it.
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use oxirush_sctp::{Event, Socket, SocketToAssociation};
    ///
    /// // Before listening: the accepted associations inherit it
    /// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// let events = [Event::Association, Event::Shutdown];
    /// socket.options().subscribe_events(&events)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe_events(&self, events: &[Event]) -> std::io::Result<()> {
        self.core.subscribe_events(events, true)
    }

    /// Unsubscribe from events, as [`subscribe_events`][Self::subscribe_events] subscribes.
    pub fn unsubscribe_events(&self, events: &[Event]) -> std::io::Result<()> {
        self.core.subscribe_events(events, false)
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
        let init = InitMsg {
            ostreams: params.outbound_streams,
            istreams: params.inbound_streams,
            retries: params.max_attempts,
            timeout: params.max_init_timeout,
        };
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_INITMSG, &init)
    }

    /// What the associations that start from now on start with.
    pub fn init_params(&self) -> std::io::Result<InitParams> {
        let empty = InitMsg {
            ostreams: 0,
            istreams: 0,
            retries: 0,
            timeout: 0,
        };
        // Safety: `InitMsg` holds integers only, so any bytes are a valid value.
        let init = unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_INITMSG, empty)? };
        Ok(InitParams {
            outbound_streams: init.ostreams,
            inbound_streams: init.istreams,
            max_attempts: init.retries,
            max_init_timeout: init.timeout,
        })
    }

    /// Send small messages as soon as congestion permits, without Nagle-like bundling delays.
    /// Linux accepted and peeled-off sockets inherit this setting.
    pub fn set_nodelay(&self, on: bool) -> std::io::Result<()> {
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_NODELAY, &libc::c_int::from(on))
    }

    /// Whether `SCTP_NODELAY` is enabled.
    pub fn nodelay(&self) -> std::io::Result<bool> {
        // Safety: every value is a valid `c_int`.
        let nodelay: libc::c_int =
            unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_NODELAY, 0)? };
        Ok(nodelay != 0)
    }

    /// Choose graceful close (`None`) or abort on close (`Some(Duration::ZERO)`). Positive
    /// durations are rejected because Linux can block close even on a nonblocking socket.
    pub fn set_linger(&self, linger: Option<std::time::Duration>) -> std::io::Result<()> {
        if linger.is_some_and(|duration| !duration.is_zero()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "positive linger can block the async runtime when the socket is dropped",
            ));
        }
        let value = libc::linger {
            l_onoff: libc::c_int::from(linger.is_some()),
            l_linger: 0,
        };
        sys::setsockopt(self.fd(), libc::SOL_SOCKET, libc::SO_LINGER, &value)
    }

    /// Set association retransmission timeouts, or socket defaults when the ID is zero.
    pub fn set_rto_info(&self, info: RtoInfo) -> std::io::Result<()> {
        let wire = RtoInfoWire {
            assoc_id: info.assoc_id,
            initial: info.initial,
            max: info.max,
            min: info.min,
        };
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_RTOINFO, &wire)
    }

    /// Query association retransmission timeouts, or socket defaults when the ID is zero.
    pub fn rto_info(&self, assoc_id: AssociationId) -> std::io::Result<RtoInfo> {
        let wire = RtoInfoWire {
            assoc_id,
            ..Default::default()
        };
        // Safety: the structure holds integers only, so any bytes are a valid value.
        let wire = unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_RTOINFO, wire)? };
        Ok(RtoInfo {
            assoc_id: wire.assoc_id,
            initial: wire.initial,
            max: wire.max,
            min: wire.min,
        })
    }

    /// Permit rebinding local addresses when Linux SCTP socket rules allow it.
    pub fn set_reuseaddr(&self, on: bool) -> std::io::Result<()> {
        let on = libc::c_int::from(on);
        sys::setsockopt(self.fd(), libc::SOL_SOCKET, libc::SO_REUSEADDR, &on)
    }

    /// Whether `SO_REUSEADDR` is enabled.
    pub fn reuseaddr(&self) -> std::io::Result<bool> {
        // Safety: every value is a valid `c_int`.
        let on: libc::c_int =
            unsafe { sys::getsockopt(self.fd(), libc::SOL_SOCKET, libc::SO_REUSEADDR, 0)? };
        Ok(on != 0)
    }

    /// Query heartbeat and retransmission settings for a peer path.
    pub fn peer_address_params(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<PeerAddressParams> {
        let value = path_params(assoc_id, address);
        // Safety: every byte pattern is valid for the byte buffer.
        let value = unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_PEER_ADDR_PARAMS, value)? };
        let bytes = value.bytes;
        Ok(PeerAddressParams {
            assoc_id,
            address,
            heartbeat_enabled: u32::from_ne_bytes(bytes[146..150].try_into().unwrap()) & 1 != 0,
            heartbeat_interval: std::time::Duration::from_millis(u32::from_ne_bytes(
                bytes[132..136].try_into().unwrap(),
            ) as u64),
            path_max_retrans: u16::from_ne_bytes(bytes[136..138].try_into().unwrap()),
        })
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
    /// # let client = client.connect(listener.local_addr()?).await?;
    /// # let id = client.status()?.assoc_id;
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
        let millis = params.heartbeat_interval.as_millis();
        if !params
            .heartbeat_interval
            .subsec_nanos()
            .is_multiple_of(1_000_000)
            || millis > u32::MAX as u128
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "heartbeat interval must fit whole u32 milliseconds",
            ));
        }
        let mut value = path_params(params.assoc_id, params.address);
        value.bytes[132..136].copy_from_slice(&(millis as u32).to_ne_bytes());
        value.bytes[136..138].copy_from_slice(&params.path_max_retrans.to_ne_bytes());
        // SPP_HB_ENABLE or SPP_HB_DISABLE, and SPP_HB_TIME_IS_ZERO for an interval of 0
        let flags: u32 =
            if params.heartbeat_enabled { 1 } else { 2 } | if millis == 0 { 1 << 7 } else { 0 };
        value.bytes[146..150].copy_from_slice(&flags.to_ne_bytes());
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_PEER_ADDR_PARAMS, &value)
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
        let address = OsSocketAddr::from(address);
        let address = address.as_ref();
        // Safety: `PeerAddrInternal` holds integers only, so any bytes are a valid value, and
        // the address fits in its storage.
        let value = unsafe {
            let mut value = std::mem::MaybeUninit::<PeerAddrInternal>::zeroed().assume_init();
            value.assoc_id = assoc_id;
            std::ptr::copy_nonoverlapping(
                address.as_ptr(),
                std::ptr::addr_of_mut!(value.address).cast::<u8>(),
                address.len(),
            );
            sys::getsockopt(self.fd(), SOL_SCTP, SCTP_GET_PEER_ADDR_INFO, value)?
        };
        value.try_into()
    }

    /// Request an immediate heartbeat on a peer path.
    pub fn request_heartbeat(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<()> {
        let mut value = path_params(assoc_id, address);
        // SPP_HB_DEMAND
        value.bytes[146..150].copy_from_slice(&(1_u32 << 2).to_ne_bytes());
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_PEER_ADDR_PARAMS, &value)
    }

    /// The parameters of an association, or the defaults of the socket when the ID is zero
    /// (`SCTP_ASSOCINFO`): the retransmissions after which an association is lost, and the
    /// life of a cookie.
    pub fn association_params(
        &self,
        assoc_id: AssociationId,
    ) -> std::io::Result<AssociationParams> {
        let wire = AssocParamsWire {
            assoc_id,
            ..Default::default()
        };
        // Safety: the structure holds integers only, so any bytes are a valid value.
        let wire = unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_ASSOCINFO, wire)? };
        Ok(AssociationParams {
            assoc_id: wire.assoc_id,
            max_retransmissions: wire.max_retransmissions,
            peer_addresses: wire.peer_addresses,
            peer_rwnd: wire.peer_rwnd,
            local_rwnd: wire.local_rwnd,
            cookie_life: wire.cookie_life,
        })
    }

    /// Read the statistics of an association (`SCTP_GET_ASSOC_STATS`): the
    /// chunks it retransmitted, the gaps and duplicates it saw, the packets
    /// and SACKs it exchanged, and its highest retransmission timeout. For
    /// the association of a One-to-one socket the ID is ignored.
    pub fn association_stats(&self, assoc_id: AssociationId) -> std::io::Result<AssociationStats> {
        let wire = AssocStatsWire {
            assoc_id,
            ..Default::default()
        };
        // Safety: the structure holds integers and opaque octets, so any bytes are a valid
        // value.
        let wire = unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_GET_ASSOC_STATS, wire)? };
        Ok(AssociationStats {
            assoc_id: wire.assoc_id,
            max_rto: wire.max_rto,
            rtx_chunks: wire.rtx_chunks,
            gap_count: wire.gap_count,
            out_of_sequence_tsns: wire.out_of_sequence_tsns,
            in_dup_chunks: wire.in_dup_chunks,
            out_packets: wire.out_packets,
            in_packets: wire.in_packets,
            out_sacks: wire.out_sacks,
            in_sacks: wire.in_sacks,
        })
    }

    /// Change the retransmission limit of an association and the life of its cookie. A zero
    /// leaves a value as it is. Linux refuses a limit above the sum of the limits of the
    /// paths ([`PeerAddressParams::path_max_retrans`]).
    pub fn set_association_params(&self, params: AssociationParams) -> std::io::Result<()> {
        // The numbers that Linux only reports are zero: it leaves them as they are
        let wire = AssocParamsWire {
            assoc_id: params.assoc_id,
            max_retransmissions: params.max_retransmissions,
            cookie_life: params.cookie_life,
            ..Default::default()
        };
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_ASSOCINFO, &wire)
    }

    /// How long an acknowledgement may wait (`SCTP_DELAYED_SACK`).
    pub fn sack_info(&self, assoc_id: AssociationId) -> std::io::Result<SackInfo> {
        let wire = SackInfoWire {
            assoc_id,
            ..Default::default()
        };
        // Safety: the structure holds integers only, so any bytes are a valid value.
        let wire = unsafe { sys::getsockopt(self.fd(), SOL_SCTP, SCTP_DELAYED_SACK, wire)? };
        Ok(SackInfo {
            assoc_id: wire.assoc_id,
            delay: wire.delay,
            frequency: wire.frequency,
        })
    }

    /// Change how long an acknowledgement may wait, in milliseconds, and after how many
    /// packets one is sent at once. A zero leaves a value as it is, and a frequency of 1
    /// turns the delay off.
    pub fn set_sack_info(&self, info: SackInfo) -> std::io::Result<()> {
        let wire = SackInfoWire {
            assoc_id: info.assoc_id,
            delay: info.delay,
            frequency: info.frequency,
        };
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_DELAYED_SACK, &wire)
    }

    /// Send to this address of the peer from now on (`SCTP_PRIMARY_ADDR`). It is one of
    /// [`peer_addrs`][crate::ConnectedSocket::peer_addrs]; a one-to-one socket ignores the
    /// association ID.
    pub fn set_primary_address(
        &self,
        assoc_id: AssociationId,
        address: SocketAddr,
    ) -> std::io::Result<()> {
        // `struct sctp_prim`: the association, then the address
        let mut wire = PrimaryAddressWire { bytes: [0; 132] };
        wire.bytes[..4].copy_from_slice(&assoc_id.to_ne_bytes());
        let address = OsSocketAddr::from(address);
        let address = address.as_ref();
        wire.bytes[4..4 + address.len()].copy_from_slice(address);
        sys::setsockopt(self.fd(), SOL_SCTP, SCTP_PRIMARY_ADDR, &wire)
    }

    /// Mark what the socket sends with a DSCP, 0 to 63, for IPv4 and for IPv6: 46 is
    /// expedited forwarding. Set it before `connect` or `listen` for the first packets to
    /// have it.
    pub fn set_dscp(&self, dscp: u8) -> std::io::Result<()> {
        if dscp > 63 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("a DSCP is 0 to 63, and this one is {dscp}"),
            ));
        }
        // The six high bits of the traffic class of IPv4 and of IPv6. A socket of IPv6 can
        // carry IPv4 as well, and is given both.
        let class = libc::c_int::from(dscp) << 2;
        if sys::socket_domain(self.fd())? == libc::AF_INET6 {
            sys::setsockopt(self.fd(), libc::IPPROTO_IPV6, libc::IPV6_TCLASS, &class)?;
        }
        sys::setsockopt(self.fd(), libc::IPPROTO_IP, libc::IP_TOS, &class)
    }

    /// The DSCP that the socket sends with.
    pub fn dscp(&self) -> std::io::Result<u8> {
        let (level, name) = if sys::socket_domain(self.fd())? == libc::AF_INET6 {
            (libc::IPPROTO_IPV6, libc::IPV6_TCLASS)
        } else {
            (libc::IPPROTO_IP, libc::IP_TOS)
        };
        // Safety: every value is a valid `c_int`.
        let class: libc::c_int = unsafe { sys::getsockopt(self.fd(), level, name, 0)? };
        Ok((class >> 2) as u8 & 0x3f)
    }

    /// Ask for a send buffer of this many octets (`SO_SNDBUF`). Linux gives twice the
    /// number, up to `net.core.wmem_max`, and [`send_buffer_size`][Self::send_buffer_size]
    /// reports what it gave. A message must fit in it.
    pub fn set_send_buffer_size(&self, octets: usize) -> std::io::Result<()> {
        self.set_buffer_size(libc::SO_SNDBUF, octets)
    }

    /// The size of the send buffer, in octets.
    pub fn send_buffer_size(&self) -> std::io::Result<usize> {
        self.buffer_size(libc::SO_SNDBUF)
    }

    /// Ask for a receive buffer of this many octets (`SO_RCVBUF`). Linux gives twice the
    /// number, up to `net.core.rmem_max`.
    pub fn set_recv_buffer_size(&self, octets: usize) -> std::io::Result<()> {
        self.set_buffer_size(libc::SO_RCVBUF, octets)
    }

    /// The size of the receive buffer, in octets.
    pub fn recv_buffer_size(&self) -> std::io::Result<usize> {
        self.buffer_size(libc::SO_RCVBUF)
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

    fn fd(&self) -> BorrowedFd<'_> {
        self.core.as_fd()
    }

    // The size of a buffer of the socket, in octets. Linux doubles the number that it is
    // given, for its own bookkeeping, and reports the double.
    fn set_buffer_size(&self, name: libc::c_int, octets: usize) -> std::io::Result<()> {
        let octets = libc::c_int::try_from(octets).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("a buffer of {octets} octets is more than the system takes"),
            )
        })?;
        sys::setsockopt(self.fd(), libc::SOL_SOCKET, name, &octets)
    }

    fn buffer_size(&self, name: libc::c_int) -> std::io::Result<usize> {
        // Safety: every value is a valid `c_int`.
        let octets: libc::c_int = unsafe { sys::getsockopt(self.fd(), libc::SOL_SOCKET, name, 0)? };
        Ok(octets.max(0) as usize)
    }
}

// `struct sctp_paddrparams` for a path, as bytes: Linux packs it and aligns it to 4.
fn path_params(assoc_id: AssociationId, address: SocketAddr) -> PeerAddressParamsInternal {
    let mut value = PeerAddressParamsInternal { bytes: [0; 156] };
    value.bytes[..4].copy_from_slice(&assoc_id.to_ne_bytes());
    let address = OsSocketAddr::from(address);
    let address = address.as_ref();
    value.bytes[4..4 + address.len()].copy_from_slice(address);
    value
}
