//! Async SCTP sockets for [Tokio](https://tokio.rs) on Linux, over the SCTP
//! of the kernel and its sockets API
//! ([RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.html)). `send` and
//! `recv` move one whole message, with its stream and its payload protocol
//! identifier.
//!
//! ```rust
//! use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> std::io::Result<()> {
//!     // Stream 1, and the payload protocol identifier of NGAP
//!     let options = SendOptions::new().stream(1).ppid(60);
//!
//!     let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
//!     socket.bind("127.0.0.1:0".parse().unwrap())?;
//!     let listener = socket.listen(5)?;
//!     let address = listener.local_addr()?;
//!
//!     let server = async {
//!         let (association, _peer) = listener.accept().await?;
//!         if let Some(NotificationOrData::Data(message)) = association.recv().await? {
//!             let text = String::from_utf8_lossy(&message.payload);
//!             let (stream, ppid) = (message.stream_id(), message.ppid());
//!             println!("server: {text:?} on stream {stream:?} with PPID {ppid:?}");
//!             association.send(b"pong", options).await?;
//!         }
//!         std::io::Result::Ok(())
//!     };
//!     let client = async {
//!         let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
//!         let (association, _) = socket.connect(address).await?;
//!         association.send(b"ping", options).await?;
//!         if let Some(NotificationOrData::Data(message)) = association.recv().await? {
//!             println!("client: {:?}", String::from_utf8_lossy(&message.payload));
//!         }
//!         std::io::Result::Ok(())
//!     };
//!     let (server, client) = tokio::join!(server, client);
//!     server.and(client)
//! }
//! ```
//!
//! ```text
//! server: "ping" on stream Some(1) with PPID Some(60)
//! client: "pong"
//! ```
//!
//! The [README](https://github.com/linouxis9/oxirush-sctp#readme) has a
//! program for each task: notifications, the end of an association,
//! multihoming, options and status, a one-to-many socket.
//!
//! # Requirements
//!
//! - Linux 5.0 or later with SCTP. `sudo modprobe sctp` loads the kernel
//!   module, which some distributions ship in a separate package of extra
//!   kernel modules. Without it, creating a socket fails with
//!   `EPROTONOSUPPORT`, or `ESOCKTNOSUPPORT` for a one-to-many socket.
//! - A Tokio runtime with the I/O driver (`#[tokio::main]`, or `enable_io` on
//!   a runtime builder). Sockets register with it when they are created, and
//!   panic without it.
//! - Rust 1.88 or later.
//!
//! The crate makes the system calls itself, without `libsctp`. Diagnostics go
//! through [`tracing`](https://crates.io/crates/tracing).
//!
//! # Socket types
//!
//! | Type | Comes from | Does |
//! | --- | --- | --- |
//! | [`Socket`] | `Socket::new_v4`, `Socket::new_v6` | binds, then becomes one of the next three |
//! | [`Listener`] | `Socket::listen` | accepts associations |
//! | [`ConnectedSocket`] | `Socket::connect`, `Socket::connectx`, `Listener::accept`, `OneToManyEndpoint::peeloff` | sends and receives on one association |
//! | [`OneToManyEndpoint`] | `Socket::into_endpoint` | sends and receives on many associations |
//! | [`SocketOptions`] | `options()` on the four above | reads and sets options |
//!
//! Every socket type has `options()`. What is set before `listen` or `connect`
//! stays, and accepted associations inherit it: subscribe to events before
//! listening.
//!
//! On a one-to-many socket, `connect` starts an association without waiting
//! for it: subscribe to `Event::Association` to learn whether it came up.
//! `send_to` sends to an address, and starts an association if there is none.
//! `peeloff` moves one association to a [`ConnectedSocket`] of its own.
//!
//! `bindx` binds several local addresses and `connectx` gives several
//! addresses of the peer. The addresses of one call share a port, which the
//! kernel chooses for port 0. Subscribe to `Event::Address` to receive a
//! `Notification::PeerAddressChange` when a path changes state.
//! `options().peer_address_params`, `set_peer_address_params` and
//! `request_heartbeat` read and set the heartbeat and the retransmission limit
//! of a path.
//!
//! # Messages
//!
//! - `recv` returns a whole message, however many reads it takes. A dropped
//!   `recv` future loses nothing: the next call continues the message.
//! - A received message longer than `options().max_message_size()`, 4 MiB by
//!   default, is discarded and reported as an `InvalidData` error. The next
//!   `recv` returns the message after it. A notification is never discarded.
//! - `recv_into` receives into a buffer of the caller, without an allocation
//!   when one read gives the message whole.
//! - `poll_recv`, `poll_recv_into`, `poll_send`, `poll_send_to` and
//!   `poll_accept` are the calls as polls, for a `Stream` or a future of a
//!   program's own: they wait for the socket to be readable or writable, and end
//!   and fail as the async calls do. A one-to-one socket subscribes to its
//!   association changes for itself, so that the end of its association makes
//!   it readable, and passes them on only to a caller that subscribes to them.
//! - `send` waits for room in the send buffer. A dropped `send` future has
//!   sent nothing.
//! - A message shorter than a packet, sent while the one before it is not
//!   acknowledged, waits for that acknowledgement, which the peer may delay by
//!   200 ms: SCTP gathers small messages as TCP does. With
//!   `options().set_nodelay(true)` it leaves at once. The benchmark took
//!   200 ms a message without it, and 9 µs with it.
//! - A received message holds the memory of its octets and no more.
//! - A sent message must fit in the send buffer (`SO_SNDBUF`): a longer one
//!   fails with `EMSGSIZE`. An empty one is `InvalidInput`, as is a stream
//!   that the association does not have: the error says which.
//! - `ReceivedData::stream_id`, `ppid` and `assoc_id` read the receive
//!   information (`SCTP_RCVINFO`) that every socket requests. They return
//!   `None` after `options().request_rcvinfo(false)`.
//!
//! # The end of an association
//!
//! On a [`ConnectedSocket`], `recv` returns `None` for an association that was
//! shut down and an error for one that was aborted or lost:
//!
//! ```no_run
//! # async fn serve(association: oxirush_sctp::ConnectedSocket) -> std::io::Result<()> {
//! while let Some(received) = association.recv().await? {
//!     println!("{received:?}");
//! }
//! // The peer has closed, or this side has shut the association down
//! # Ok(())
//! # }
//! ```
//!
//! Linux reports the end once, to the call that happens to be there. The crate
//! records it wherever it learns of it, in `recv`, in `send` or in a
//! notification, and every later call returns it at once: none waits.
//!
//! | What happened | `recv` returns, at each call | `send` returns, at each call | Notifications, when subscribed |
//! | --- | --- | --- | --- |
//! | The peer closed its socket or shut the association down | what the peer had sent, then `None` | `BrokenPipe` | `Shutdown`, then `AssociationChange` with `ShutdownComplete` |
//! | The peer aborted, or was dropped with a message that it had not read | `ConnectionReset` | `ConnectionReset` | `AssociationChange` with `CommLost`, before the error |
//! | The peer was lost: nothing was acknowledged up to the retransmission limit | `TimedOut` | `TimedOut` | `AssociationChange` with `CommLost`, before the error |
//! | This side called `shutdown(Shutdown::Write)` | what the peer had still sent, then `None` once the peer has answered | `BrokenPipe`, from the call on | `AssociationChange` with `ShutdownComplete` |
//! | This side called `shutdown(Shutdown::Read)` | `None`, from the call on | sends: the association lives | none |
//! | This side called `shutdown(Shutdown::Both)` | `None`, from the call on | `BrokenPipe`, from the call on | `AssociationChange` with `ShutdownComplete` |
//! | `connect` failed | nothing: `connect` returns the error, such as `ConnectionRefused` or `TimedOut`, and has consumed the socket | | |
//!
//! Each row is the same whichever call learns of the end first: a `send` that
//! takes the error of an abort leaves it for `recv`, which first returns what
//! the kernel still holds of the association. A [`OneToManyEndpoint`] learns of
//! the end of an association from the notifications only.
//!
//! `close(deadline)` on a [`ConnectedSocket`] loses nothing that was sent: it
//! shuts the association down for writing, reads until the peer has answered
//! and returns the messages that the peer had still sent. A dropped socket is
//! closed by Linux, which shuts the association down in the background when
//! everything that it received was read, and aborts it when a message or a
//! notification was not: what this side had sent and the peer had not
//! acknowledged is then lost. With `options().set_linger(Some(Duration::ZERO))`
//! a drop always aborts, as `abort` does at once. Other durations are refused,
//! because closing would then block the runtime.
//!
//! # Payload protocol identifiers
//!
//! A payload protocol identifier is a plain number in every call and in every
//! value: 60 is NGAP, in `SendOptions::ppid`, `SendInfo::ppid`,
//! `ReceivedData::ppid`, `RcvInfo::ppid` and `NxtInfo::ppid`. The crate converts
//! it to the network byte order that the kernel carries.
//!
//! # Addresses
//!
//! A socket that connects without being bound offers the peer every address
//! of the host, loopback and the others: the peer may then send to any of
//! them. `bind` or `bindx` before `connect` to offer what was chosen:
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> std::io::Result<()> {
//! use oxirush_sctp::{Socket, SocketToAssociation};
//! # let listener = Socket::new_v4(SocketToAssociation::OneToOne)?;
//! # listener.bind("127.0.0.1:0".parse().unwrap())?;
//! # let listener = listener.listen(1)?;
//! # let server = listener.local_addr()?;
//!
//! let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
//! client.bind("127.0.0.1:0".parse().unwrap())?;
//! let (association, _) = client.connect(server).await?;
//! # let (accepted, _) = listener.accept().await?;
//! // The peer sees the one address that was bound
//! assert_eq!(accepted.peer_addrs()?.len(), 1);
//! # let _ = association;
//! # Ok(())
//! # }
//! ```
//!
//! # What it covers
//!
//! Sockets of both kinds, with the options of RFC 6458 that the table below
//! names: each row is a section of the RFC and what gives it here.
//!
//! | RFC 6458 | Provided by |
//! | --- | --- |
//! | 3.1, 4.1: socket, bind, listen, accept, connect, send, receive, shutdown, close | the socket types; a dropped socket is closed |
//! | 5.3.4 `SCTP_SNDINFO` | `SendOptions`, `SendInfo` |
//! | 5.3.5 `SCTP_RCVINFO`, 5.3.6 `SCTP_NXTINFO` | `ReceivedData::rcv_info`, `nxt_info` |
//! | 6.1.1 `SCTP_ASSOC_CHANGE` | `Notification::AssociationChange` |
//! | 6.1.2 `SCTP_PEER_ADDR_CHANGE` | `Notification::PeerAddressChange` |
//! | 6.1.5 `SCTP_SHUTDOWN_EVENT` | `Notification::Shutdown` |
//! | 6.1.11 `SCTP_SEND_FAILED_EVENT` | `Notification::SendFailure` |
//! | 6.1: the other notifications | `Notification::Unsupported`, with the number of their type and their octets |
//! | 6.1.3 `SCTP_REMOTE_ERROR` | `Notification::RemoteError` |
//! | 6.1.7 `SCTP_PARTIAL_DELIVERY_EVENT` | `Notification::PartialDelivery` |
//! | 6.1.9 `SCTP_SENDER_DRY_EVENT` | `Notification::SenderDry` |
//! | 8.1.1 `SCTP_RTOINFO` | `set_rto_info`, `rto_info` |
//! | 8.1.2 `SCTP_ASSOCINFO` | `set_association_params`, `association_params`: the retransmission limit of an association |
//! | 8.1.3 `SCTP_INITMSG` | `set_init_params` |
//! | 8.1.4 `SO_LINGER` | `set_linger`, off or zero |
//! | 8.1.5 `SCTP_NODELAY` | `set_nodelay`, `nodelay` |
//! | 8.1.9 `SCTP_PRIMARY_ADDR` | `set_primary_address` |
//! | 8.1.19 `SCTP_DELAYED_SACK` | `set_sack_info`, `sack_info` |
//! | `SO_SNDBUF`, `SO_RCVBUF`, `IP_TOS`, `IPV6_TCLASS` | `set_send_buffer_size`, `set_recv_buffer_size`, `set_dscp` and their getters |
//! | 8.1.12 `SCTP_PEER_ADDR_PARAMS` | `peer_address_params`, `set_peer_address_params`, `request_heartbeat`: heartbeat and retransmission limit |
//! | 8.1.28 `SCTP_EVENT` | `subscribe_events`, `unsubscribe_events` |
//! | 8.1.29 `SCTP_RECVRCVINFO`, 8.1.30 `SCTP_RECVNXTINFO` | `request_rcvinfo`, `request_nxtinfo` |
//! | 8.1.31 `SCTP_DEFAULT_SNDINFO` | `set_default_sendinfo` |
//! | 8.2.1 `SCTP_STATUS` | `status` |
//! | 9.1 `sctp_bindx`, 9.9 `sctp_connectx` | `bindx`, `connectx` |
//! | 9.2 `sctp_peeloff` | `OneToManyEndpoint::peeloff` |
//! | 9.3 `sctp_getpaddrs`, 9.5 `sctp_getladdrs` | `peer_addrs`, `local_addrs` |
//!
//! # Limits
//!
//! | Subject | Limit |
//! | --- | --- |
//! | SCTP itself | The protocol runs in the Linux kernel: the crate is not an SCTP implementation. |
//! | Other systems | Linux only. |
//! | SCTP over DTLS | The data channels of WebRTC are another stack: not served. |
//! | Many associations on one socket | On a [`OneToManyEndpoint`], the parts of the messages of several associations come in turns (`SCTP_FRAGMENT_INTERLEAVE` 1) and are assembled apart: a long message of one association holds back none of the others. Each association has one message in assembly at most, of `max_message_size()` at most, until it ends. The associations share the receive buffer of the socket (Linux's `net.sctp.rcvbuf_policy` 0): one that fills it makes the others wait for room, and their peers send again after a time-out; `net.sctp.rcvbuf_policy` 1, or a [`Listener`], gives each its own. After `request_rcvinfo(false)` the parts come one message after the other, and a long message holds back the others. |
//! | The rest of RFC 6458 | Not wrapped: among others authentication, partial reliability and stream reconfiguration. The sockets implement `AsFd` and `AsRawFd` for them. The interfaces that the RFC deprecates are left out on purpose. |
//!
//! # Terms
//!
//! | Term | Meaning |
//! | --- | --- |
//! | SCTP | Stream Control Transmission Protocol (RFC 9260): a transport that carries whole messages, in order within a stream, between two endpoints that may each have several addresses |
//! | Association | What SCTP has in the place of a TCP connection |
//! | Stream | One of the numbered sequences of messages of an association. Messages of two streams do not wait for each other |
//! | PPID | Payload protocol identifier: a number that travels with each message and says what it carries. 60 is NGAP and 18 is S1AP |
//! | One-to-one socket | A socket with one association, which is used like a TCP socket |
//! | One-to-many socket | A socket with many associations, which is used like a UDP socket |
//! | Multihoming | An association between several addresses of each side: when a path fails, another carries it |
//! | Notification | A message of the kernel about an association, which `recv` returns in the place of data when it is subscribed |
//!
//! # Tests
//!
//! The tests open SCTP associations over loopback, so the kernel's `sctp`
//! module must be available. The tests of a connection that times out and
//! of a peer that is lost need `unshare` and `ip`, and the tests of long
//! messages need `net.core.wmem_max` and `net.core.rmem_max` large enough;
//! each skips otherwise.
//!
//! `fuzz/` has three `cargo fuzz` targets for what the kernel hands over, which
//! CI runs for a minute each: a notification of any octets, the control
//! messages of a `recvmsg`, and the assembly of messages from any sequence of
//! parts.
#![warn(missing_docs)]

#[cfg(not(any(target_os = "linux", target_os = "android")))]
compile_error!(
    "oxirush-sctp is for Linux: it calls the SCTP of the Linux kernel through its sockets API"
);

mod connected_socket;
mod endpoint;
mod listener;
mod options;
mod socket;

#[doc(inline)]
pub use socket::Socket;

#[doc(inline)]
pub use listener::Listener;

#[doc(inline)]
pub use connected_socket::ConnectedSocket;

#[doc(inline)]
pub use endpoint::OneToManyEndpoint;

#[doc(inline)]
pub use options::SocketOptions;

mod internal;

// What `cargo fuzz` drives, which builds the crate with `--cfg fuzzing`: the readers of what
// the kernel hands over. No other build has this module.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzzing {
    pub use crate::internal::fuzzing::*;
}

mod consts;

mod types;

#[doc(inline)]
pub use types::{
    AssocChangeState, AssociationChange, AssociationId, AssociationParams, BindxFlags, ConnState,
    ConnStatus, Event, EventSubscriptionError, Notification, NotificationOrData, NxtInfo,
    PartialDelivery, PeerAddress, PeerAddressChange, PeerAddressParams, PeerAddressState, RcvInfo,
    ReceivedData, ReceivedInto, RemoteError, RtoInfo, SackInfo, SendData, SendFailure, SendInfo,
    SendOptions, Shutdown, SocketToAssociation, SubscribeEventAssocId,
};
