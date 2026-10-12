# Changelog

## 0.1.0 (unreleased)

First release. The crate started as a fork of sctp-rs 0.3.1 and does not
keep its API. What follows is what differs, for a program that moves from
sctp-rs.

### The socket types

- `Socket` binds, then becomes one of three types. `listen` gives a
  `Listener`, which accepts. `connect` and `connectx` give a
  `ConnectedSocket`, one association. `into_endpoint`, which is new, gives a
  `OneToManyEndpoint` for a one-to-many socket. In sctp-rs a `Listener` also
  sent, received and peeled off for a one-to-many socket, and a
  `ConnectedSocket` had methods that only such a socket can serve. A call
  on the wrong kind of socket is `InvalidInput`.
- Every type has `options()`, a `SocketOptions` with what sctp-rs had on
  each type, and `local_addr()`.
- `ConnectedSocket::from_rawfd` is `unsafe`: it takes ownership of the
  descriptor. It refuses a one-to-many socket.

### Names

| sctp-rs 0.3.1 | oxirush-sctp 0.1.0 |
| --- | --- |
| `sctp_recv()` | `recv()` |
| `ConnectedSocket::sctp_send(SendData)` | `send(&[u8], SendOptions)`, which borrows the message and takes the payload protocol identifier as a plain number; `send_data(SendData)` is the former call |
| `Listener::sctp_send(to, SendData)` | `OneToManyEndpoint::send(&[u8], SendOptions)`, `send_to(..)` and `send_data(Option<SocketAddr>, SendData)` |
| `Listener::sctp_peeloff` | `OneToManyEndpoint::peeloff` |
| `sctp_bindx`, `sctp_connectx` | `bindx`, `connectx` |
| `sctp_getladdrs`, `sctp_getpaddrs` | `local_addrs`, `peer_addrs` |
| `sctp_get_status` | `status` |
| `sctp_subscribe_event`, `sctp_subscribe_events` | `options().subscribe_events(&[Event], SubscribeEventAssocId)` |
| `sctp_unsubscribe_event`, `sctp_unsubscribe_events` | `options().unsubscribe_events(..)` |
| `sctp_setup_init_params` | `options().set_init_params` |
| `sctp_request_rcvinfo`, `sctp_request_nxtinfo` | `options().request_rcvinfo`, `options().request_nxtinfo` |
| `sctp_set_default_sendinfo` | `options().set_default_sendinfo` |

### Types

- `Notification` and `Event` are `#[non_exhaustive]`: a `match` on them
  needs a wildcard arm. `Notification` has two more variants,
  `PeerAddressChange` and `SendFailure`, and `Unsupported` now carries the
  type and the octets of a notification that the crate does not parse,
  which sctp-rs dropped.
- `ReceivedData` has a `from` field, the address the message came from, and
  the methods `stream_id()`, `ppid()` and `assoc_id()`.
- `SendOptions` is new: the stream, the payload protocol identifier as a
  plain number, the unordered flag, the context and the association.
  `SendInfo::ppid`, `RcvInfo::ppid` and `NxtInfo::ppid` stay as the kernel
  carries them, in network byte order.
- A failed subscription is an `EventSubscriptionError` inside the
  `io::Error`, with every event that failed and the error of each.
- `CmsgType` is no longer public: no function took or returned it.

### Receiving

- `recv` returns a whole message. sctp-rs returned what one `recvmsg` read,
  which is a part of a message longer than its buffer. A dropped `recv`
  future loses nothing: the next call continues the message.
- A message longer than `options().max_message_size()`, 4 MiB by default,
  is discarded and reported as `InvalidData`, so that a peer cannot make
  the receiver buffer without bound.
- Every socket requests the receive information (`SCTP_RCVINFO`) when it
  is created: a received message has its stream and its payload protocol
  identifier without a call to `request_rcvinfo(true)`.
- The end of an association is remembered. Once `recv` has returned the
  error of an association that was aborted or lost, every later call
  returns that error again; and after `shutdown(Shutdown::Write)`, `recv`
  returns the empty message that ends an association when the shutdown is
  complete. In sctp-rs the later call waits forever in both cases.
- The parts of a message of a multihomed peer may come from several of its
  addresses: a socket with one association keeps them together.
- A notification too short for its type is `Unsupported`, where sctp-rs
  panicked. Control messages are read within the buffer that `recvmsg`
  filled.

### Sending

- `send` waits until the socket takes the message. sctp-rs returned
  `EWOULDBLOCK` and left the socket marked as writable.
- Sends pass `MSG_NOSIGNAL`: an association that has ended is `EPIPE` and
  no `SIGPIPE`.
- The futures of `connect` and `connectx` are `Send`, and a failed
  `connectx` reports the error of the socket where sctp-rs reported
  `ECONNREFUSED` for every failure.

### Descriptors

- A socket closes its descriptor on every path, after the runtime has
  deregistered it. In sctp-rs a failed `bind` or `listen`, a dropped
  `connect` future and an error after `accept` or `peeloff` leaked it.
- Descriptors are created non-blocking and close-on-exec in one call.
- Every type implements `AsFd` and `AsRawFd`, for an option that the crate
  does not wrap.

### Added

- `OneToManyEndpoint::connect`, which starts an association without
  consuming the socket, and `send_to`, which sends to an address.
- `bindx` on a `ConnectedSocket`.
- Options: `set_nodelay` and `nodelay`, `set_rto_info` and `rto_info`,
  `set_linger` (off or zero), `set_reuseaddr` and `reuseaddr`,
  `peer_address_params`, `set_peer_address_params` and `request_heartbeat`,
  `set_max_message_size` and `max_message_size`.
- `Notification::PeerAddressChange` and `Notification::SendFailure`, typed.
- `ConnStatus`, `ConnState`, `PeerAddress` and the other status types are
  exported and documented, as is every public item.
- The examples `hello`, `server` and `multihoming`, which run both sides on
  loopback, replace `ping` and `pong`.
- Three `cargo fuzz` targets under `fuzz/`, which CI runs for a minute each:
  `notification` (a notification of any octets), `ancillary` (the control
  messages of a `recvmsg`) and `receive` (the assembly of messages from any
  sequence of parts).
- A `SECURITY.md` that says how to report a vulnerability.

### Requirements

- Linux 5.0 or later, and Rust 1.88 or later.
- tokio's `net` feature alone, where sctp-rs enabled `macros` and `rt` for
  its dependents. The declared minimum versions of the dependencies are
  ones the crate builds with.
- Diagnostics go through `tracing`, where sctp-rs used `log`.
