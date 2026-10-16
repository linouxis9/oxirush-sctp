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

- Every public enum and every public structure is `#[non_exhaustive]`: a
  `match` needs a wildcard arm, and a structure is not written as a literal.
  `SendData::new(payload).with_info(info)`, `RtoInfo::new(initial, max,
  min)`, `PeerAddressParams::new(assoc_id, address)` and `Default` make the
  values that a program gives, whose fields it can then set.
- `Notification` has five more variants, `PeerAddressChange`,
  `SendFailure`, `RemoteError`, `PartialDelivery` and `SenderDry`, and
  `Unsupported` now carries the number of the type and the octets of a
  notification that the crate does not parse, which sctp-rs dropped.
- `AssociationChange` and `Shutdown` no longer mirror the C structure:
  `ev_type`, `flags` and `length` are gone, `ob_streams` and `ib_streams`
  are `outbound_streams` and `inbound_streams`, and `error` is `cause`, the
  error cause of SCTP as a number (12 after a user-initiated abort).
  `SendFailure` has `sent` and `cause` in the place of `flags` and `error`.
- `AssocChangeState::Unknown` and `ConnState::Unknown` keep the value of
  the kernel. `Event::Unknown` is gone: it could not be subscribed.
- `ReceivedData` has a `from` field, the address the message came from, and
  the methods `stream_id()`, `ppid()` and `assoc_id()`.
- `SendOptions` is new: `SendOptions::new().stream(1).ppid(60)`, with
  `unordered`, `context` and `association`.
- A payload protocol identifier is a plain number everywhere: 60 for NGAP
  in `SendOptions`, `SendInfo::ppid`, `RcvInfo::ppid` and `NxtInfo::ppid`.
  sctp-rs took and returned it in network byte order.
- `Debug` of a socket prints its local address, the address of its peer
  and its descriptor.
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
- The end of an association is in the type: `ConnectedSocket::recv`
  returns `Ok(None)` for an association that was shut down, where sctp-rs
  returned a message without octets, and an error for one that was aborted
  or lost. `while let Some(received) = association.recv().await?` ends
  there.
- The end is recorded by the call that learns of it, `recv`, `send` or a
  notification, and every later call returns it at once: `recv` the end or
  the error, `send` `BrokenPipe` or the error. Linux tells one call alone:
  in sctp-rs, a `recv` after a `send` that had taken the error of an abort
  waited forever. The documentation has the table of every way an
  association ends, and `tests/association_end.rs` a test for each row.
- A notification is never discarded for its length: the limit is for the
  messages of the peer.
- `recv_into` receives into a buffer of the caller.
- `poll_recv`, `poll_recv_into` and `poll_send` of `ConnectedSocket` and of
  `OneToManyEndpoint`, `OneToManyEndpoint::poll_send_to` and
  `Listener::poll_accept`: the calls as polls, for a `Stream` or a future
  of one's own. A one-to-one socket subscribes to its association changes
  for itself, so that the end of its association makes it readable: Linux
  reports an abort as an error alone otherwise, which wakes no caller that
  waits for the socket to be readable. It passes them on only to a caller
  that subscribes to them, as before.
- A received message holds the memory of its octets and no more: the
  buffer that it was read into has room for 4 KiB at least, which a message
  of 64 octets kept.
- A message of up to 64 KiB takes one read and one allocation: the first
  read of a message goes into a buffer of the thread, from which a message
  that it holds whole is copied to a payload of its length. A message of
  16 KiB took three reads into a payload that doubled.
- The parts of a message of a multihomed peer may come from several of its
  addresses: a socket with one association keeps them together.
- On a `OneToManyEndpoint` the parts of the messages of several
  associations come in turns (`SCTP_FRAGMENT_INTERLEAVE` 1), and each is
  assembled apart: a long message of one association holds back none of
  the others. With Linux's default and one assembler for the socket, a
  short message waited for a long one, 1.2 s for 512 KiB with a receive
  buffer of 64 KiB on loopback, where it now takes 75 µs. The endpoint
  subscribes to `Event::PartialDelivery` for itself, to drop the parts of
  a message whose delivery the kernel gives up, and passes those
  notifications on only to a caller that subscribes to them.
- A notification too short for its type is `Unsupported`, where sctp-rs
  panicked. Control messages are read within the buffer that `recvmsg`
  filled.

### Sending

- `send` waits until the socket takes the message. sctp-rs returned
  `EWOULDBLOCK` and left the socket marked as writable.
- Sends pass `MSG_NOSIGNAL`: an association that has ended is an error and
  no `SIGPIPE`.
- An empty message, a stream that the association does not have, an
  address of IPv6 on a socket of IPv4 and addresses of one call with
  different ports are `InvalidInput` with a text that says which, where
  Linux answers a bare `EINVAL`.
- `status`, `peer_addrs` and `local_addrs` of a `ConnectedSocket`, a
  `Listener` and a `Socket` take no association identifier, which they
  ignored, and a `ConnectedSocket` refuses `SendOptions::association`.
- `Listener` has no `shutdown`: a listener is closed by dropping it.
- The library logs nothing at ERROR: what a call returns is the caller's
  to judge.
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
  `set_max_message_size` and `max_message_size`,
  `set_association_params` and `association_params` (the retransmission
  limit of an association), `set_sack_info` and `sack_info`,
  `set_primary_address`, `set_dscp` and `dscp`, `set_send_buffer_size`,
  `set_recv_buffer_size` and their getters.
- `ConnectedSocket::close(deadline)`, which shuts the association down,
  reads to its end and returns what the peer had still sent: a socket that
  is dropped with a message that it has not read aborts, and loses what it
  had sent.
- `ConnectedSocket::abort(reason)` and `OneToManyEndpoint::abort(assoc_id,
  reason)`.
- `Notification::PeerAddressChange` and `Notification::SendFailure`, typed.
- `ConnStatus`, `ConnState`, `PeerAddress` and the other status types are
  exported and documented, as is every public item.
- The examples `hello`, `notifications`, `association_end`, `multihoming`,
  `options`, `one_to_many` and `server`, which run both sides on loopback,
  replace `ping` and `pong`. The README shows all but the last: a test
  compares the two, and what each prints with the text that the README
  shows under it.
- Three `cargo fuzz` targets under `fuzz/`, which CI runs for a minute each:
  `notification` (a notification of any octets), `ancillary` (the control
  messages of a `recvmsg`) and `receive` (the assembly of messages from any
  sequence of parts).
- A `SECURITY.md` that says how to report a vulnerability.
- `benches/`, a package of its own that times a message through an
  association with criterion and profiles it with pprof. The published
  crate does not have it.

### Requirements

- Linux 5.0 or later, and Rust 1.88 or later.
- tokio's `net` feature alone, where sctp-rs enabled `macros` and `rt` for
  its dependents. The declared minimum versions of the dependencies are
  ones the crate builds with.
- Diagnostics go through `tracing`, where sctp-rs used `log`.
