# Changelog

## Unreleased

### Changed

- The test of a message longer than 4 MiB is skipped, with a message, where Linux
  refuses the memory of its send (ENOMEM): other programs hold the SCTP memory of
  the host, which `net.sctp.sctp_mem` bounds. Any other error still fails it.

### Removed

- `readme/`, the package that compared what each block of the README and of the
  front page of the documentation printed with the text under it: the blocks check
  what they give with `assert_eq!`, and run as documentation tests.

## 0.2.0 - 2026-11-08

### Added

- `OneToManyEndpoint::shutdown(assoc_id)` shuts one association down, beside
  `abort`.
- `ConnectedSocket::try_from(OwnedFd)` imports a descriptor, and turns its
  receive information on.

### Changed

- `SendOptions` alone says how a message is sent. A `OneToManyEndpoint`
  names the association where its calls take one: `send(assoc_id, payload,
  options)`, `poll_send(cx, assoc_id, payload, options)` and
  `send_many(&[(assoc_id, payload, options)])`.
- `ReceivedData` has `stream`, `ppid`, `assoc_id` and `unordered` as fields.
  Every socket receives the stream, the payload protocol identifier and the
  association of each message.
- `subscribe_events(&[Event])` and `unsubscribe_events(&[Event])` apply to
  every association of the socket and to those to come, and fail at the
  first event that the kernel refuses, with an error that names it.
- `Event` has the events whose notifications the crate parses; the send
  failure is `Event::SendFailure`.
- `SendFailure` has the `stream`, `ppid` and `context` of the message.
- `Socket::connect` and `connectx` return the `ConnectedSocket`;
  `status().assoc_id` has the ID of its association.
- `NotificationOrData` is no longer `#[non_exhaustive]`.
- The writer of a driver has the type of its socket: `driver::Writer<S>`.
  That of an endpoint names the association of each message, as the
  endpoint does.
- An endpoint takes the parts of a message one after the other, as Linux
  has it by default: while a long message of one association comes in
  parts, the messages of the others wait. It holds one message in parts at
  most, which `max_message_size` bounds.
- The crate logs nothing: it reports through its errors and notifications,
  and no longer depends on `tracing`.

### Removed

- `SendInfo`, `SendData`, `send_data`, `SendOptions::association` and
  `SocketOptions::set_default_sendinfo`.
- `RcvInfo`, `NxtInfo`, `ReceivedData::rcv_info`, `nxt_info`, `stream_id()`,
  `ppid()` and `assoc_id()`, `SocketOptions::request_rcvinfo` and
  `request_nxtinfo`.
- `SubscribeEventAssocId`, `EventSubscriptionError`, and the events
  `DataIo`, `SendFailure` (the deprecated one), `AdaptationLayer`,
  `Authentication`, `StreamReset`, `AssociationReset` and `StreamChange`.
- `recv_into`, `poll_recv_into` and `ReceivedInto`.
- `poll_send_many` and `poll_recv_many` of both socket types; the methods
  of `driver::Batched`, which is a sealed marker.
- `SocketOptions::set_max_assembly` and `max_assembly`.
- `ConnectedSocket::from_rawfd`.

### Migration from 0.1

| 0.1 | 0.2 |
|-----|-----|
| `socket.send_data(SendData::new(payload))` | `socket.send(&payload, SendOptions::new())` |
| `SendData::new(payload).with_info(SendInfo::from(options))` | `send(&payload, options)` |
| `SendInfo { flags: SendInfo::UNORDERED, .. }` | `SendOptions::new().unordered(true)` |
| `SendInfo { flags: SendInfo::EOF, assoc_id, .. }` on an endpoint | `endpoint.shutdown(assoc_id)` |
| `SendInfo { flags: SendInfo::ABORT, .. }` | `abort(reason)`, `endpoint.abort(assoc_id, reason)` |
| `options().set_default_sendinfo(info)` | the options of each send |
| `endpoint.send(payload, SendOptions::new().association(id))` | `endpoint.send(id, payload, SendOptions::new())` |
| `endpoint.send_many(&[(payload, options.association(id))])` | `endpoint.send_many(&[(id, payload, options)])` |
| `failure.snd_info.sid`, `.ppid`, `.context` | `failure.stream`, `.ppid`, `.context` |
| `message.stream_id()`, `message.ppid()`, `message.assoc_id()` | `message.stream`, `message.ppid`, `message.assoc_id` |
| `message.rcv_info.unwrap().flags & 1 != 0` | `message.unordered` |
| `options().request_rcvinfo(true)` | nothing: always on |
| `options().request_nxtinfo(true)`, `message.nxt_info` | none |
| `subscribe_events(&events, SubscribeEventAssocId::All)` | `subscribe_events(&events)` |
| `Event::SendFailureEvent` | `Event::SendFailure` |
| `let (socket, assoc_id) = socket.connect(address).await?` | `let socket = socket.connect(address).await?; let assoc_id = socket.status()?.assoc_id;` |
| `unsafe { ConnectedSocket::from_rawfd(fd) }` | `ConnectedSocket::try_from(OwnedFd::from(..))` |
| `socket.recv_into(&mut buffer)` | `socket.recv()` |
| `poll_send_many`, `poll_recv_many` | `send_many`, `recv_many` |
| `driver::Writer` | `driver::Writer<ConnectedSocket>`, `driver::Writer<OneToManyEndpoint>` |
| `endpoint_writer.send(payload, options.association(id))` | `endpoint_writer.send(id, payload, options)` |
| `options().set_max_assembly(octets)` | `options().set_max_message_size(octets)` bounds the one message in parts |
| `match received { Data(..) => .., Notification(..) => .., _ => .. }` | without the `_` arm |
| a `tracing` filter for `oxirush_sctp` | the errors and notifications |

## 0.1.1 - 2026-10-31

### Added

- `SocketOptions::association_stats` reads `SCTP_GET_ASSOC_STATS`: the
  per-association statistics the kernel keeps, as `AssociationStats`. It
  reports the chunks an association retransmitted, the gap-acknowledgement
  blocks and duplicate and out-of-sequence chunks it saw, the packets and
  SACKs it sent and received, and its highest measured retransmission
  timeout. A caller that watched only the socket-wide counters could not
  tell one association's loss from another's.

## 0.1.0 - 2026-10-24

First release, started as a fork of sctp-rs 0.3.1 whose API it does not keep.

- Async SCTP sockets for Tokio on the Linux kernel stack (RFC 6458): `Socket`,
  `Listener`, `ConnectedSocket` (one association) and `OneToManyEndpoint`
  (many), with `SocketOptions`, multihoming (`bindx`, `connectx`), typed
  notifications, the end of an association reported to every later call,
  `close(deadline)` that loses nothing that was sent, and batches of sends and
  receives (`send_many`, `recv_many`).
- The module `driver`: a writer and a reader of a socket, each a task that
  sends or receives in batches.
- Linux 5.0 or later, Rust 1.88 or later.
