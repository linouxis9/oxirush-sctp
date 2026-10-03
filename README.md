# oxirush-sctp

[![Crates.io](https://img.shields.io/crates/v/oxirush-sctp.svg)](https://crates.io/crates/oxirush-sctp)
[![Documentation](https://docs.rs/oxirush-sctp/badge.svg)](https://docs.rs/oxirush-sctp)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](https://github.com/linouxis9/oxirush-sctp/blob/main/LICENSE)

Async Rust APIs for the Linux kernel SCTP stack, per RFC 6458, on Tokio. It is a maintained fork of [sctp-rs](https://github.com/gabhijit/ellora) 0.3.1 by Abhijit Gadgil.

## Features

- **Kernel SCTP, no `libsctp`** — system calls through `libc`, using Rust socket addresses
- **Explicit socket roles** — `Socket` configures and binds; `Listener` accepts one-to-one associations; `ConnectedSocket` owns one association; `OneToManyEndpoint` shares I/O across multiple associations
- **Multi-homing** — local address changes, multihomed connects, address lists and association peel-off
- **Whole messages** — `recv` assembles complete records through `MSG_EOR`, retaining partial delivery across cancellation; records over a configurable limit, 4 MiB by default, are drained and reported as `InvalidData`
- **Waiting sends** — borrowed `send` and owned `send_data` wait for capacity and never raise `SIGPIPE`; `SendOptions` uses host-order PPIDs
- **Notifications and ancillary data** — typed association, shutdown, peer-address and modern send-failure events; unsupported notifications preserve their type and bytes; bounded `SCTP_RCVINFO`/`SCTP_NXTINFO` parsing
- **Shared configuration** — `options()` borrows the same implementation on every role; kernel settings and the receive limit survive consuming transitions
- **Owned descriptors** — nonblocking, close-on-exec sockets with Tokio deregistration before close; `AsFd` and `AsRawFd` support additional kernel options

### Changes from sctp-rs 0.3.1

The first release separates socket roles and configuration. Existing callers need the new crate name and the API migration below. `ConnectedSocket::from_rawfd` is unsafe and restricted to established nonblocking `SOCK_STREAM` SCTP descriptors. `Notification` is non-exhaustive, unsupported notifications preserve their bytes, and `ReceivedData` includes the sender's address.

The fork also fixes partial-record truncation, ancillary parsing, stale readiness, descriptor leaks on failures and cancelled connects, lost kernel error codes, oversized address lists and short notifications. The [changelog](CHANGELOG.md) records the earlier fixes.

## Quick start

```toml
[dependencies]
oxirush-sctp = "0.1"
```

Requires Rust 1.85 or later and Linux 5.0 or later with SCTP available.

## Usage

### Accept an association and receive messages

```rust,no_run
use oxirush_sctp::{NotificationOrData, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:38412".parse().unwrap())?;
    socket.options().set_nodelay(true)?;
    socket.options().set_max_message_size(1 << 20);
    let listener = socket.listen(10)?;
    let (association, _peer) = listener.accept().await?;
    loop {
        match association.recv().await? {
            NotificationOrData::Notification(_notification) => {}
            NotificationOrData::Data(data) if data.payload.is_empty() => break,
            NotificationOrData::Data(data) => println!("{} bytes", data.payload.len()),
        }
    }
    Ok(())
}
```

`examples/src/ping.rs` and `examples/src/pong.rs` provide a client and server:

```bash
cargo run --example pong -- --help
cargo run --example ping -- --help
```

### Send borrowed messages

```rust,no_run
use oxirush_sctp::{ConnectedSocket, SendOptions};

async fn send_ngap(socket: &ConnectedSocket, encoded_pdu: &[u8]) -> std::io::Result<()> {
    socket.send(encoded_pdu, SendOptions {
        stream_id: 1,
        ppid: 60,
        ..Default::default()
    }).await
}
```

`SendOptions::ppid` is a logical identifier in host byte order. The low-level `send_data`/`SendInfo` API uses wire-order PPIDs and retains kernel send flags such as EOF and ABORT. A send waits for capacity; cancelling before completion sends no part of the record.

Receive assembly assumes `SCTP_FRAGMENT_INTERLEAVE` level 0. It does not enable the RFC's explicit send-EOR mode, which the Linux SCTP API does not provide.

### Manage one-to-many associations

```rust,no_run
use oxirush_sctp::{Event, OneToManyEndpoint, Socket, SocketToAssociation, SubscribeEventAssocId};

fn endpoint() -> std::io::Result<OneToManyEndpoint> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    socket.options().sctp_subscribe_events(&[Event::Association], SubscribeEventAssocId::All)?;
    let endpoint = socket.into_endpoint(10)?;
    let _association = endpoint.connect(&["127.0.0.1:38412".parse().unwrap()])?;
    Ok(endpoint)
}
```

`connect` initiates an association without consuming the endpoint. Association notifications report whether establishment succeeds. `send` selects `SendOptions::assoc_id`; `send_to` supplies a peer address and can initiate an association. Incoming and outgoing associations share `recv`. Receive metadata is enabled by default so each message identifies its association; callers can explicitly disable it through `options().sctp_request_rcvinfo(false)`, losing that identity.

`peeloff` transfers one association and its queued messages into a `ConnectedSocket`, inheriting the receive limit. It rejects peeling an association during a partially delivered record already held by the endpoint: finish or drain that record first. A one-to-many endpoint has no `accept` method, and a one-to-one listener performs no shared association I/O.

### Observe and configure peer paths

All configuration uses `options()`. Subscribe to `Event::Address` for `Notification::PeerAddressChange`, or `Event::SendFailureEvent` for `Notification::SendFailure`. Unsupported formats retain their original bytes.

Batch subscription attempts every event. Its `io::Error` contains `EventSubscriptionError`; downcast `error.get_ref()` and inspect `failures()` for each original errno.

`peer_address_params`, `set_peer_address_params` and `request_heartbeat` control heartbeat intervals and path retransmission limits. Intervals use whole milliseconds; disabling heartbeats preserves the configured interval. PMTU, SACK delay and other unrelated path settings remain intact.

### Closing an association

Dropping a socket starts graceful shutdown by default. `options().set_linger(None)` selects that behavior; `options().set_linger(Some(std::time::Duration::ZERO))` aborts on drop. Positive linger durations return `InvalidInput` because Linux close can block the Tokio runtime.

### Migrating existing code

| Earlier API | First-release API |
| --- | --- |
| `socket.set_nodelay(true)` and other configuration | `socket.options().set_nodelay(true)`; all roles share `SocketOptions` |
| `socket.sctp_recv()` | `association.recv()` or `endpoint.recv()` |
| `association.sctp_send(data)` | `association.send_data(data)` |
| One-to-many `socket.listen(backlog)` | `socket.into_endpoint(backlog)` |
| One-to-many consuming `socket.connect(addr)` | `endpoint.connect(&[addr])`; completion arrives as a notification |
| `listener.sctp_peeloff(id)` | `endpoint.peeloff(id)` |
| One-to-many `listener.sctp_send(addr, data)` | `endpoint.send_data(Some(addr), data)` or borrowed `endpoint.send_to(...)` |

One-to-many `Socket::listen` and consuming `Socket::connect` now return `InvalidInput` before initiating the incompatible operation. Single-event subscription uses a one-element slice with `sctp_subscribe_events`/`sctp_unsubscribe_events`. Address operations and association status queries keep their SCTP names. Unsafe raw-descriptor import preserves unique ownership; peeled-off descriptors should be obtained through `endpoint.peeloff`.

## Architecture

One private `SocketCore` owns each Tokio registration, descriptor, partial record and receive limit. `Socket` moves that core into a `Listener`, `ConnectedSocket` or `OneToManyEndpoint` without re-registering the descriptor. Accept and peel-off create independently owned association cores with inherited receive limits.

`SocketOptions` borrows the core and implements configuration once for every role. Public roles expose their own operations: listeners accept, associations exchange records, and endpoints manage multiple associations.

`src/internal/sys/` separates descriptor lifecycle, addresses, socket options and message syscalls. Notification decoding has no socket dependencies; ancillary parsing checks initialized byte slices before reading native integer layouts. `src/internal/receive.rs` assembles records directly into their buffers and retains state across cancelled receives.

## References

- [RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.html): SCTP sockets API
- 3GPP TS 38.412: NG signalling transport (NGAP over SCTP)
- 3GPP TS 36.412: S1 signalling transport (S1AP over SCTP)

### RFC 6458 compatibility

This section captures the current support for `SCTP` features with [RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.txt) as a reference. In particular, features marked as `DEPRECATED` in the said RFC are not implemented. Since the Sockets Extension API defined in the RFC describes an API based on C programming language, there is not one to one mapping in the implementation, see notes for further details.

Notification rows describe typed variants; other notifications are delivered
as `Notification::Unsupported` with their type and raw octets.

| Section | Compatibility | Notes |
| ---- | ---- | ---- |
| 3.1.1 | yes | |
| 3.1.2 | yes | |
| 3.1.3 | yes | |
| 3.1.4 | yes | See Note 2. |
| 3.1.5 | yes | See Note 1. |
| 3.1.6 | yes | |
| 4.1.1 | yes | |
| 4.1.2 | yes | |
| 4.1.3 | yes | |
| 4.1.4 | yes | |
| 4.1.5 | yes | |
| 4.1.6 | yes | See Note 1. |
| 4.1.7 | yes | |
| 4.1.8 | yes | See Note 2. |
| 4.1.9 | yes | |
| 5.3.1 | no | Initialization is exposed through `SCTP_INITMSG`, not ancillary `SCTP_INIT`. |
| 5.3.2 | N/A | |
| 5.3.3 | N/A | |
| 5.3.4 | yes | |
| 5.3.5 | yes | |
| 5.3.6 | yes | `NxtInfo` |
| 5.3.7 | no | |
| 5.3.8 | no | |
| 5.3.9 | no | |
| 5.3.10 | no | |
| 6.1.1 | yes | |
| 6.1.2 | yes | `PeerAddressChange` |
| 6.1.3 | no | |
| 6.1.4 | N/A | |
| 6.1.5 | yes | |
| 6.1.6 | no | |
| 6.1.7 | no | |
| 6.1.8 | no | |
| 6.1.9 | no | |
| 6.1.10 | no | |
| 6.1.11 | yes | `SendFailure`; the deprecated format stays available as raw bytes. |
| 6.2.1 | N/A | |
| 6.2.2 | yes | |
| 8.1.1 | yes | |
| 8.1.2 | no | |
| 8.1.3 | yes | |
| 8.1.4 | partial | `SocketOptions::set_linger`: disabled or zero only; positive durations return `InvalidInput`. |
| 8.1.5 | yes | |
| 8.1.6 | no | |
| 8.1.7 | no | |
| 8.1.8 | no | |
| 8.1.9 | no | |
| 8.1.10 | no | |
| 8.1.11 | no | |
| 8.1.12 | partial | Heartbeat and path retransmission controls; other parameters via `AsFd`. |
| 8.1.13 | N/A | |
| 8.1.14 | N/A | |
| 8.1.15 | no | |
| 8.1.16 | no | |
| 8.1.17 | no | |
| 8.1.18 | no | |
| 8.1.19 | no | |
| 8.1.20 | no | |
| 8.1.21 | no | |
| 8.1.22 | N/A | |
| 8.1.23 | no | |
| 8.1.24 | no | |
| 8.1.25 | no | |
| 8.1.26 | no | |
| 8.1.27 | no | |
| 8.1.28 | yes | `SocketOptions::sctp_subscribe_events` / `sctp_unsubscribe_events` |
| 8.1.29 | yes | `sctp_request_rcvinfo` |
| 8.1.30 | yes | `sctp_request_nxtinfo` |
| 8.1.31 | yes | |
| 8.1.32 | no | |
| 8.2.1 | yes | |
| 8.2.2 | no | |
| 8.2.3 | no | |
| 8.2.4 | no | |
| 8.2.5 | no | |
| 8.2.6 | no | |
| 8.3.1 | no | |
| 8.3.2 | no | |
| 8.3.3 | no | |
| 8.3.4 | no | |
| 8.3.5 | no | |
| 9.1 | yes | Available on unconnected, listening and connected sockets. |
| 9.2 | yes | `OneToManyEndpoint::peeloff` |
| 9.3 | yes | `sctp_getpaddrs` |
| 9.4 | N/A | Address lists are owned Rust vectors. See Note 3. |
| 9.5 | yes | `sctp_getladdrs` |
| 9.6 | N/A | Address lists are owned Rust vectors. See Note 3. |
| 9.7 | N/A | |
| 9.8 | N/A | |
| 9.9 | yes | `Socket::sctp_connectx` |
| 9.10 | N/A | |
| 9.11 | N/A | |
| 9.12 | no | |
| 9.13 | no | |

Notes:
1. The `drop` implementation on the socket 'close'es the socket, hence no explicit `close` call supported.
2. The async `send`, `send_to`, `send_data` and `recv` methods wrap the message-oriented kernel operations.
3. This API is not required to be implemented in Rust.

## Tests

```bash
cargo test
```

The tests open SCTP associations over loopback, so the kernel's `sctp` module must be available. The connect-timeout test needs `unshare` and `ip`, and the 4 MiB message tests need `net.core.wmem_max` and `net.core.rmem_max` large enough; each skips otherwise. Sender errors in the receive-limit tests fail immediately, and a timeout bounds the send/receive pair.

## Documentation

Full API reference: **<https://docs.rs/oxirush-sctp>**

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Sign off your commits (`git commit -s`)
4. Open a Pull Request

### Developer Certificate of Origin (DCO)

By contributing to this project, you agree to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/). This means that you have the right to submit your contributions and you agree to license them according to the project's license.

All commits should be signed-off with `git commit -s` to indicate your agreement to the DCO.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-Apache2](LICENSE-Apache2) or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Acknowledgements

oxirush-sctp is a fork of [sctp-rs](https://github.com/gabhijit/ellora) by Abhijit Gadgil and the [Ellora contributors](AUTHORS.md).

Fixes that apply to sctp-rs are also worth offering [upstream](https://github.com/gabhijit/ellora).
