# oxirush-sctp

[![Crates.io](https://img.shields.io/crates/v/oxirush-sctp.svg)](https://crates.io/crates/oxirush-sctp)
[![Documentation](https://docs.rs/oxirush-sctp/badge.svg)](https://docs.rs/oxirush-sctp)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](LICENSE)

Idiomatic async Rust APIs for the Linux kernel SCTP stack, per the SCTP sockets API of RFC 6458, on Tokio. It is a fork of [sctp-rs](https://github.com/gabhijit/ellora) 0.3.1 by Abhijit Gadgil.

## Features

- **Kernel SCTP, no `libsctp`** — system calls through `libc`, with `std::net::SocketAddr` instead of C socket addresses
- **One-to-one and one-to-many sockets** — `Socket`, `Listener` and `ConnectedSocket`, with multi-homing (`sctp_bindx`, `sctp_connectx`), peel-off and local/peer address lists of any size
- **Whole messages** — `sctp_recv` returns a complete message with its sender's address, however many reads the kernel delivers it in, and stays cancel-safe in the middle of one; messages over a per-socket limit, 4 MiB by default, are an `InvalidData` error
- **Waiting sends** — `sctp_send` waits for room in the send buffer instead of failing with `EWOULDBLOCK`, and never raises `SIGPIPE`
- **Notifications and ancillary data** — association change and shutdown events, the type and octets of any other notification, `SCTP_RCVINFO` and `SCTP_NXTINFO`, parsed with bounds checks
- **Socket options** — `SCTP_NODELAY`, `SCTP_RTOINFO`, `SCTP_INITMSG`, `SCTP_STATUS`, default send info, `SO_REUSEADDR` and nonblocking `SO_LINGER`; `AsRawFd` and `AsFd` for any other option
- **Owned descriptors** — sockets own their descriptor, close it after Tokio deregisters it, and are created close-on-exec

### Changes from sctp-rs 0.3.1

Code written for sctp-rs 0.3 needs the new crate name and three changes: `ConnectedSocket::from_rawfd` is `unsafe`, `Notification` is `#[non_exhaustive]` and its `Unsupported` variant carries the notification's type and octets, and `ReceivedData` has a `from` field with the sender's address. Everything else is a fix or an addition.

- Received control messages are parsed safely: 0.3.1 computed buffer bounds from the wrong header, lost `RCVINFO` when `NXTINFO` was also requested, and spun forever on a control message of another level.
- No descriptor leaks: dropped, unbound or unconnected sockets, cancelled connects and failures after accept or peel-off leaked their descriptor.
- `sctp_recv` returns whole messages (0.3.1 returned at most 4096 bytes and did not report `MSG_EOR`).
- `sctp_send` waits instead of returning `EWOULDBLOCK` with stale write readiness, and sends with `MSG_NOSIGNAL`.
- Connect failures carry the kernel's reason (`ETIMEDOUT`, `EHOSTUNREACH`, …) rather than always `ECONNREFUSED`; the connect futures are `Send`.
- `sctp_getladdrs` and `sctp_getpaddrs` work for more than a few addresses; short notifications no longer panic; `accept` and `SCTP_STATUS` use `socklen_t`.
- New: `set_nodelay`/`nodelay`, `sctp_set_rto_info`/`sctp_get_rto_info` with `RtoInfo`, `Socket::set_reuseaddr`/`reuseaddr`, `ConnectedSocket::set_linger`, `set_max_message_size`/`max_message_size`, and `AsRawFd`/`AsFd`.

The [changelog](CHANGELOG.md) has the details.

## Quick start

```toml
[dependencies]
oxirush-sctp = "0.1"
```

The crate needs Linux 5.0 or later.

## Usage

### Accept an association and receive messages

```rust,no_run
use oxirush_sctp::{NotificationOrData, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // A TCP-style socket: one association per socket.
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:38412".parse().unwrap())?;
    // Send small messages at once rather than waiting for the peer's SACK;
    // accepted sockets inherit the option.
    socket.set_nodelay(true)?;
    let listener = socket.listen(10)?;

    let (association, _peer) = listener.accept().await?;
    loop {
        match association.sctp_recv().await? {
            NotificationOrData::Notification(_notification) => {}
            NotificationOrData::Data(data) if data.payload.is_empty() => break,
            NotificationOrData::Data(data) => println!("{} bytes", data.payload.len()),
        }
    }
    Ok(())
}
```

`examples/src/ping.rs` and `examples/src/pong.rs` are a client and a server:

```bash
cargo run --example pong -- --help
cargo run --example ping -- --help
```

### Closing an association

Dropping a socket starts graceful shutdown by default.
`ConnectedSocket::set_linger(None)` selects that behavior;
`set_linger(Some(std::time::Duration::ZERO))` aborts the association on
drop. Positive linger durations return `InvalidInput` because blocking
close would stall the Tokio runtime.

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
| 6.1.2 | no | |
| 6.1.3 | no | |
| 6.1.4 | N/A | |
| 6.1.5 | yes | |
| 6.1.6 | no | |
| 6.1.7 | no | |
| 6.1.8 | no | |
| 6.1.9 | no | |
| 6.1.10 | no | |
| 6.1.11 | no | |
| 6.2.1 | N/A | |
| 6.2.2 | yes | |
| 8.1.1 | yes | |
| 8.1.2 | no | |
| 8.1.3 | yes | |
| 8.1.4 | partial | `ConnectedSocket::set_linger`: disabled or zero only; positive durations return `InvalidInput`. |
| 8.1.5 | yes | |
| 8.1.6 | no | |
| 8.1.7 | no | |
| 8.1.8 | no | |
| 8.1.9 | no | |
| 8.1.10 | no | |
| 8.1.11 | no | |
| 8.1.12 | no | |
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
| 8.1.28 | yes | `sctp_subscribe_event` / `sctp_unsubscribe_event` |
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
| 9.1 | partial | `Socket::sctp_bindx` and `Listener::sctp_bindx`; connected sockets return `EOPNOTSUPP`. |
| 9.2 | yes | `Listener::sctp_peeloff` |
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
2. All the Send and Receive functions are available as two APIs `sctp_send` and `sctp_recv`, hence no separate implementation for the C like system calls.
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
