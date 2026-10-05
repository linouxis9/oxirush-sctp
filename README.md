# oxirush-sctp

[![Crates.io](https://img.shields.io/crates/v/oxirush-sctp.svg)](https://crates.io/crates/oxirush-sctp)
[![Documentation](https://docs.rs/oxirush-sctp/badge.svg)](https://docs.rs/oxirush-sctp)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](https://github.com/linouxis9/oxirush-sctp/blob/main/LICENSE)

Async SCTP sockets for [Tokio](https://tokio.rs) on Linux. `send` and `recv` move one whole message at a time, with its stream and payload protocol identifier, over the kernel's SCTP implementation and its sockets API ([RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.html)).

- **One-to-one sockets**, which connect, listen and accept like TCP ones, and **one-to-many sockets**, which carry many associations each
- **Multihoming**: several local and peer addresses in one association
- **Notifications**: an association comes up, is shut down or is lost, a peer address changes state, a message could not be delivered
- **No `libsctp`**: the crate makes the system calls itself

It is not an SCTP implementation. The protocol runs in the Linux kernel, so the crate serves neither other systems nor SCTP over DTLS (WebRTC data channels). It wraps the part of RFC 6458 [listed below](#rfc-6458-coverage).

## Requirements

- Linux 5.0 or later with SCTP. `sudo modprobe sctp` loads the kernel module, which some distributions ship in a separate package of extra kernel modules. Without it, creating a socket fails with `EPROTONOSUPPORT`, or `ESOCKTNOSUPPORT` for a one-to-many socket.
- A Tokio runtime with the I/O driver (`#[tokio::main]`, or `enable_io` on a runtime builder). Sockets register with it when they are created, and panic without it.
- Rust 1.85 or later.

```toml
[dependencies]
oxirush-sctp = "0.1"
tokio = { version = "1", features = ["macros", "rt"] }
```

## Usage

### A client and a server

Both sides send on stream 1 with a payload protocol identifier, and read them back from what they receive.

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Stream 1, and the payload protocol identifier of NGAP.
    let options = SendOptions {
        stream_id: 1,
        ppid: 60,
        ..Default::default()
    };

    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;
    let address = listener.local_addr()?;

    let server = async {
        let (association, peer) = listener.accept().await?;
        if let NotificationOrData::Data(message) = association.recv().await? {
            println!(
                "{peer} sent {:?} on stream {:?} with PPID {:?}",
                String::from_utf8_lossy(&message.payload),
                message.stream_id(),
                message.ppid()
            );
            association.send(b"pong", options).await?;
        }
        std::io::Result::Ok(())
    };
    let client = async {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let (association, _) = socket.connect(address).await?;
        association.send(b"ping", options).await?;
        if let NotificationOrData::Data(message) = association.recv().await? {
            let answer = String::from_utf8_lossy(&message.payload);
            println!("the server answered {answer:?}");
        }
        std::io::Result::Ok(())
    };
    let (server, client) = tokio::join!(server, client);
    server.and(client)
}
```

This is `examples/hello.rs`: `cargo run --example hello`.

### Several associations, and how each one ends

A listener accepts one `ConnectedSocket` for each association. Subscribe to events before listening: the accepted associations inherit the subscription.

```rust,no_run
use oxirush_sctp::{
    AssocChangeState, ConnectedSocket, Event, Listener, Notification, NotificationOrData, Socket,
    SocketToAssociation, SubscribeEventAssocId,
};

fn listen() -> std::io::Result<Listener> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("0.0.0.0:38412".parse().unwrap())?;
    socket.options().sctp_subscribe_events(
        &[Event::Association, Event::Shutdown],
        SubscribeEventAssocId::All,
    )?;
    socket.listen(5)
}

async fn accept(listener: Listener) -> std::io::Result<()> {
    loop {
        let (association, _peer) = listener.accept().await?;
        tokio::spawn(serve(association));
    }
}

async fn serve(association: ConnectedSocket) {
    loop {
        match association.recv().await {
            Ok(NotificationOrData::Data(message)) if message.payload.is_empty() => break,
            Ok(NotificationOrData::Data(message)) => println!("{} octets", message.payload.len()),
            Ok(NotificationOrData::Notification(Notification::Shutdown(_))) => {
                println!("the peer shuts the association down")
            }
            Ok(NotificationOrData::Notification(Notification::AssociationChange(change)))
                if change.state == AssocChangeState::CommLost =>
            {
                println!("the association is lost")
            }
            Ok(NotificationOrData::Notification(_)) => {}
            Err(error) => {
                println!("{error}");
                break;
            }
        }
    }
}
```

`examples/server.rs` runs this against a peer that closes its association and one that aborts it.

On a `ConnectedSocket`, the end of the association reaches `recv` as follows. The notifications come first, when subscribed.

| What happened | `recv` returns | Notifications |
| --- | --- | --- |
| The peer closed its socket or shut the association down | a message with an empty payload, on every call | `Shutdown`, then `AssociationChange` with `ShutdownComplete` |
| The peer aborted the association | an error, `ConnectionReset`, once; later calls wait forever | `AssociationChange` with `CommLost` |
| This side called `shutdown(Shutdown::Write)` | nothing: the call waits forever | `AssociationChange` with `ShutdownComplete` |

A `OneToManyEndpoint` learns of the end of an association from the notifications only.

A dropped socket shuts its association down. With `options().set_linger(Some(Duration::ZERO))` it aborts it instead. Other durations are refused, because closing would then block the runtime.

### Multihoming

`sctp_bindx` binds several local addresses and `sctp_connectx` gives several addresses of the peer. The addresses of one call share a port, which the kernel chooses for port 0.

```rust,no_run
use oxirush_sctp::{BindxFlags, ConnectedSocket, Socket, SocketToAssociation};
use std::net::SocketAddr;

async fn connect(local: &[SocketAddr], peer: &[SocketAddr]) -> std::io::Result<ConnectedSocket> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.sctp_bindx(local, BindxFlags::Add)?;
    let (association, id) = socket.sctp_connectx(peer).await?;
    println!("local addresses {:?}", association.sctp_getladdrs(id)?);
    println!("peer addresses {:?}", association.sctp_getpaddrs(id)?);
    Ok(association)
}
```

`examples/multihoming.rs` does it on loopback addresses. Subscribe to `Event::Address` to receive a `Notification::PeerAddressChange` when a path changes state. `options().peer_address_params`, `set_peer_address_params` and `request_heartbeat` read and set the heartbeat and the retransmission limit of a path.

### Status and options

Every socket type has `options()`. What is set before `listen` or `connect` stays, and accepted associations inherit it.

```rust,no_run
use oxirush_sctp::{ConnectedSocket, Socket};

fn configure(socket: &Socket) -> std::io::Result<()> {
    let options = socket.options();
    options.set_nodelay(true)?; // send small messages at once
    options.sctp_setup_init_params(4, 4, 0, 0)?; // four streams each way
    options.set_max_message_size(64 << 10); // refuse longer received messages
    Ok(())
}

fn report(association: &ConnectedSocket) -> std::io::Result<()> {
    let status = association.sctp_get_status(0)?;
    println!(
        "{:?}, {} outgoing streams, primary path {}",
        status.state, status.outstreams, status.peer_primary.address
    );
    Ok(())
}
```

`examples/server.rs` sets options before it listens and prints the status of each association it accepts. For an option the crate does not wrap, the sockets implement `AsFd` and `AsRawFd`.

### One socket for many associations

A one-to-many socket needs no accept: its messages arrive on one `recv`, each with the association it belongs to.

```rust,no_run
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

async fn echo() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    socket.bind("0.0.0.0:38412".parse().unwrap())?;
    let endpoint = socket.into_endpoint(5)?;
    loop {
        if let NotificationOrData::Data(message) = endpoint.recv().await? {
            let options = SendOptions {
                assoc_id: message.assoc_id().unwrap(),
                ..Default::default()
            };
            endpoint.send(&message.payload, options).await?;
        }
    }
}
```

`connect` starts an association without waiting for it: subscribe to `Event::Association` to learn whether it came up. `send_to` sends to an address, and starts an association if there is none. `peeloff` moves one association to a `ConnectedSocket` of its own.

## Reference

### Socket types

| Type | Comes from | Does |
| --- | --- | --- |
| `Socket` | `Socket::new_v4`, `Socket::new_v6` | binds, then becomes one of the next three |
| `Listener` | `Socket::listen` | accepts associations |
| `ConnectedSocket` | `Socket::connect`, `Socket::sctp_connectx`, `Listener::accept`, `OneToManyEndpoint::peeloff` | sends and receives on one association |
| `OneToManyEndpoint` | `Socket::into_endpoint` | sends and receives on many associations |
| `SocketOptions` | `options()` on the four above | reads and sets options |

### Messages

- `recv` returns a whole message, however many reads it takes. A dropped `recv` future loses nothing: the next call continues the message.
- A received message longer than `options().max_message_size()`, 4 MiB by default, is discarded and reported as an `InvalidData` error. The next `recv` returns the message after it.
- `send` waits for room in the send buffer. A dropped `send` future has sent nothing.
- A sent message must fit in the send buffer (`SO_SNDBUF`): a longer one fails with `EMSGSIZE`. An empty one fails with `EINVAL`, as does a stream that the association does not have.
- `ReceivedData::stream_id`, `ppid` and `assoc_id` read the receive information (`SCTP_RCVINFO`) that every socket requests. They return `None` after `options().sctp_request_rcvinfo(false)`, and for the empty message that ends an association.

### Payload protocol identifiers

`SendOptions::ppid` and `ReceivedData::ppid` are plain numbers: 60 is NGAP. The lower-level `SendInfo::ppid`, `RcvInfo::ppid` and `NxtInfo::ppid` hold the identifier as the kernel carries it, in network byte order: `60_u32.to_be()`.

### RFC 6458 coverage

| RFC 6458 | Provided by |
| --- | --- |
| 3.1, 4.1: socket, bind, listen, accept, connect, send, receive, shutdown, close | the socket types; a dropped socket is closed |
| 5.3.4 `SCTP_SNDINFO` | `SendOptions`, `SendInfo` |
| 5.3.5 `SCTP_RCVINFO`, 5.3.6 `SCTP_NXTINFO` | `ReceivedData::rcv_info`, `nxt_info` |
| 6.1.1 `SCTP_ASSOC_CHANGE` | `Notification::AssociationChange` |
| 6.1.2 `SCTP_PEER_ADDR_CHANGE` | `Notification::PeerAddressChange` |
| 6.1.5 `SCTP_SHUTDOWN_EVENT` | `Notification::Shutdown` |
| 6.1.11 `SCTP_SEND_FAILED_EVENT` | `Notification::SendFailure` |
| 6.1: the other notifications | `Notification::Unsupported`, with their type and octets |
| 8.1.1 `SCTP_RTOINFO` | `sctp_set_rto_info`, `sctp_get_rto_info` |
| 8.1.3 `SCTP_INITMSG` | `sctp_setup_init_params` |
| 8.1.4 `SO_LINGER` | `set_linger`, off or zero |
| 8.1.5 `SCTP_NODELAY` | `set_nodelay`, `nodelay` |
| 8.1.12 `SCTP_PEER_ADDR_PARAMS` | `peer_address_params`, `set_peer_address_params`, `request_heartbeat`: heartbeat and retransmission limit |
| 8.1.28 `SCTP_EVENT` | `sctp_subscribe_events`, `sctp_unsubscribe_events` |
| 8.1.29 `SCTP_RECVRCVINFO`, 8.1.30 `SCTP_RECVNXTINFO` | `sctp_request_rcvinfo`, `sctp_request_nxtinfo` |
| 8.1.31 `SCTP_DEFAULT_SNDINFO` | `sctp_set_default_sendinfo` |
| 8.2.1 `SCTP_STATUS` | `sctp_get_status` |
| 9.1 `sctp_bindx`, 9.9 `sctp_connectx` | `sctp_bindx`, `sctp_connectx` |
| 9.2 `sctp_peeloff` | `OneToManyEndpoint::peeloff` |
| 9.3 `sctp_getpaddrs`, 9.5 `sctp_getladdrs` | `sctp_getpaddrs`, `sctp_getladdrs` |

The rest is not wrapped: among others authentication, partial reliability, the association parameters, the choice of the primary path and the buffer sizes. The interfaces that the RFC deprecates are left out on purpose.

## Examples

```bash
cargo run --example hello        # a client and a server
cargo run --example server       # options, several associations, their status and how each one ends
cargo run --example multihoming  # two addresses on each side
```

Each one runs both sides on loopback and needs no argument.

## Tests

```bash
cargo test
```

The tests open SCTP associations over loopback, so the kernel's `sctp` module must be available. The connect-timeout test needs `unshare` and `ip`, and the 4 MiB message tests need `net.core.wmem_max` and `net.core.rmem_max` large enough; each skips otherwise.

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

oxirush-sctp started as a fork of [sctp-rs](https://github.com/gabhijit/ellora).
