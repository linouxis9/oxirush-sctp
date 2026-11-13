# oxirush-sctp

[![Crates.io](https://img.shields.io/crates/v/oxirush-sctp.svg)](https://crates.io/crates/oxirush-sctp)
[![Documentation](https://docs.rs/oxirush-sctp/badge.svg)](https://docs.rs/oxirush-sctp)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](COPYRIGHT)

Async SCTP sockets for [Tokio](https://tokio.rs) on Linux, on the kernel's
SCTP and its sockets API ([RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.html)):
`send` and `recv` move one whole message, with its stream and its payload
protocol identifier.

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Stream 1, and the payload protocol identifier of NGAP
    let options = SendOptions::new().stream(1).ppid(60);

    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(5)?;
    let address = listener.local_addr()?;

    let server = async {
        let (association, _peer) = listener.accept().await?;
        if let Some(NotificationOrData::Data(message)) = association.recv().await? {
            assert_eq!(&message.payload[..], b"ping");
            assert_eq!((message.stream, message.ppid), (1, 60));
            association.send(b"pong", options).await?;
        }
        std::io::Result::Ok(())
    };
    let client = async {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let association = socket.connect(address).await?;
        association.send(b"ping", options).await?;
        if let Some(NotificationOrData::Data(message)) = association.recv().await? {
            assert_eq!(&message.payload[..], b"pong");
        }
        std::io::Result::Ok(())
    };
    let (server, client) = tokio::join!(server, client);
    Ok(server.and(client)?)
}
```

Each example shows what it gives with `assert_eq!`, and runs as a test of the crate.

## Installation

```toml
[dependencies]
oxirush-sctp = "0.2"
tokio = { version = "1", features = ["macros", "rt"] }
```

The crate has no Cargo feature. It needs Linux 5.0 or later with SCTP,
which `sudo modprobe sctp` loads, and a Tokio runtime with the I/O driver.
It reports through its errors and notifications, and logs nothing.

The minimum supported Rust version is 1.88.

## An NGAP association, with a writer and a reader

[`driver::spawn`](https://docs.rs/oxirush-sctp/latest/oxirush_sctp/driver/)
gives a socket two tasks. One sends what its `Writer` queues, in batches,
and serves the associations of an endpoint in turn; the other receives.
When messages keep waiting in the queue for more than 5 ms, `overload()`
says that the queue is in overload, and a send then fails at once or waits,
as `DriverConfig` says.

```rust
use oxirush_sctp::driver::{self, DriverConfig};
use oxirush_sctp::{InitParams, NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The AMF listens with four streams each way: NGAP has stream 0 for the
    // messages of no UE, and the others for those of the UEs
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    socket.options().set_nodelay(true)?;
    socket
        .options()
        .set_init_params(InitParams::streams(4, 4))?;
    let listener = socket.listen(5)?;

    // The gNB connects
    let gnb = Socket::new_v4(SocketToAssociation::OneToOne)?;
    gnb.options().set_nodelay(true)?;
    gnb.options().set_init_params(InitParams::streams(4, 4))?;
    let gnb = gnb.connect(listener.local_addr()?).await?;
    let (amf, _) = listener.accept().await?;
    let status = amf.status()?;
    assert_eq!((format!("{:?}", status.state), status.outstreams), ("Established".into(), 4));

    // A writer, which every task that sends can clone, and a reader, for
    // each side. A writer sends what was queued in batches of 64 at most
    let (gnb_writer, mut gnb_reader) = driver::spawn(gnb, DriverConfig::default())?;
    let (amf_writer, mut amf_reader) = driver::spawn(amf, DriverConfig::default())?;

    // The gNB: a message of no UE on stream 0, then a thousand messages of
    // UEs on streams 1 to 3, each with the payload protocol identifier of NGAP
    let setup = SendOptions::new().stream(0).ppid(60);
    gnb_writer.send(b"NG SETUP REQUEST".to_vec(), setup).await?;
    for ue in 0..1000_u16 {
        let options = SendOptions::new().stream(1 + ue % 3).ppid(60);
        gnb_writer.send(ue.to_be_bytes().to_vec(), options).await?;
    }

    // The AMF: each message with its stream, then an answer on stream 0
    let mut streams = [0; 4];
    for _ in 0..1001 {
        if let Some(Ok(NotificationOrData::Data(message))) = amf_reader.recv().await {
            streams[usize::from(message.stream)] += 1;
        }
    }
    assert_eq!(streams, [1, 334, 333, 333]);
    amf_writer
        .send(b"NG SETUP RESPONSE".to_vec(), setup)
        .await?;
    if let Some(Ok(NotificationOrData::Data(message))) = gnb_reader.recv().await {
        assert_eq!(&message.payload[..], b"NG SETUP RESPONSE");
    }

    // What the writer sent and refused, and whether its queue is in overload
    let overload = gnb_writer.overload();
    assert_eq!((overload.sent, overload.refused, overload.overloaded), (1001, 0, false));
    Ok(())
}
```

An association carries the messages of its protocol, such as NGAP on the
streams and with the payload protocol identifier 60 of TS 38.412.
`examples/server.rs` serves several associations, each in a task of its own.

## Notifications

`recv` returns the notifications of the events that a socket subscribed
to, in place of data.

```rust
use oxirush_sctp::{
    Event, Notification, NotificationOrData, SendOptions, Socket, SocketToAssociation,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Subscribe before listening: the accepted associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    socket.options().subscribe_events(&[Event::Association, Event::Shutdown])?;
    let listener = socket.listen(5)?;

    // A peer that sends one message and closes its socket
    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let peer = peer.connect(listener.local_addr()?).await?;
    peer.send(b"hello", SendOptions::new()).await?;
    drop(peer);

    let (association, _) = listener.accept().await?;
    // `None` is the end of the association
    let mut seen = Vec::new();
    while let Some(received) = association.recv().await? {
        seen.push(match received {
            NotificationOrData::Notification(Notification::AssociationChange(change)) => {
                format!("association change: {:?}", change.state)
            }
            NotificationOrData::Notification(Notification::Shutdown(_)) => {
                "the peer shuts the association down".to_string()
            }
            NotificationOrData::Data(message) => format!("{} octets", message.payload.len()),
            other => format!("{other:?}"),
        });
    }
    assert_eq!(
        seen,
        [
            "association change: CommUp",
            "5 octets",
            "the peer shuts the association down",
            "association change: ShutdownComplete",
        ],
    );
    Ok(())
}
```

`Event::Address` subscribes to `Notification::PeerAddressChange`, the
change of state of a path.

## The end of an association

```rust
use oxirush_sctp::{ConnectedSocket, Listener, Socket, SocketToAssociation};
use std::time::Duration;

/// The two ends of a new association of `listener`.
async fn connected(listener: &Listener) -> std::io::Result<(ConnectedSocket, ConnectedSocket)> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let peer = socket.connect(listener.local_addr()?).await?;
    let (accepted, _) = listener.accept().await?;
    Ok((accepted, peer))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(5)?;

    // The peer closes its socket: `None`, at every call from then on
    let (association, peer) = connected(&listener).await?;
    drop(peer);
    for _ in 0..2 {
        assert!(association.recv().await?.is_none());
    }

    // The peer aborts, as a socket does that is dropped with a linger of
    // zero: an error
    let (association, peer) = connected(&listener).await?;
    peer.options().set_linger(Some(Duration::ZERO))?;
    drop(peer);
    let end = association.recv().await.map(|_| ());
    assert_eq!(end.map_err(|error| error.kind()), Err(std::io::ErrorKind::ConnectionReset));
    Ok(())
}
```

Once an association has ended, every call of `recv` returns `None` if it
was shut down, or the error if it was aborted or lost. So
`while let Some(received) = association.recv().await?` ends with the
association. The documentation lists every way an association ends.

To close without loss, `association.close(deadline).await?` shuts the
association down, reads until the peer answers, and returns what the peer
still sent. Linux closes a socket that is dropped, and aborts its
association when a message or a notification was left unread: what the peer
had not acknowledged is then lost. A `OneToManyEndpoint` learns that an
association ended from the notifications only.

## Multihoming

`bindx` binds several local addresses, and `connectx` gives several
addresses of the peer. The addresses of one call share a port. A socket that
connects without being bound offers the peer every address of the host,
loopback included: bind the client first, as here, to offer only the
addresses you chose.

```rust
use oxirush_sctp::{BindxFlags, Socket, SocketToAssociation};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

fn ips(addresses: &[SocketAddr]) -> Vec<IpAddr> {
    addresses.iter().map(SocketAddr::ip).collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // With port 0 the kernel chooses one port for both addresses
    let server = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let addresses: [SocketAddr; 2] = ["127.0.0.1:0".parse()?, "127.0.0.2:0".parse()?];
    server.bindx(&addresses, BindxFlags::Add)?;
    let listener = server.listen(5)?;
    let server_addresses = listener.local_addrs()?;

    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let addresses: [SocketAddr; 2] = ["127.0.0.1:0".parse()?, "127.0.0.3:0".parse()?];
    client.bindx(&addresses, BindxFlags::Add)?;
    let client = client.connectx(&server_addresses).await?;
    let id = client.status()?.assoc_id;
    let (accepted, _) = listener.accept().await?;

    let (one, two, three): (IpAddr, IpAddr, IpAddr) =
        ("127.0.0.1".parse()?, "127.0.0.2".parse()?, "127.0.0.3".parse()?);
    assert_eq!(ips(&client.local_addrs()?), [one, three]);
    assert_eq!(ips(&client.peer_addrs()?), [one, two]);
    assert_eq!(ips(&accepted.peer_addrs()?), [one, three]);
    assert_eq!(client.status()?.peer_primary.address.ip(), one);

    // Probe every path each second instead of each 30 seconds
    for address in client.peer_addrs()? {
        let mut path = client.options().peer_address_params(id, address)?;
        path.heartbeat_interval = Duration::from_secs(1);
        client.options().set_peer_address_params(path)?;
        let path = client.options().peer_address_params(id, address)?;
        assert_eq!((path.heartbeat_interval, path.path_max_retrans), (Duration::from_secs(1), 5));
    }
    Ok(())
}
```

## Options and status

Every socket type has `options()`. The sockets implement `AsFd` and
`AsRawFd`, for an option that the crate does not wrap.

```rust
use oxirush_sctp::{InitParams, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // What is set before `listen` or `connect` stays, and the accepted
    // associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let options = socket.options();
    options.set_nodelay(true)?; // send small messages at once
    options.set_init_params(InitParams::streams(4, 4))?; // four streams each way
    options.set_max_message_size(64 << 10); // refuse longer received messages
    let listener = socket.listen(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let _peer = peer.connect(listener.local_addr()?).await?;
    let (association, _) = listener.accept().await?;

    let status = association.status()?;
    assert_eq!(format!("{:?}", status.state), "Established");
    assert_eq!((status.outstreams, status.instreams), (4, 4));
    let options = association.options();
    assert!(options.nodelay()?);
    assert_eq!(options.max_message_size(), 65536);
    Ok(())
}
```

## One socket for many associations

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let endpoint = socket.into_endpoint(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let peer = peer.connect(endpoint.local_addr()?).await?;
    peer.send(b"ping", SendOptions::new()).await?;

    // A message comes with its association, which the answer names
    if let NotificationOrData::Data(message) = endpoint.recv().await? {
        assert_eq!(&message.payload[..], b"ping");
        endpoint.send(message.assoc_id, b"pong", SendOptions::new()).await?;
    }
    if let Some(NotificationOrData::Data(message)) = peer.recv().await? {
        assert_eq!(&message.payload[..], b"pong");
    }
    Ok(())
}
```

`connect` starts an association without waiting for it, `send_to` sends to
an address, `shutdown` and `abort` end one association, and `peeloff` moves
one to a `ConnectedSocket` of its own.

## Many messages a call

`send_many` sends a batch, each message with its options, and returns how
many the socket took: all of them, unless its buffer filled. The kernel puts
consecutive messages of an association in as few packets as it can.
`recv_many` returns whole messages, as `recv` does.

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(5)?;
    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let client = client.connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;

    // Three messages, each with its options, in one system call
    let messages = [
        (&b"one"[..], SendOptions::new().stream(0).ppid(60)),
        (b"two", SendOptions::new().stream(1).ppid(60)),
        (b"three", SendOptions::new().stream(1).ppid(60)),
    ];
    let mut sent = 0;
    while sent < messages.len() {
        sent += client.send_many(&messages[sent..]).await?;
    }

    // What has come, 64 messages a call at most
    let mut received = Vec::new();
    while received.len() < messages.len() {
        server.recv_many(&mut received, 64).await?;
    }
    let data: Vec<_> = (received.iter())
        .filter_map(|item| match item {
            NotificationOrData::Data(message) => Some((&message.payload[..], message.stream)),
            _ => None,
        })
        .collect();
    assert_eq!(data, [(&b"one"[..], 0), (b"two", 1), (b"three", 1)]);
    Ok(())
}
```

## Limits

- It does not implement SCTP: the protocol runs in the Linux kernel.
- It runs on Linux alone.
- It has no SCTP over DTLS, which the data channels of WebRTC use.
- It does not cover the rest of RFC 6458: among others authentication,
  partial reliability and stream reconfiguration.
- The associations of a `OneToManyEndpoint` share the receive buffer of the
  socket, unless `net.sctp.rcvbuf_policy` is 1: one that fills it makes the
  others wait. So does a message that comes in parts, longer than a packet
  or than what the receiver had room for: the messages of the other
  associations come after its last part. A `Listener` gives each association
  a socket of its own.
- They share its send buffer too: unless `net.sctp.sndbuf_policy` is 1,
  Linux wakes the endpoint once for each of its associations when one of
  them has data acknowledged. To 100 associations, a message took 34 µs of
  CPU on loopback, and to one, 11 µs.
- Now and then Linux stops delivering a message many times longer than the
  receive buffer: nothing more of it becomes readable, while the sender
  sends its parts again.

## Performance

On an AMD Ryzen 7 5700G with Linux 7.0 and Rust 1.98.1, over the loopback
interface, at ea7c778 on 2026-10-18:

| | `send` and `recv` | `send_many` and `recv_many` |
|---|---|---|
| A message of 200 octets sent and received, one at a time, on one thread | 8.0 µs | |
| A message of 64 KiB, the same | 35 µs | |
| Messages of 200 octets from one task to another, on two threads | 163,000 a second, 11 µs each | 377,000 a second, 4.2 µs each |
| The same from 1,000 connected sockets to one endpoint | 185,000 a second, 10 µs each | 349,000 a second, 4.5 µs each |
| The same from one endpoint to 100 associations, held back by Linux ([Limits](#limits)) | 54,000 a second, 34 µs each | 56,000 a second, 33 µs each |

A time per message is the CPU time of the process, the kernel's work for the
loopback interface included. Without `options().set_nodelay(true)` a small
message can wait 200 ms for the acknowledgement of the one before it. The
[driver](https://docs.rs/oxirush-sctp/latest/oxirush_sctp/driver/) carries
what `send_many` carries. [`benches/`](benches/README.md) has the
benchmarks, the method and how to profile.

### Against other implementations

The same loops, at ea7c778 on cores 2 and 3 of the same machine, for a
program in C on the kernel's sockets and the SCTP crates of crates.io that
run here: tokio-sctp 0.2.0, async-sctp 1.0.0, lksctp 0.1.0 and sctp-rs
0.3.1. A ratio is the time of the other implementation over this crate's,
and "about the same" is within 10 %:

| Loop, 200 octets | This crate | Other / this crate |
|---|---|---|
| Ping-pong | 7,999 ns | C and the four crates: about the same |
| One association, two threads | 7,118 ns | C and the four crates: about the same |
| The same, 64 messages a call (`send_many`, `recv_many`) | 2,713 ns | C with `sendmmsg` and `recvmmsg`: 0.84x |

With one message a call, the kernel takes most of the time.
[`benches/`](benches/README.md#against-other-implementations) has every
figure, with cycles, instructions, allocations and peak memory, and the rows
for 16 KiB and 100 associations. `benches/compare.sh` builds and runs them
all:

```sh
CPUS=2,3 ROUNDS=3 benches/compare.sh
```

## Tests and fuzzing

```sh
sudo modprobe sctp
cargo test
```

The tests open associations over the loopback interface. CI runs four
`cargo fuzz` targets for a minute each, and an hour each day: what the
kernel hands over is read without a panic and within its octets. Each Rust
block of this page checks what it gives with `assert_eq!`, and runs as a
documentation test of the crate.

[The documentation](https://docs.rs/oxirush-sctp) has the terms, the
sections of RFC 6458 that the crate covers, the socket types, and the rules
of `send` and `recv`.

A pull request comes with a test for what it changes. Sign off your commits
(`git commit -s`): by contributing you agree to the [Developer Certificate
of Origin](https://developercertificate.org/). [SECURITY.md](SECURITY.md)
says how to report a vulnerability.

## The OxiRush family

- [oxirush-utils](https://github.com/linouxis9/oxirush-utils): the 3GPP identities, and the JSON tree with its paths that the others share.
- [oxirush-aper](https://github.com/linouxis9/oxirush-aper): aligned PER, the runtime of the NGAP and S1AP codecs.
- [oxirush-nas](https://github.com/linouxis9/oxirush-nas): the NAS messages of 5GS and EPS.
- [oxirush-security](https://github.com/linouxis9/oxirush-security): NAS security, AKA and the derivation of keys.
- [oxirush-ngap](https://github.com/linouxis9/oxirush-ngap): NGAP, between a gNB and an AMF.
- [oxirush-s1ap](https://github.com/linouxis9/oxirush-s1ap): S1AP, between an eNB and an MME.
- [oxirush-gtp-u](https://github.com/linouxis9/oxirush-gtp-u): GTP-U tunnels, with a test UPF.
- [oxirush-sctp](https://github.com/linouxis9/oxirush-sctp): SCTP sockets, which carry NGAP and S1AP.

## License

Licensed under the Apache License, Version 2.0, or the MIT License, at your
option: see [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT).

oxirush-sctp started as a fork of [sctp-rs](https://github.com/gabhijit/ellora).
