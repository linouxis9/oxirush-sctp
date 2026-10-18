# oxirush-sctp

[![Crates.io](https://img.shields.io/crates/v/oxirush-sctp.svg)](https://crates.io/crates/oxirush-sctp)
[![Documentation](https://docs.rs/oxirush-sctp/badge.svg)](https://docs.rs/oxirush-sctp)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](COPYRIGHT)

Async SCTP sockets for [Tokio](https://tokio.rs) on Linux, over the SCTP of
the kernel and its sockets API
([RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.html)). `send` and `recv`
move one whole message, with its stream and its payload protocol identifier.

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Stream 1, and the payload protocol identifier of NGAP
    let options = SendOptions::new().stream(1).ppid(60);

    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;
    let address = listener.local_addr()?;

    let server = async {
        let (association, _peer) = listener.accept().await?;
        if let Some(NotificationOrData::Data(message)) = association.recv().await? {
            let text = String::from_utf8_lossy(&message.payload);
            let (stream, ppid) = (message.stream_id(), message.ppid());
            println!("server: {text:?} on stream {stream:?} with PPID {ppid:?}");
            association.send(b"pong", options).await?;
        }
        std::io::Result::Ok(())
    };
    let client = async {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let (association, _) = socket.connect(address).await?;
        association.send(b"ping", options).await?;
        if let Some(NotificationOrData::Data(message)) = association.recv().await? {
            println!("client: {:?}", String::from_utf8_lossy(&message.payload));
        }
        std::io::Result::Ok(())
    };
    let (server, client) = tokio::join!(server, client);
    server.and(client)
}
```

```text
server: "ping" on stream Some(1) with PPID Some(60)
client: "pong"
```

## Installation

```toml
[dependencies]
oxirush-sctp = "0.1"
tokio = { version = "1", features = ["macros", "rt"] }
```

The crate has no Cargo feature. It needs Linux 5.0 or later with SCTP,
which `sudo modprobe sctp` loads, and a Tokio runtime with the I/O driver.
Its diagnostics go through [`tracing`](https://crates.io/crates/tracing).

Until 0.1.0 is on crates.io, a program takes the crate from its repository:

```toml
[dependencies]
oxirush-sctp = { git = "https://github.com/linouxis9/oxirush-sctp" }
```

The minimum supported Rust version is 1.88.

## Notifications

`recv` returns the notifications of the events that a socket subscribed
to, in the place of data.

```rust
use oxirush_sctp::{
    Event, Notification, NotificationOrData, SendOptions, Socket, SocketToAssociation,
    SubscribeEventAssocId,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Subscribe before listening: the accepted associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    socket.options().subscribe_events(
        &[Event::Association, Event::Shutdown],
        SubscribeEventAssocId::All,
    )?;
    let listener = socket.listen(5)?;

    // A peer that sends one message and closes its socket
    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (peer, _) = peer.connect(listener.local_addr()?).await?;
    peer.send(b"hello", SendOptions::new()).await?;
    drop(peer);

    let (association, _) = listener.accept().await?;
    // `None` is the end of the association
    while let Some(received) = association.recv().await? {
        match received {
            NotificationOrData::Notification(Notification::AssociationChange(change)) => {
                println!("association change: {:?}", change.state)
            }
            NotificationOrData::Notification(Notification::Shutdown(_)) => {
                println!("the peer shuts the association down")
            }
            NotificationOrData::Data(message) => println!("{} octets", message.payload.len()),
            other => println!("{other:?}"),
        }
    }
    Ok(())
}
```

```text
association change: CommUp
5 octets
the peer shuts the association down
association change: ShutdownComplete
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
    let (peer, _) = socket.connect(listener.local_addr()?).await?;
    let (accepted, _) = listener.accept().await?;
    Ok((accepted, peer))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;

    // The peer closes its socket: `None`, at every call from then on
    let (association, peer) = connected(&listener).await?;
    drop(peer);
    for _ in 0..2 {
        println!("closed: {:?}", association.recv().await?);
    }

    // The peer aborts, as a socket does that is dropped with a linger of
    // zero: an error
    let (association, peer) = connected(&listener).await?;
    peer.options().set_linger(Some(Duration::ZERO))?;
    drop(peer);
    let end = association.recv().await.map(|_| ());
    println!("aborted: {:?}", end.map_err(|error| error.kind()));
    Ok(())
}
```

```text
closed: None
closed: None
aborted: Err(ConnectionReset)
```

`recv` returns `None` once an association was shut down, and the error of
one that was aborted or lost, at every call and whichever call learned of
the end first: `while let Some(received) = association.recv().await?` ends
with the association. The documentation has the table of every way an
association ends.

To close without loss, `association.close(deadline).await?` shuts the
association down, reads until the peer has answered and returns what the
peer had still sent. A socket that is dropped is closed by Linux: with a
message or a notification that was not read, the association is aborted,
and what this side had sent and the peer had not acknowledged is lost. A
`OneToManyEndpoint` learns of the end of an association from the
notifications only.

## Multihoming

`bindx` binds several local addresses and `connectx` gives several
addresses of the peer. The addresses of one call share a port. A socket
that connects without being bound offers the peer every address of the
host, loopback and the others: bind the client first, as here, to offer
the addresses that were chosen.

```rust
use oxirush_sctp::{BindxFlags, Socket, SocketToAssociation};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

fn addresses(first: &str, second: &str) -> [SocketAddr; 2] {
    [first.parse().unwrap(), second.parse().unwrap()]
}

fn ips(addresses: &[SocketAddr]) -> Vec<IpAddr> {
    addresses.iter().map(SocketAddr::ip).collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // With port 0 the kernel chooses one port for both addresses
    let server = Socket::new_v4(SocketToAssociation::OneToOne)?;
    server.bindx(&addresses("127.0.0.1:0", "127.0.0.2:0"), BindxFlags::Add)?;
    let listener = server.listen(5)?;
    let server_addresses = listener.local_addrs()?;

    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    client.bindx(&addresses("127.0.0.1:0", "127.0.0.3:0"), BindxFlags::Add)?;
    let (client, id) = client.connectx(&server_addresses).await?;
    let (accepted, _) = listener.accept().await?;

    println!("client: {:?}", ips(&client.local_addrs()?));
    println!("server: {:?}", ips(&client.peer_addrs()?));
    println!("the server sees: {:?}", ips(&accepted.peer_addrs()?));
    println!(
        "primary path: {}",
        client.status()?.peer_primary.address.ip()
    );

    // Probe every path each second instead of each 30 seconds
    for address in client.peer_addrs()? {
        let mut path = client.options().peer_address_params(id, address)?;
        path.heartbeat_interval = Duration::from_secs(1);
        client.options().set_peer_address_params(path)?;
        let path = client.options().peer_address_params(id, address)?;
        println!(
            "{}: heartbeat every {:?}, lost after {} retransmissions",
            address.ip(),
            path.heartbeat_interval,
            path.path_max_retrans
        );
    }
    Ok(())
}
```

```text
client: [127.0.0.1, 127.0.0.3]
server: [127.0.0.1, 127.0.0.2]
the server sees: [127.0.0.1, 127.0.0.3]
primary path: 127.0.0.1
127.0.0.1: heartbeat every 1s, lost after 5 retransmissions
127.0.0.2: heartbeat every 1s, lost after 5 retransmissions
```

## Options and status

Every socket type has `options()`. The sockets implement `AsFd` and
`AsRawFd` for an option that the crate does not wrap.

```rust
use oxirush_sctp::{InitParams, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // What is set before `listen` or `connect` stays, and the accepted
    // associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let options = socket.options();
    options.set_nodelay(true)?; // send small messages at once
    options.set_init_params(InitParams::streams(4, 4))?; // four streams each way
    options.set_max_message_size(64 << 10); // refuse longer received messages
    let listener = socket.listen(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (_peer, _) = peer.connect(listener.local_addr()?).await?;
    let (association, _) = listener.accept().await?;

    let status = association.status()?;
    println!(
        "{:?}, {} outgoing and {} incoming streams",
        status.state, status.outstreams, status.instreams
    );
    let options = association.options();
    println!(
        "nodelay {}, messages of {} octets at most",
        options.nodelay()?,
        options.max_message_size()
    );
    Ok(())
}
```

```text
Established, 4 outgoing and 4 incoming streams
nodelay true, messages of 65536 octets at most
```

## One socket for many associations

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let endpoint = socket.into_endpoint(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (peer, _) = peer.connect(endpoint.local_addr()?).await?;
    peer.send(b"ping", SendOptions::new()).await?;

    // A message comes with its association, which the answer names
    if let NotificationOrData::Data(message) = endpoint.recv().await? {
        println!("endpoint: {:?}", String::from_utf8_lossy(&message.payload));
        let options = SendOptions::new().association(message.assoc_id().unwrap());
        endpoint.send(b"pong", options).await?;
    }
    if let Some(NotificationOrData::Data(message)) = peer.recv().await? {
        println!("peer: {:?}", String::from_utf8_lossy(&message.payload));
    }
    Ok(())
}
```

```text
endpoint: "ping"
peer: "pong"
```

`connect` starts an association without waiting for it, `send_to` sends to
an address, and `peeloff` moves one association to a `ConnectedSocket` of
its own.

## Many messages a call

`send_many` sends a batch, each message with its options, and returns how
many the socket took: all of them unless its buffer filled. The kernel
puts the messages of an association that follow one another in as few
packets as it can. `recv_many` returns whole messages, as `recv` does.

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;
    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (client, _) = client.connect(listener.local_addr()?).await?;
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
    for item in &received {
        if let NotificationOrData::Data(message) = item {
            let text = String::from_utf8_lossy(&message.payload);
            println!("{text:?} on stream {:?}", message.stream_id());
        }
    }
    Ok(())
}
```

```text
"one" on stream Some(0)
"two" on stream Some(1)
"three" on stream Some(1)
```

## A writer and a reader for a socket

[`driver::spawn`](https://docs.rs/oxirush-sctp/latest/oxirush_sctp/driver/)
gives a socket a task that sends what its `Writer` queued, in batches, the
associations of an endpoint in turns, and one that receives. The queue is
in overload when its messages wait: once the shortest wait of an interval
of 100 ms is above 5 ms, as CoDel tells a queue that stands from a burst.
A send then fails at once, or waits for the end of the overload, as
`DriverConfig` says.

```rust
use oxirush_sctp::driver::{self, DriverConfig};
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let listener = socket.listen(5)?;
    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (client, _) = client.connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;

    // A writer, which every task that sends can clone, and a reader
    let (writer, _) = driver::spawn(client, DriverConfig::default())?;
    let (_, mut reader) = driver::spawn(server, DriverConfig::default())?;
    for index in 0..1000_u32 {
        let options = SendOptions::new().ppid(60);
        writer.send(index.to_be_bytes().to_vec(), options).await?;
    }
    let mut last = 0;
    for _ in 0..1000 {
        if let Some(Ok(NotificationOrData::Data(message))) = reader.recv().await {
            last = u32::from_be_bytes(message.payload[..4].try_into().unwrap());
        }
    }
    let overload = writer.overload();
    println!(
        "last {last}, refused {}, in overload {}",
        overload.refused, overload.overloaded
    );
    Ok(())
}
```

```text
last 999, refused 0, in overload false
```

`examples/` has these programs, which `cargo run --example <name>` runs,
and `server`, which serves several associations in a task each.

## Limits

- SCTP itself: the protocol runs in the Linux kernel.
- Other systems than Linux.
- SCTP over DTLS, the data channels of WebRTC.
- The rest of RFC 6458: among others authentication, partial reliability
  and stream reconfiguration.
- A receive buffer for each association of a `OneToManyEndpoint`: they
  share that of the socket, unless `net.sctp.rcvbuf_policy` is 1, and one
  that fills it makes the others wait. A `Listener` gives each association
  a socket of its own.
- A send buffer for each: unless `net.sctp.sndbuf_policy` is 1, Linux
  wakes the endpoint once for each of its associations when one of them has
  data acknowledged. To 100 associations a message took 34 µs of CPU
  on loopback, and to one 11 µs.

## Performance

On an AMD Ryzen 7 5700G (16 threads) with Linux 7.0 and Rust 1.98.1, over the
loopback interface, measured on a quiet machine on 2026-10-18:

| | `send` and `recv` | `send_many` and `recv_many` |
|---|---|---|
| A message of 200 octets sent and received, one at a time, on one thread | 8.0 µs | |
| A message of 64 KiB, the same | 35 µs | |
| Messages of 200 octets from one task to another, on two threads | 163 000 a second, 11 µs each | 377 000 a second, 4.2 µs each |
| The same from 1000 connected sockets to one endpoint | 185 000 a second, 10 µs each | 349 000 a second, 4.5 µs each |
| The same from one endpoint to 100 associations, held back by Linux ([Limits](#limits)) | 54 000 a second, 34 µs each | 56 000 a second, 33 µs each |

The first row is the ping-pong of the comparison below, the second the
lowest of five runs of the `messages` benchmark, and a rate the median of
three runs of `throughput.sh` on cores 2 and 3, each the median of three
rounds. A rate is in messages a second, with the CPU time of the process a
message, the kernel's work for the loopback interface included. Without
`options().set_nodelay(true)` a small message can wait 200 ms for the
acknowledgement of the one before it. The
[driver](https://docs.rs/oxirush-sctp/latest/oxirush_sctp/driver/) carries
what `send_many` carries. [`benches/`](benches/README.md) has the
benchmarks, `throughput.sh` for the rates, and how to profile.

### Against other implementations

The same loops for the SCTP crates of crates.io that run here, at pinned
versions, and for a program in C on the kernel's sockets, on cores 2 and 3
of the same machine. The time is the fastest of five runs in turns, and each
figure the median of three runs of the script; cycles and instructions are a
message, user and kernel, from `perf stat`; the last column is the time
against this crate's `send` and `recv`.
With one message a call every crate is within 13 % of C, as the kernel
takes most of the time; 64 messages a call take two fifths of the time of
one, near C with `sendmmsg` and `recvmmsg`.

| Loop | Implementation | ns a message | Cycles | Instructions | Allocations | Octets allocated | Peak memory | Time ÷ oxirush-sctp |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Ping-pong, 200 octets | C | 7 694 | 35 694 | 25 729 | 0.00 | 0 | 1.9 MiB | 0.96 |
|  | oxirush-sctp | 7 999 | 37 624 | 27 981 | 1.00 | 200 | 3.8 MiB |  |
|  | oxirush-sctp, `recv_into` | 7 846 | 36 770 | 27 505 | 0.00 | 0 | 3.8 MiB | 0.98 |
|  | tokio-sctp 0.2.0 | 8 283 | 38 903 | 27 877 | 0.00 | 0 | 3.7 MiB | 1.04 |
|  | async-sctp 1.0.0 | 8 698 | 40 881 | 31 430 | 1.00 | 65 536 | 3.7 MiB | 1.09 |
|  | lksctp 0.1.0 | 8 137 | 38 262 | 27 373 | 0.00 | 0 | 3.7 MiB | 1.02 |
|  | sctp-rs 0.3.1 | 7 998 | 37 587 | 28 428 | 5.00 | 4 648 | 3.7 MiB | 1.00 |
| 200 octets, one association, two threads | C | 7 045 | 49 551 | 32 047 | 0.00 | 0 | 2.2 MiB | 0.99 |
|  | C, `sendmmsg` and `recvmmsg` | 2 278 | 15 210 | 11 684 | 0.00 | 0 | 2.4 MiB | 0.32 |
|  | oxirush-sctp | 7 118 | 51 647 | 36 053 | 1.00 | 200 | 4.3 MiB |  |
|  | oxirush-sctp, `send_many` and `recv_many` | 2 713 | 18 869 | 14 927 | 1.00 | 201 | 4.3 MiB | 0.38 |
|  | tokio-sctp 0.2.0 | 7 306 | 52 740 | 36 026 | 0.00 | 0 | 4.3 MiB | 1.03 |
|  | async-sctp 1.0.0 | 7 301 | 53 070 | 39 312 | 1.00 | 65 536 | 4.2 MiB | 1.03 |
|  | lksctp 0.1.0 | 7 232 | 51 586 | 35 473 | 0.00 | 0 | 4.3 MiB | 1.02 |
|  | sctp-rs 0.3.1 | 7 283 | 53 084 | 36 727 | 5.00 | 4 648 | 4.1 MiB | 1.02 |

`benches/compare.sh` builds and runs them all; the rows for 16 KiB and for
100 associations are in [`benches/`](benches/README.md).

## Tests and fuzzing

```sh
sudo modprobe sctp
cargo test
```

The tests open associations over the loopback interface. CI fuzzes three
`cargo fuzz` targets for a minute each: what the kernel hands over is read
without a panic.

[The documentation](https://docs.rs/oxirush-sctp) has the terms, the
sections of RFC 6458 that the crate covers, the socket types, and the rules
of `send` and `recv`.

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
