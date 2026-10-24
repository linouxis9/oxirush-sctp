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
    Ok(server.and(client)?)
}
```

```text
server: "ping" on stream Some(1) with PPID Some(60)
client: "pong"
```

Each example is followed by its output.

This page describes oxirush-sctp 0.1.0, which is not on crates.io yet.

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
    let (gnb, _) = gnb.connect(listener.local_addr()?).await?;
    let (amf, _) = listener.accept().await?;
    let status = amf.status()?;
    println!("{:?}, {} streams each way", status.state, status.outstreams);

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
            streams[usize::from(message.stream_id().ok_or("no stream")?)] += 1;
        }
    }
    println!("AMF: {streams:?} messages on streams 0 to 3");
    amf_writer
        .send(b"NG SETUP RESPONSE".to_vec(), setup)
        .await?;
    if let Some(Ok(NotificationOrData::Data(message))) = gnb_reader.recv().await {
        println!("gNB: {:?}", String::from_utf8_lossy(&message.payload));
    }

    // What the writer sent and refused, and whether its queue is in overload
    let overload = gnb_writer.overload();
    println!(
        "gNB writer: {} sent, {} refused, in overload {}",
        overload.sent, overload.refused, overload.overloaded
    );
    Ok(())
}
```

```text
Established, 4 streams each way
AMF: [1, 334, 333, 333] messages on streams 0 to 3
gNB: "NG SETUP RESPONSE"
gNB writer: 1001 sent, 0 refused, in overload false
```

`cargo run --example driver` runs this program, and `examples/` has the
other programs of this page, except the next one. `examples/server.rs` serves
several associations, each in a task of its own.

## NG SETUP over SCTP

An association carries the messages of its protocol: here the NG SETUP of
[oxirush-ngap](https://github.com/linouxis9/oxirush-ngap), on stream 0,
with the payload protocol identifier of NGAP, 60 (TS 38.412).

```rust,ignore
use oxirush_ngap::helpers::{global_gnb_id, guami, plmn, s_nssai};
use oxirush_ngap::{build_ngap, ngap::*};
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ngap = SendOptions::new().stream(0).ppid(60);
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let listener = socket.listen(5)?;
    let gnb = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (gnb, _) = gnb.connect(listener.local_addr()?).await?;
    let (amf, _) = listener.accept().await?;

    // The gNB: NG SETUP REQUEST, its identity and the slices of its tracking area
    let network = plmn("208", "93")?;
    let slices = || SliceSupportList(vec![SliceSupportItem::new(s_nssai(1, None), None)]);
    let plmns = BroadcastPLMNList(vec![BroadcastPLMNItem::new(network, slices(), None)]);
    let area = SupportedTAItem::new(TAC([0, 0, 1]), plmns, None);
    let request = build_ngap!(InitiatingMessage, NGSetup, NGSetupRequest,
        GlobalRANNodeID(GlobalRANNodeID::globalGNB_ID(global_gnb_id(network, 0x123456, 24)?)),
        SupportedTAList(SupportedTAList(vec![area])),
        DefaultPagingDRX(PagingDRX::v128),
    )?;
    gnb.send(&request.encode()?, ngap).await?;

    // The AMF: the PDU of each message, and an answer on the same stream
    if let Some(NotificationOrData::Data(message)) = amf.recv().await? {
        let pdu = NGAP_PDU::decode(&message.payload)?;
        let (stream, ppid) = (message.stream_id(), message.ppid());
        println!("AMF: {:?} on stream {stream:?}, PPID {ppid:?}", pdu.kind());
        let served = ServedGUAMIItem::new(guami(network, 1, 1, 0)?, None, None);
        let response = build_ngap!(SuccessfulOutcome, NGSetup, NGSetupResponse,
            AMFName("amf-1"),
            ServedGUAMIList(ServedGUAMIList(vec![served])),
            RelativeAMFCapacity(RelativeAMFCapacity(255)),
            PLMNSupportList(PLMNSupportList(vec![PLMNSupportItem::new(network, slices(), None)])),
        )?;
        amf.send(&response.encode()?, ngap).await?;
    }
    if let Some(NotificationOrData::Data(message)) = gnb.recv().await? {
        println!("gNB: {:?}", NGAP_PDU::decode(&message.payload)?.kind());
    }
    Ok(())
}
```

```text
AMF: Initiating_NGSetup on stream Some(0), PPID Some(60)
gNB: Successful_NGSetup
```

## Notifications

`recv` returns the notifications of the events that a socket subscribed
to, in place of data.

```rust
use oxirush_sctp::{
    Event, Notification, NotificationOrData, SendOptions, Socket, SocketToAssociation,
    SubscribeEventAssocId,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Subscribe before listening: the accepted associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Socket::new_v4(SocketToAssociation::OneToMany)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    let endpoint = socket.into_endpoint(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (peer, _) = peer.connect(endpoint.local_addr()?).await?;
    peer.send(b"ping", SendOptions::new()).await?;

    // A message comes with its association, which the answer names
    if let NotificationOrData::Data(message) = endpoint.recv().await? {
        println!("endpoint: {:?}", String::from_utf8_lossy(&message.payload));
        let options = SendOptions::new().association(message.assoc_id().ok_or("no association")?);
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

## Limits

- It does not implement SCTP: the protocol runs in the Linux kernel.
- It runs on Linux alone.
- It has no SCTP over DTLS, which the data channels of WebRTC use.
- It does not cover the rest of RFC 6458: among others authentication,
  partial reliability and stream reconfiguration.
- The associations of a `OneToManyEndpoint` share the receive buffer of the
  socket, unless `net.sctp.rcvbuf_policy` is 1: one that fills it makes the
  others wait. A `Listener` gives each association a socket of its own.
- They share its send buffer too: unless `net.sctp.sndbuf_policy` is 1,
  Linux wakes the endpoint once for each of its associations when one of
  them has data acknowledged. To 100 associations, a message took 34 µs of
  CPU on loopback, and to one, 11 µs.

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

The tests open associations over the loopback interface. CI runs three
`cargo fuzz` targets for a minute each: what the kernel hands over is read
without a panic. Each Rust block of this page is checked with what it prints
([`tests/readme.rs`](tests/readme.rs)). The block that uses another crate of
the family runs in the package of [`family/`](family/), which takes that
crate at the revision that its manifest names.

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
