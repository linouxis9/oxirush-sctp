# oxirush-sctp

[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](https://github.com/linouxis9/oxirush-sctp/blob/main/COPYRIGHT)

Async SCTP sockets for [Tokio](https://tokio.rs) on Linux, over the SCTP of
the kernel and its sockets API
([RFC 6458](https://www.rfc-editor.org/rfc/rfc6458.html)). `send` and `recv`
move one whole message, with its stream and its payload protocol identifier.

```rust
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Stream 1, and the payload protocol identifier of NGAP
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
        let (association, _peer) = listener.accept().await?;
        if let NotificationOrData::Data(message) = association.recv().await? {
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
        if let NotificationOrData::Data(message) = association.recv().await? {
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

The text under a block is what the block prints.

> This crate is not on crates.io yet.

## Install

```toml
[dependencies]
oxirush-sctp = { git = "https://github.com/linouxis9/oxirush-sctp" }
tokio = { version = "1", features = ["macros", "rt"] }
```

It needs Linux 5.0 or later with SCTP, which `sudo modprobe sctp` loads, a
Tokio runtime with the I/O driver, and Rust 1.88 or later.

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
    peer.send(b"hello", SendOptions::default()).await?;
    drop(peer);

    let (association, _) = listener.accept().await?;
    loop {
        match association.recv().await? {
            NotificationOrData::Notification(Notification::AssociationChange(change)) => {
                println!("association change: {:?}", change.state)
            }
            NotificationOrData::Notification(Notification::Shutdown(_)) => {
                println!("the peer shuts the association down")
            }
            NotificationOrData::Notification(other) => println!("{other:?}"),
            // An empty message is the end of the association
            NotificationOrData::Data(message) if message.payload.is_empty() => break,
            NotificationOrData::Data(message) => println!("{} octets", message.payload.len()),
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
use oxirush_sctp::{ConnectedSocket, Listener, NotificationOrData, Socket, SocketToAssociation};
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

    // The peer closes its socket: an empty message, at every call from then on
    let (association, peer) = connected(&listener).await?;
    drop(peer);
    for _ in 0..2 {
        if let NotificationOrData::Data(message) = association.recv().await? {
            println!("closed: a message of {} octets", message.payload.len());
        }
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
closed: a message of 0 octets
closed: a message of 0 octets
aborted: Err(ConnectionReset)
```

A dropped socket shuts its association down, and aborts it with a linger of
zero. A `OneToManyEndpoint` learns of the end of an association from the
notifications only.

## Multihoming

`bindx` binds several local addresses and `connectx` gives several
addresses of the peer. The addresses of one call share a port.

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
    let server_addresses = listener.local_addrs(0)?;

    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    client.bindx(&addresses("127.0.0.1:0", "127.0.0.3:0"), BindxFlags::Add)?;
    let (client, id) = client.connectx(&server_addresses).await?;
    let (accepted, _) = listener.accept().await?;

    println!("client: {:?}", ips(&client.local_addrs(id)?));
    println!("server: {:?}", ips(&client.peer_addrs(id)?));
    println!("the server sees: {:?}", ips(&accepted.peer_addrs(0)?));
    println!(
        "primary path: {}",
        client.status(id)?.peer_primary.address.ip()
    );

    // Probe every path each second instead of each 30 seconds
    for address in client.peer_addrs(id)? {
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
use oxirush_sctp::{Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // What is set before `listen` or `connect` stays, and the accepted
    // associations inherit it
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let options = socket.options();
    options.set_nodelay(true)?; // send small messages at once
    options.set_init_params(4, 4, 0, 0)?; // four streams each way
    options.set_max_message_size(64 << 10); // refuse longer received messages
    let listener = socket.listen(5)?;

    let peer = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (_peer, _) = peer.connect(listener.local_addr()?).await?;
    let (association, _) = listener.accept().await?;

    let status = association.status(0)?;
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
    peer.send(b"ping", SendOptions::default()).await?;

    // A message comes with its association, which the answer names
    if let NotificationOrData::Data(message) = endpoint.recv().await? {
        println!("endpoint: {:?}", String::from_utf8_lossy(&message.payload));
        let options = SendOptions {
            assoc_id: message.assoc_id().unwrap(),
            ..Default::default()
        };
        endpoint.send(b"pong", options).await?;
    }
    if let NotificationOrData::Data(message) = peer.recv().await? {
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

`examples/` has these programs, which `cargo run --example <name>` runs,
and `server`, which serves several associations in a task each.

## Features

The crate has no Cargo feature. Its diagnostics go through
[`tracing`](https://crates.io/crates/tracing).

## What it does not do

- SCTP itself: the protocol runs in the Linux kernel.
- Other systems than Linux.
- SCTP over DTLS, the data channels of WebRTC.
- The rest of RFC 6458: among others authentication, partial reliability
  and the choice of the primary path.

## Performance

On one core of an AMD Ryzen 7 5700G, a message sent on one end of an
association and received on the other, over the loopback interface, takes
9 µs for 64 or 1400 octets and 40 µs for 64 KiB. Without
`options().set_nodelay(true)` a small message can wait 200 ms for the
acknowledgement of the one before it.
[`benches/`](https://github.com/linouxis9/oxirush-sctp/blob/main/benches/README.md)
has the benchmarks and how to profile.

## Tests and fuzzing

```sh
sudo modprobe sctp
cargo test
```

The tests open associations over the loopback interface. CI fuzzes three
`cargo fuzz` targets for a minute each: what the kernel hands over is read
without a panic.

[The documentation](https://github.com/linouxis9/oxirush-sctp/blob/main/src/lib.rs)
has the terms, the sections of RFC 6458 that the crate covers, the socket
types, and the rules of `send` and `recv`: `cargo doc --open` builds it.

## The OxiRush family

[oxirush-nas](https://github.com/linouxis9/oxirush-nas) (NAS messages),
[oxirush-security](https://github.com/linouxis9/oxirush-security) (NAS
security), [oxirush-ngap](https://github.com/linouxis9/oxirush-ngap)
(NGAP), [oxirush-s1ap](https://github.com/linouxis9/oxirush-s1ap) (S1AP)
and [oxirush-gtp-u](https://github.com/linouxis9/oxirush-gtp-u) (GTP-U).
NGAP and S1AP are carried over SCTP.

## Contributing

Open a pull request with a test for what it changes. The programs of this
page are those of `examples/`, and
[`tests/readme.rs`](https://github.com/linouxis9/oxirush-sctp/blob/main/tests/readme.rs)
checks what they print. Sign off your commits (`git commit -s`): by
contributing you agree to the
[Developer Certificate of Origin](https://developercertificate.org/).
[SECURITY.md](https://github.com/linouxis9/oxirush-sctp/blob/main/SECURITY.md)
says how to report a vulnerability.

## License

Licensed under either of the Apache License, Version 2.0
([LICENSE-APACHE](https://github.com/linouxis9/oxirush-sctp/blob/main/LICENSE-APACHE))
or the MIT License
([LICENSE-MIT](https://github.com/linouxis9/oxirush-sctp/blob/main/LICENSE-MIT)),
at your option.

oxirush-sctp started as a fork of [sctp-rs](https://github.com/gabhijit/ellora).
