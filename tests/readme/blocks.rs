// The Rust blocks of the README and of the documentation, each as a function.
// `tests/readme.rs` writes this file and says how: do not edit it.
#![allow(unused)]

use std::cell::RefCell;

thread_local!(static PRINTED: RefCell<String> = const { RefCell::new(String::new()) });

// What a block prints is kept, to be compared with what its page shows.
macro_rules! println {
    () => {
        PRINTED.with(|printed| printed.borrow_mut().push('\n'))
    };
    // The line is made first, where the block is: its arguments may have a
    // `?` or an `await`.
    ($($argument:tt)*) => {{
        let line = format!($($argument)*);
        PRINTED.with(|printed| {
            let mut printed = printed.borrow_mut();
            printed.push_str(&line);
            printed.push('\n');
        })
    }};
}

/// What `block` prints.
pub fn printed(block: fn()) -> String {
    PRINTED.with(|printed| printed.borrow_mut().clear());
    block();
    PRINTED.with(|printed| printed.take())
}

pub const BLOCKS: &[fn()] = &[
    block_1,
    block_2,
    block_3,
    block_4,
    block_5,
    block_6,
    block_7,
    block_8,
    block_9,
];

/// The Rust block 1 of README.md.
fn block_1() {
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
    main().unwrap();
}

/// The Rust block 2 of README.md.
fn block_2() {
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
                streams[usize::from(message.stream_id().unwrap())] += 1;
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
    main().unwrap();
}

/// The Rust block 3 of README.md.
fn block_3() {
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
    main().unwrap();
}

/// The Rust block 4 of README.md.
fn block_4() {
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
    main().unwrap();
}

/// The Rust block 5 of README.md.
fn block_5() {
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
    main().unwrap();
}

/// The Rust block 6 of README.md.
fn block_6() {
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
    main().unwrap();
}

/// The Rust block 7 of README.md.
fn block_7() {
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
            let options = SendOptions::new().association(message.assoc_id().unwrap());
            endpoint.send(b"pong", options).await?;
        }
        if let Some(NotificationOrData::Data(message)) = peer.recv().await? {
            println!("peer: {:?}", String::from_utf8_lossy(&message.payload));
        }
        Ok(())
    }
    main().unwrap();
}

/// The Rust block 8 of README.md.
fn block_8() {
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
    main().unwrap();
}

/// The Rust block 1 of src/lib.rs.
fn block_9() {
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
    main().unwrap();
}
