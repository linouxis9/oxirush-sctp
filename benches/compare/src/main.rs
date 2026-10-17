//! One loop for oxirush-sctp and for each SCTP crate of crates.io that runs here, over
//! loopback: what `../compare.sh` times.
//!
//! ```bash
//! compare <implementation> pingpong <octets> <messages>
//! compare <implementation> stream <octets> <messages> <associations>
//! ```
//!
//! `pingpong` sends a message on one end of an association and receives it on the other, on
//! one thread, one message at a time. `stream` sends the messages from one task, in turns to
//! each association, and receives them in a task for each association, on two threads; the
//! sender keeps at most 4096 messages ahead of the receivers. Every socket has `SCTP_NODELAY`
//! and a receive buffer of 4 MiB, set on its descriptor, for the crates that have no option
//! for them. It prints the time a message, the allocations and their octets a message, and
//! the peak of the resident memory.
//!
//! The implementations: `oxirush-sctp` (`send` and `recv`), `oxirush-sctp-into` (`recv_into`),
//! `oxirush-sctp-many` (`send_many` and `recv_many`, 64 a call, `stream` only),
//! `tokio-sctp` 0.2.0, `async-sctp` 1.0.0, `lksctp` 0.1.0 and `sctp-rs` 0.3.1.

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

/// Counts what is allocated, which the loops divide by their messages.
struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);

fn count(octets: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED.fetch_add(octets as u64, Ordering::Relaxed);
}

// Safety: every call goes to the allocator of the system, as it came.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const LOOPBACK: &str = "127.0.0.1:0";
const RECEIVE_BUFFER: libc::c_int = 4 << 20;
const WINDOW: usize = 4096;
const BATCH: usize = 64;

fn loopback() -> SocketAddr {
    LOOPBACK.parse().unwrap()
}

/// `SCTP_NODELAY` and a receive buffer of 4 MiB on every SCTP socket of the process.
fn tune_every_socket() {
    for fd in 0..65536 {
        let mut protocol: libc::c_int = 0;
        let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // Safety: the option writes one integer into `protocol`.
        let found = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PROTOCOL,
                std::ptr::from_mut(&mut protocol).cast(),
                &mut length,
            )
        } == 0;
        if !found || protocol != libc::IPPROTO_SCTP {
            continue;
        }
        let set = |level, name, value: libc::c_int| {
            // Safety: the option reads one integer.
            unsafe {
                libc::setsockopt(
                    fd,
                    level,
                    name,
                    std::ptr::from_ref(&value).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            }
        };
        // SCTP_NODELAY is 3
        set(libc::IPPROTO_SCTP, 3, 1);
        set(libc::SOL_SOCKET, libc::SO_RCVBUF, RECEIVE_BUFFER);
    }
}

/// One end of an association, as each crate has it.
trait Sctp: Sized + Send + Sync + 'static {
    /// A connected pair on 127.0.0.1.
    fn pair() -> impl Future<Output = std::io::Result<(Self, Self)>>;
    fn send(&self, message: &[u8]) -> impl Future<Output = std::io::Result<()>> + Send;
    /// The octets of the next message, into `buffer` where the crate reads into one.
    fn recv(&self, buffer: &mut Vec<u8>) -> impl Future<Output = std::io::Result<usize>> + Send;
}

async fn oxirush_pair()
-> std::io::Result<(oxirush_sctp::ConnectedSocket, oxirush_sctp::ConnectedSocket)> {
    use oxirush_sctp::{Socket, SocketToAssociation};
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind(loopback())?;
    let listener = socket.listen(1024)?;
    let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    let (client, _) = client.connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;
    Ok((client, server))
}

fn oxirush_options() -> oxirush_sctp::SendOptions {
    oxirush_sctp::SendOptions::new().stream(1).ppid(60)
}

struct Oxirush(oxirush_sctp::ConnectedSocket);

impl Sctp for Oxirush {
    async fn pair() -> std::io::Result<(Self, Self)> {
        let (client, server) = oxirush_pair().await?;
        Ok((Self(client), Self(server)))
    }

    fn send(&self, message: &[u8]) -> impl Future<Output = std::io::Result<()>> + Send {
        self.0.send(message, oxirush_options())
    }

    async fn recv(&self, _: &mut Vec<u8>) -> std::io::Result<usize> {
        match self.0.recv().await? {
            Some(oxirush_sctp::NotificationOrData::Data(message)) => Ok(message.payload.len()),
            other => Err(std::io::Error::other(format!("{other:?}"))),
        }
    }
}

struct OxirushInto(oxirush_sctp::ConnectedSocket);

impl Sctp for OxirushInto {
    async fn pair() -> std::io::Result<(Self, Self)> {
        let (client, server) = oxirush_pair().await?;
        Ok((Self(client), Self(server)))
    }

    fn send(&self, message: &[u8]) -> impl Future<Output = std::io::Result<()>> + Send {
        self.0.send(message, oxirush_options())
    }

    async fn recv(&self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        buffer.resize(1 << 17, 0);
        match self.0.recv_into(buffer).await? {
            Some(oxirush_sctp::ReceivedInto::Data { len, .. }) => Ok(len),
            other => Err(std::io::Error::other(format!("{other:?}"))),
        }
    }
}

struct TokioSctp(tokio_sctp::SctpStream);

fn tokio_sctp_options() -> tokio_sctp::SendOptions {
    tokio_sctp::SendOptions {
        ppid: 60,
        stream: 1,
        ..Default::default()
    }
}

impl Sctp for TokioSctp {
    async fn pair() -> std::io::Result<(Self, Self)> {
        let listener = tokio_sctp::SctpListener::bind(loopback())?;
        let address = listener.local_addr()?;
        let (client, accepted) =
            tokio::join!(tokio_sctp::SctpStream::connect(address), listener.accept());
        Ok((Self(client?), Self(accepted?.0)))
    }

    async fn send(&self, message: &[u8]) -> std::io::Result<()> {
        self.0.sendmsg(message, None, &tokio_sctp_options()).await?;
        Ok(())
    }

    async fn recv(&self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        buffer.clear();
        Ok(self.0.recvmsg_eor_buf(buffer).await?.0)
    }
}

struct AsyncSctp(async_sctp::SctpAssociation);

impl Sctp for AsyncSctp {
    async fn pair() -> std::io::Result<(Self, Self)> {
        let config = async_sctp::SctpConfig::new();
        let other = |error: async_sctp::SctpError| std::io::Error::other(error.to_string());
        let listener = async_sctp::SctpListener::bind_config(loopback(), &config).map_err(other)?;
        let address = listener.local_addr().map_err(other)?;
        let (client, accepted) = tokio::join!(
            async_sctp::SctpAssociation::connect_with(address, &config),
            listener.accept()
        );
        Ok((
            Self(client.map_err(other)?),
            Self(accepted.map_err(other)?.0),
        ))
    }

    async fn send(&self, message: &[u8]) -> std::io::Result<()> {
        self.0
            .send(message, 1, 60)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(())
    }

    async fn recv(&self, _: &mut Vec<u8>) -> std::io::Result<usize> {
        let (data, _) = self
            .0
            .recv()
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(data.len())
    }
}

struct Lksctp(lksctp::SctpStream);

impl Sctp for Lksctp {
    async fn pair() -> std::io::Result<(Self, Self)> {
        let listener = lksctp::SctpListener::bind(loopback())?;
        let address = listener.local_addr()?;
        let builder = lksctp::SctpStream::builder(lksctp::Family::Ipv4)?;
        let addresses = [address];
        let (client, accepted) = tokio::join!(builder.connectx(&addresses), listener.accept());
        Ok((Self(client?), Self(accepted?.0)))
    }

    async fn send(&self, message: &[u8]) -> std::io::Result<()> {
        let info = lksctp::SendInfo {
            sid: 1,
            ppid: lksctp::Ppid(60),
            ..Default::default()
        };
        self.0.send_msg(message, &info).await?;
        Ok(())
    }

    async fn recv(&self, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        buffer.resize(1 << 17, 0);
        match self.0.recv_msg(buffer).await? {
            lksctp::RecvMsg::Data { len, eor: true, .. } => Ok(len),
            _ => Err(std::io::Error::other("not a whole message")),
        }
    }
}

struct SctpRs(sctp_rs::ConnectedSocket);

impl Sctp for SctpRs {
    async fn pair() -> std::io::Result<(Self, Self)> {
        use sctp_rs::{Socket, SocketToAssociation};
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        socket.bind(loopback())?;
        let listener = socket.listen(1024)?;
        let address = listener.sctp_getladdrs(0)?[0];
        let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let (client, _) = client.connect(address).await?;
        let (server, _) = listener.accept().await?;
        Ok((Self(client), Self(server)))
    }

    async fn send(&self, message: &[u8]) -> std::io::Result<()> {
        let info = sctp_rs::SendInfo {
            sid: 1,
            ppid: 60_u32.to_be(),
            ..Default::default()
        };
        self.0
            .sctp_send(sctp_rs::SendData {
                payload: message.to_vec(),
                snd_info: Some(info),
            })
            .await
    }

    async fn recv(&self, _: &mut Vec<u8>) -> std::io::Result<usize> {
        match self.0.sctp_recv().await? {
            sctp_rs::NotificationOrData::Data(data) => Ok(data.payload.len()),
            _ => Err(std::io::Error::other("a notification")),
        }
    }
}

/// What the receivers have received, which the sender waits on.
#[derive(Default)]
struct Window {
    received: AtomicUsize,
    progress: Notify,
}

impl Window {
    fn received(&self, count: usize) {
        self.received.fetch_add(count, Ordering::Relaxed);
        self.progress.notify_one();
    }

    async fn wait(&self, sent: usize) {
        while sent > self.received.load(Ordering::Relaxed) + WINDOW {
            self.progress.notified().await;
        }
    }
}

/// When a loop started, and what had been allocated.
struct Start {
    at: Instant,
    allocations: u64,
    allocated: u64,
}

impl Start {
    fn now() -> Self {
        Self {
            at: Instant::now(),
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            allocated: ALLOCATED.load(Ordering::Relaxed),
        }
    }

    /// The time, the allocations and their octets, a message.
    fn per_message(&self, messages: usize) -> (f64, f64, f64) {
        let messages = messages as f64;
        (
            self.at.elapsed().as_nanos() as f64 / messages,
            (ALLOCATIONS.load(Ordering::Relaxed) - self.allocations) as f64 / messages,
            (ALLOCATED.load(Ordering::Relaxed) - self.allocated) as f64 / messages,
        )
    }
}

fn pingpong<S: Sctp>(octets: usize, messages: usize) -> std::io::Result<(f64, f64, f64)> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (sender, receiver) = S::pair().await?;
        tune_every_socket();
        let payload = vec![0x5a; octets];
        let mut buffer = Vec::with_capacity(1 << 17);
        for _ in 0..1000 {
            sender.send(&payload).await?;
            receiver.recv(&mut buffer).await?;
        }
        let start = Start::now();
        for _ in 0..messages {
            sender.send(&payload).await?;
            let got = receiver.recv(&mut buffer).await?;
            if got != octets {
                return Err(std::io::Error::other(format!("{got} octets of {octets}")));
            }
        }
        Ok(start.per_message(messages))
    })
}

fn two_threads() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
}

/// The share of the messages of association `index`.
fn share(messages: usize, associations: usize, index: usize) -> usize {
    messages / associations + usize::from(index < messages % associations)
}

fn stream<S: Sctp>(
    octets: usize,
    messages: usize,
    associations: usize,
) -> std::io::Result<(f64, f64, f64)> {
    let runtime = two_threads()?;
    runtime.block_on(async move {
        let mut senders = Vec::with_capacity(associations);
        let mut receivers = Vec::with_capacity(associations);
        for _ in 0..associations {
            let (sender, receiver) = S::pair().await?;
            senders.push(sender);
            receivers.push(receiver);
        }
        tune_every_socket();
        let window = Arc::new(Window::default());
        let start = Start::now();
        let tasks: Vec<_> = receivers
            .into_iter()
            .enumerate()
            .map(|(index, receiver)| {
                let window = window.clone();
                let share = share(messages, associations, index);
                tokio::spawn(async move {
                    let mut buffer = Vec::with_capacity(1 << 17);
                    for _ in 0..share {
                        let got = receiver.recv(&mut buffer).await?;
                        if got != octets {
                            return Err(std::io::Error::other(format!("{got} octets of {octets}")));
                        }
                        window.received(1);
                    }
                    Ok(receiver)
                })
            })
            .collect();
        let payload = vec![0x5a; octets];
        for index in 0..messages {
            window.wait(index).await;
            senders[index % associations].send(&payload).await?;
        }
        let mut kept = Vec::with_capacity(tasks.len());
        for task in tasks {
            kept.push(task.await.map_err(std::io::Error::other)??);
        }
        let measured = start.per_message(messages);
        drop((senders, kept));
        Ok(measured)
    })
}

/// `stream` with `send_many` and `recv_many`: a batch to each association in turn.
fn stream_many(
    octets: usize,
    messages: usize,
    associations: usize,
) -> std::io::Result<(f64, f64, f64)> {
    let runtime = two_threads()?;
    runtime.block_on(async move {
        let mut senders = Vec::with_capacity(associations);
        let mut receivers = Vec::with_capacity(associations);
        for _ in 0..associations {
            let (sender, receiver) = oxirush_pair().await?;
            senders.push(sender);
            receivers.push(receiver);
        }
        tune_every_socket();
        let window = Arc::new(Window::default());
        let start = Start::now();
        let tasks: Vec<_> = receivers
            .into_iter()
            .enumerate()
            .map(|(index, receiver)| {
                let window = window.clone();
                let share = share(messages, associations, index);
                tokio::spawn(async move {
                    let mut out = Vec::with_capacity(BATCH);
                    let mut received = 0;
                    while received < share {
                        out.clear();
                        let got = receiver.recv_many(&mut out, BATCH).await?;
                        received += got;
                        window.received(got);
                    }
                    std::io::Result::Ok(receiver)
                })
            })
            .collect();
        let payload = vec![0x5a; octets];
        let batch: Vec<(&[u8], oxirush_sctp::SendOptions)> =
            vec![(payload.as_slice(), oxirush_options()); BATCH];
        let mut left: Vec<usize> = (0..associations)
            .map(|index| share(messages, associations, index))
            .collect();
        let mut sent = 0;
        while sent < messages {
            for (sender, left) in senders.iter().zip(&mut left) {
                if *left > 0 {
                    window.wait(sent).await;
                    let count = sender.send_many(&batch[..BATCH.min(*left)]).await?;
                    *left -= count;
                    sent += count;
                }
            }
        }
        let mut kept = Vec::with_capacity(tasks.len());
        for task in tasks {
            kept.push(task.await.map_err(std::io::Error::other)??);
        }
        let measured = start.per_message(messages);
        drop((senders, kept));
        Ok(measured)
    })
}

/// The peak of the resident memory of the process, in KiB.
fn peak() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| value.trim().trim_end_matches("kB").trim().parse().ok())
        })
        .unwrap_or(0)
}

fn run<S: Sctp>(
    case: &str,
    octets: usize,
    messages: usize,
    associations: usize,
) -> std::io::Result<(f64, f64, f64)> {
    match case {
        "pingpong" => pingpong::<S>(octets, messages),
        "stream" => stream::<S>(octets, messages, associations),
        _ => panic!("no case {case}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (implementation, case) = (args[0].as_str(), args[1].as_str());
    let octets: usize = args[2].parse().unwrap();
    let messages: usize = args[3].parse().unwrap();
    let associations: usize = args.get(4).map_or(1, |value| value.parse().unwrap());
    let measured = match implementation {
        "oxirush-sctp" => run::<Oxirush>(case, octets, messages, associations),
        "oxirush-sctp-into" => run::<OxirushInto>(case, octets, messages, associations),
        "oxirush-sctp-many" if case == "stream" => stream_many(octets, messages, associations),
        "tokio-sctp" => run::<TokioSctp>(case, octets, messages, associations),
        "async-sctp" => run::<AsyncSctp>(case, octets, messages, associations),
        "lksctp" => run::<Lksctp>(case, octets, messages, associations),
        "sctp-rs" => run::<SctpRs>(case, octets, messages, associations),
        _ => panic!("no implementation {implementation} for {case}"),
    };
    match measured {
        Ok((time, allocations, allocated)) => println!(
            "{implementation} {case} {octets} {messages} {associations}: {time:.0} ns, \
             {allocations:.2} allocations, {allocated:.0} octets, peak {} KiB",
            peak()
        ),
        Err(error) => {
            println!("{implementation} {case} {octets} {messages} {associations}: fails: {error}");
            std::process::exit(1);
        }
    }
}
