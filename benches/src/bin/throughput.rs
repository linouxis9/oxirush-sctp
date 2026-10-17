//! Messages a second through SCTP associations over loopback: many small messages, as NGAP
//! has, sent by one thread and received by another, each with a runtime of its own.
//!
//! ```bash
//! cargo run --release --bin throughput -- pair 200 1000000 1 64
//! ```
//!
//! The arguments are the scenario, the octets of a message, the messages, the associations
//! and the batch. A batch of 1 is `send` and `recv`, one message a call; a larger one is
//! `send_many` and `recv_many`, up to that many a call; `64:1` is a batch of 64 for the
//! senders and of 1 for the receivers. The scenarios:
//!
//! - `pair`: one association, from a connected socket to another;
//! - `to-endpoint`: one connected socket for each association, which send in turns, to one
//!   endpoint, which receives;
//! - `from-endpoint`: one endpoint, which sends to its associations in turns, to a connected
//!   socket for each, which receive.
//!
//! It prints the messages a second, the time and the CPU time of the process a message, user
//! and system, from when the associations are up.

use oxirush_sctp::{
    ConnectedSocket, NotificationOrData, OneToManyEndpoint, SendOptions, Socket,
    SocketToAssociation,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

/// The messages that the senders have ahead of the receivers at most, all associations
/// together: past it a sender waits. Without it, senders that are faster than the receiver
/// fill its receive buffer, Linux drops what comes past it, and the run times retransmission
/// timeouts of a second instead of calls.
const WINDOW: usize = 4096;

/// What the receivers have received, which the senders wait on.
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

    /// Wait until `sent` is within the window of what was received.
    async fn wait(&self, sent: usize) {
        while sent > self.received.load(Ordering::Relaxed) + WINDOW {
            self.progress.notified().await;
        }
    }
}

struct Run {
    octets: usize,
    messages: usize,
    associations: usize,
    send_batch: usize,
    receive_batch: usize,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [scenario, octets, messages, associations, batch] = &args[..] else {
        eprintln!(
            "usage: throughput <pair|to-endpoint|from-endpoint> <octets> <messages> \
             <associations> <batch>"
        );
        std::process::exit(2);
    };
    let run = Run {
        octets: octets.parse().unwrap(),
        messages: messages.parse().unwrap(),
        associations: associations.parse().unwrap(),
        send_batch: batch.split(':').next().unwrap().parse().unwrap(),
        receive_batch: batch.split(':').next_back().unwrap().parse().unwrap(),
    };
    let (elapsed, cpu) = match scenario.as_str() {
        "pair" => pair(&run),
        "to-endpoint" => to_endpoint(&run),
        "from-endpoint" => from_endpoint(&run),
        _ => panic!("no scenario {scenario}"),
    };
    println!(
        "{scenario} octets {} messages {} associations {} batch {}:{}: \
         {:.0} messages/s, {:.3} s, {:.2} us of CPU a message",
        run.octets,
        run.messages,
        run.associations,
        run.send_batch,
        run.receive_batch,
        run.messages as f64 / elapsed,
        elapsed,
        cpu * 1e6 / run.messages as f64
    );
}

/// The CPU time of the process so far, user and system, in seconds: the kernel's work for
/// the loopback interface is done mostly on the sender's thread, and counted here.
fn cpu() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` is a valid place for one `rusage`.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// Two threads, one for the sending task and one for the receiving one, as an application
/// that serves many tasks has.
fn new_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// The receive buffer of every socket, 4 MiB, which Linux doubles if `net.core.rmem_max` lets
/// it. Linux counts several times the octets of a small message against it, and drops what
/// comes past it, for a retransmission timeout of a second: with the default of 208 KiB, an
/// endpoint that reads one message a call falls behind 100 senders by that much.
const RECEIVE_BUFFER: usize = 4 << 20;

/// A listening socket of each kind on 127.0.0.1.
fn listen(style: SocketToAssociation) -> Socket {
    let socket = Socket::new_v4(style).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket.options().set_nodelay(true).unwrap();
    socket
        .options()
        .set_recv_buffer_size(RECEIVE_BUFFER)
        .unwrap();
    socket
}

async fn connect(address: SocketAddr) -> ConnectedSocket {
    let socket = Socket::new_v4(SocketToAssociation::OneToOne).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket.options().set_nodelay(true).unwrap();
    socket
        .options()
        .set_recv_buffer_size(RECEIVE_BUFFER)
        .unwrap();
    let (socket, _) = socket.connect(address).await.unwrap();
    socket
}

/// Send `count` messages on `socket`, `batch` a call; `total` counts what all senders sent.
async fn send_on(
    socket: &ConnectedSocket,
    payload: &[u8],
    count: usize,
    batch: usize,
    window: &Window,
    total: &mut usize,
) {
    let options = SendOptions::new().ppid(60);
    if batch == 1 {
        for _ in 0..count {
            window.wait(*total).await;
            socket.send(payload, options).await.unwrap();
            *total += 1;
        }
        return;
    }
    let messages: Vec<(&[u8], SendOptions)> = vec![(payload, options); batch];
    let mut sent = 0;
    while sent < count {
        window.wait(*total).await;
        let room = batch.min(count - sent);
        let now = socket.send_many(&messages[..room]).await.unwrap();
        sent += now;
        *total += now;
    }
}

/// Receive `count` messages on `socket`, `batch` a call.
async fn receive_on(socket: &ConnectedSocket, count: usize, batch: usize, window: &Window) {
    let mut received = 0;
    let mut out = Vec::with_capacity(batch);
    while received < count {
        let now = if batch == 1 {
            match socket.recv().await.unwrap() {
                Some(NotificationOrData::Data(_)) => 1,
                Some(_) => 0,
                None => panic!("the association ended"),
            }
        } else {
            out.clear();
            socket.recv_many(&mut out, batch).await.unwrap();
            out.iter()
                .filter(|item| matches!(item, NotificationOrData::Data(_)))
                .count()
        };
        received += now;
        window.received(now);
    }
}

fn pair(run: &Run) -> (f64, f64) {
    let runtime = new_runtime();
    let (count, octets) = (run.messages, run.octets);
    let (send_batch, receive_batch) = (run.send_batch, run.receive_batch);
    runtime.block_on(async move {
        let listener = listen(SocketToAssociation::OneToOne).listen(1).unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, accepted) = tokio::join!(connect(address), listener.accept());
        let receiver = accepted.unwrap().0;
        let window = Arc::new(Window::default());
        let (begun, cpu_begun) = (Instant::now(), cpu());
        let receiving = tokio::spawn({
            let window = window.clone();
            async move {
                receive_on(&receiver, count, receive_batch, &window).await;
                (begun.elapsed().as_secs_f64(), cpu() - cpu_begun)
            }
        });
        let payload = vec![0x5a; octets];
        send_on(&sender, &payload, count, send_batch, &window, &mut 0).await;
        receiving.await.unwrap()
    })
}

/// An endpoint with `associations` clients, and the association identifier of each client,
/// in the order of the clients.
fn endpoint_and_clients(
    runtime: &tokio::runtime::Runtime,
    associations: usize,
) -> (OneToManyEndpoint, Vec<ConnectedSocket>, Vec<i32>) {
    runtime.block_on(async {
        let socket = listen(SocketToAssociation::OneToMany);
        let endpoint = socket.into_endpoint(1024).unwrap();
        let address = endpoint.local_addr().unwrap();
        let mut clients = Vec::with_capacity(associations);
        let mut identities = Vec::with_capacity(associations);
        for _ in 0..associations {
            let client = connect(address).await;
            // The endpoint learns the identifier from the client's first message.
            client.send(b"hello", SendOptions::new()).await.unwrap();
            let identity = loop {
                if let NotificationOrData::Data(message) = endpoint.recv().await.unwrap() {
                    break message.assoc_id().unwrap();
                }
            };
            clients.push(client);
            identities.push(identity);
        }
        (endpoint, clients, identities)
    })
}

fn to_endpoint(run: &Run) -> (f64, f64) {
    let runtime = new_runtime();
    let (endpoint, clients, _) = endpoint_and_clients(&runtime, run.associations);
    let (count, octets) = (run.messages, run.octets);
    let (send_batch, batch) = (run.send_batch, run.receive_batch);
    runtime.block_on(async move {
        let window = Arc::new(Window::default());
        let (begun, cpu_begun) = (Instant::now(), cpu());
        let receiving = tokio::spawn({
            let window = window.clone();
            async move {
                let mut received = 0;
                let mut out = Vec::with_capacity(batch);
                while received < count {
                    let now = if batch == 1 {
                        match endpoint.recv().await.unwrap() {
                            NotificationOrData::Data(_) => 1,
                            _ => 0,
                        }
                    } else {
                        out.clear();
                        endpoint.recv_many(&mut out, batch).await.unwrap();
                        out.iter()
                            .filter(|item| matches!(item, NotificationOrData::Data(_)))
                            .count()
                    };
                    received += now;
                    window.received(now);
                }
                (begun.elapsed().as_secs_f64(), cpu() - cpu_begun)
            }
        });
        // Each client sends its share, a batch at a time, in turns, in one task.
        let payload = vec![0x5a; octets];
        let per_client = count / clients.len();
        let mut left: Vec<usize> = (0..clients.len())
            .map(|index| per_client + usize::from(index < count % clients.len()))
            .collect();
        let mut total = 0;
        while left.iter().any(|left| *left > 0) {
            for (client, left) in clients.iter().zip(&mut left) {
                if *left > 0 {
                    let share = send_batch.min(*left);
                    send_on(client, &payload, share, send_batch, &window, &mut total).await;
                    *left -= share;
                }
            }
        }
        let elapsed = receiving.await.unwrap();
        drop(clients);
        elapsed
    })
}

fn from_endpoint(run: &Run) -> (f64, f64) {
    let runtime = new_runtime();
    let (endpoint, clients, identities) = endpoint_and_clients(&runtime, run.associations);
    let (count, octets, associations) = (run.messages, run.octets, run.associations);
    let (batch, receive_batch) = (run.send_batch, run.receive_batch);
    runtime.block_on(async move {
        let window = Arc::new(Window::default());
        let (begun, cpu_begun) = (Instant::now(), cpu());
        // Each client receives its share in a task of its own.
        let per_client = count / associations;
        let receiving: Vec<_> = clients
            .into_iter()
            .enumerate()
            .map(|(index, client)| {
                let share = per_client + usize::from(index < count % associations);
                let window = window.clone();
                tokio::spawn(async move {
                    receive_on(&client, share, receive_batch, &window).await;
                    client
                })
            })
            .collect();
        let payload = vec![0x5a; octets];
        // Message `i` goes to association `i % associations`.
        let options: Vec<SendOptions> = identities
            .iter()
            .map(|identity| SendOptions::new().ppid(60).association(*identity))
            .collect();
        if batch == 1 {
            for index in 0..count {
                window.wait(index).await;
                endpoint
                    .send(&payload, options[index % associations])
                    .await
                    .unwrap();
            }
        } else {
            let mut sent = 0;
            let mut messages: Vec<(&[u8], SendOptions)> = Vec::with_capacity(batch);
            while sent < count {
                window.wait(sent).await;
                messages.clear();
                for index in sent..(sent + batch).min(count) {
                    messages.push((&payload, options[index % associations]));
                }
                sent += endpoint.send_many(&messages).await.unwrap();
            }
        }
        let mut clients = Vec::with_capacity(receiving.len());
        for task in receiving {
            clients.push(task.await.unwrap());
        }
        let measured = (begun.elapsed().as_secs_f64(), cpu() - cpu_begun);
        drop((endpoint, clients));
        measured
    })
}
