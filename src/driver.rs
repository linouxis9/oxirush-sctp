//! One task that writes the messages of a socket and one that reads them, each in batches, and
//! the overload of the queue of the writer, which its latency tells.
//!
//! [`spawn`] takes a [`ConnectedSocket`] or a [`OneToManyEndpoint`] and returns a [`Writer`],
//! which queues messages, and a [`Reader`], which receives whole ones. The writer's task
//! sends what is queued as soon as it is: a batch is what the queue has at that moment, up to
//! [`DriverConfig::batch`] messages, and nothing waits for a batch to fill. It takes the
//! associations of an endpoint in turns, a few messages of each, and keeps the order of the
//! messages of each association. The reader's task receives up to a batch a call, and passes
//! on the messages of every association as they come, whole, with their association.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> std::io::Result<()> {
//! use oxirush_sctp::driver::{self, DriverConfig};
//! use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
//!
//! let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
//! socket.bind("127.0.0.1:0".parse().unwrap())?;
//! let listener = socket.listen(1)?;
//! let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
//! let (client, _) = client.connect(listener.local_addr()?).await?;
//! let (server, _) = listener.accept().await?;
//!
//! let (writer, _) = driver::spawn(client, DriverConfig::default())?;
//! let (_, mut reader) = driver::spawn(server, DriverConfig::default())?;
//! for message in [b"one", b"two"] {
//!     writer.send(message.to_vec(), SendOptions::new().ppid(60)).await?;
//! }
//! for expected in [b"one", b"two"] {
//!     match reader.recv().await {
//!         Some(Ok(NotificationOrData::Data(message))) => assert_eq!(message.payload, expected),
//!         other => panic!("{other:?}"),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Overload
//!
//! The writer measures how long each message waits in its queue, its sojourn. A queue that a
//! burst fills and the socket then drains is no overload; one that stands is, as CoDel
//! (RFC 8289) tells them apart: the queue is in overload once the shortest sojourn of an
//! interval ([`DriverConfig::interval`], 100 ms) is above [`DriverConfig::target`] (5 ms),
//! and leaves it once the shortest of an interval is under it again. A slower trigger has it
//! in overload too while the 99th percentile of the sojourns is above
//! [`DriverConfig::ceiling`] (100 ms) for [`DriverConfig::ceiling_intervals`] intervals in a
//! row (5). In overload, [`Writer::send`] fails at once with [`SendError::Overloaded`], or
//! waits for the overload to end ([`OnOverload::Wait`]), and the writer counts what it
//! refused. Nothing is tuned to a rate: by Little's law the length of a queue is the rate of
//! its arrivals times their sojourn (L = λW), so a sojourn that stands high is a rate of
//! arrivals above what the socket carries, whatever that rate is.
//!
//! The queue also holds [`DriverConfig::max_queued`] octets at most (64 MiB), a limit against
//! memory alone: past it, `send` waits for room, and [`Writer::try_send`] fails with
//! [`SendError::Full`].
//!
//! [`Writer::overload`] has the state, the shortest sojourn and the 50th and 99th percentiles
//! of the last interval, from a histogram of powers of two, and the counters.

use crate::{ConnectedSocket, NotificationOrData, OneToManyEndpoint, SendOptions, SocketOptions};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, mpsc};

/// The sockets that a driver drives: [`ConnectedSocket`] and [`OneToManyEndpoint`].
pub trait Batched: Send + Sync + 'static + sealed::Sealed {
    #[doc(hidden)]
    fn poll_send_batch(
        &self,
        cx: &mut Context<'_>,
        messages: &[(&[u8], SendOptions)],
    ) -> Poll<std::io::Result<usize>>;

    #[doc(hidden)]
    fn poll_recv_batch(
        &self,
        cx: &mut Context<'_>,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> Poll<std::io::Result<usize>>;

    #[doc(hidden)]
    fn one_association(&self) -> bool;

    #[doc(hidden)]
    fn options(&self) -> SocketOptions<'_>;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::ConnectedSocket {}
    impl Sealed for crate::OneToManyEndpoint {}
}

impl Batched for ConnectedSocket {
    fn poll_send_batch(
        &self,
        cx: &mut Context<'_>,
        messages: &[(&[u8], SendOptions)],
    ) -> Poll<std::io::Result<usize>> {
        self.poll_send_many(cx, messages)
    }

    fn poll_recv_batch(
        &self,
        cx: &mut Context<'_>,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> Poll<std::io::Result<usize>> {
        self.poll_recv_many(cx, out, limit)
    }

    fn one_association(&self) -> bool {
        true
    }

    fn options(&self) -> SocketOptions<'_> {
        ConnectedSocket::options(self)
    }
}

impl Batched for OneToManyEndpoint {
    fn poll_send_batch(
        &self,
        cx: &mut Context<'_>,
        messages: &[(&[u8], SendOptions)],
    ) -> Poll<std::io::Result<usize>> {
        self.poll_send_many(cx, messages)
    }

    fn poll_recv_batch(
        &self,
        cx: &mut Context<'_>,
        out: &mut Vec<NotificationOrData>,
        limit: usize,
    ) -> Poll<std::io::Result<usize>> {
        self.poll_recv_many(cx, out, limit)
    }

    fn one_association(&self) -> bool {
        false
    }

    fn options(&self) -> SocketOptions<'_> {
        OneToManyEndpoint::options(self)
    }
}

/// How a driver batches and when its writer's queue is in overload.
///
/// ```
/// use oxirush_sctp::driver::{DriverConfig, OnOverload};
/// use std::time::Duration;
///
/// // A writer that waits in overload, with a queue that stands from 10 ms
/// let mut config = DriverConfig::default();
/// config.on_overload = OnOverload::Wait;
/// config.target = Duration::from_millis(10);
/// assert_eq!(config.batch, 64);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DriverConfig {
    /// The most messages of a batch, sent or received in one call: 64.
    pub batch: usize,
    /// The most messages of one association that a batch takes before the next
    /// association's: 16.
    pub per_association: usize,
    /// The sojourn above which a queue stands: 5 ms, CoDel's, a few round trips of the
    /// loopback interface and far below the timers of the protocols that SCTP carries.
    pub target: Duration,
    /// The interval over which the shortest sojourn is taken: 100 ms, CoDel's, longer than a
    /// burst that the socket drains.
    pub interval: Duration,
    /// The 99th percentile of the sojourns above which the slower trigger counts an
    /// interval: 100 ms.
    pub ceiling: Duration,
    /// The intervals in a row that the 99th percentile must be above the ceiling: 5.
    pub ceiling_intervals: u32,
    /// The most octets that the queue holds: 64 MiB, against memory alone.
    pub max_queued: usize,
    /// What [`Writer::send`] does in overload: [`OnOverload::Refuse`].
    pub on_overload: OnOverload,
    /// The messages that the reader passes on and the caller has not taken yet, past which
    /// the reader stops reading the socket: 1024.
    pub reader_capacity: usize,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            batch: 64,
            per_association: 16,
            target: Duration::from_millis(5),
            interval: Duration::from_millis(100),
            ceiling: Duration::from_millis(100),
            ceiling_intervals: 5,
            max_queued: 64 << 20,
            on_overload: OnOverload::Refuse,
            reader_capacity: 1024,
        }
    }
}

/// What [`Writer::send`] does while the queue is in overload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OnOverload {
    /// Fail at once with [`SendError::Overloaded`].
    Refuse,
    /// Wait until the overload ends.
    Wait,
}

/// Why a message was not queued.
#[derive(Debug)]
#[non_exhaustive]
pub enum SendError {
    /// The queue is in overload: its sojourns stand above the target.
    Overloaded,
    /// The queue holds [`DriverConfig::max_queued`] octets ([`Writer::try_send`]).
    Full,
    /// The writer has stopped: the socket failed, with this error, or the association ended.
    Closed(std::io::Error),
    /// An empty message, which SCTP does not have.
    Empty,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Overloaded => f.write_str("the queue of the writer is in overload"),
            SendError::Full => f.write_str("the queue of the writer is full"),
            SendError::Closed(error) => write!(f, "the writer has stopped: {error}"),
            SendError::Empty => f.write_str("a message has one octet at least"),
        }
    }
}

impl std::error::Error for SendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SendError::Closed(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SendError> for std::io::Error {
    fn from(error: SendError) -> Self {
        match error {
            SendError::Closed(error) => error,
            SendError::Empty => std::io::Error::new(std::io::ErrorKind::InvalidInput, error),
            other => std::io::Error::new(std::io::ErrorKind::WouldBlock, other),
        }
    }
}

/// The state of the queue of a writer, as [`Writer::overload`] reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Overload {
    /// Whether the queue is in overload, by either trigger.
    pub overloaded: bool,
    /// The shortest sojourn of the last interval that had messages.
    pub min: Duration,
    /// The 50th percentile of its sojourns, as the upper end of a power of two of
    /// microseconds.
    pub p50: Duration,
    /// Its 99th percentile, likewise.
    pub p99: Duration,
    /// The messages in the queue.
    pub queued: usize,
    /// Its octets.
    pub queued_octets: usize,
    /// The messages sent.
    pub sent: u64,
    /// The messages refused in overload.
    pub refused: u64,
    /// Their octets.
    pub refused_octets: u64,
    /// The messages dropped because their association ended or a send failed.
    pub failed: u64,
}

/// What the writer's task and the writers share.
struct Shared {
    config: DriverConfig,
    queue: Mutex<VecDeque<Queued>>,
    /// Woken when a message is queued, or the last writer goes.
    queued: Notify,
    /// Octets of room in the queue.
    room: Semaphore,
    /// Woken when the overload ends.
    relieved: Notify,
    overloaded: AtomicBool,
    length: AtomicUsize,
    octets: AtomicUsize,
    sent: AtomicU64,
    refused: AtomicU64,
    refused_octets: AtomicU64,
    failed: AtomicU64,
    /// What the last complete interval had.
    window: Mutex<Window>,
    /// Why the writer stopped.
    closed: Mutex<Option<(std::io::ErrorKind, String)>>,
    writers: AtomicUsize,
}

struct Queued {
    payload: Vec<u8>,
    options: SendOptions,
    since: Instant,
    /// The octets of room that it takes in the queue.
    permits: usize,
}

#[derive(Default, Clone, Copy)]
struct Window {
    min: Duration,
    p50: Duration,
    p99: Duration,
}

/// The queue of the messages of a socket, which a task of its own writes in batches. Clones
/// share it; the task ends, after the queue is empty, when the last clone is dropped.
pub struct Writer {
    shared: Arc<Shared>,
}

impl Clone for Writer {
    fn clone(&self) -> Self {
        self.shared.writers.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.shared.writers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.queued.notify_one();
        }
    }
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("overload", &self.overload())
            .finish()
    }
}

impl Writer {
    /// Queue a message: at once, unless the queue is in overload, where it fails with
    /// [`SendError::Overloaded`] or waits, as [`DriverConfig::on_overload`] says, or full,
    /// where it waits for room. On an endpoint, `options` names the association.
    ///
    /// ```
    /// # use oxirush_sctp::{Socket, SocketToAssociation};
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # socket.bind("127.0.0.1:0".parse().unwrap())?;
    /// # let listener = socket.listen(5)?;
    /// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # let (client, _) = client.connect(listener.local_addr()?).await?;
    /// # let (server, _) = listener.accept().await?;
    /// use oxirush_sctp::driver::{self, DriverConfig, SendError};
    /// use oxirush_sctp::SendOptions;
    ///
    /// // Every task that sends has a clone of the writer
    /// let (writer, _) = driver::spawn(client, DriverConfig::default())?;
    /// let (_, _reader) = driver::spawn(server, DriverConfig::default())?;
    /// let other = writer.clone();
    /// tokio::spawn(async move { other.send(vec![1], SendOptions::new()).await });
    /// writer.send(vec![2], SendOptions::new().stream(1)).await?;
    ///
    /// // SCTP has no empty message
    /// let empty = writer.send(Vec::new(), SendOptions::new()).await;
    /// assert!(matches!(empty, Err(SendError::Empty)));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&self, payload: Vec<u8>, options: SendOptions) -> Result<(), SendError> {
        if payload.is_empty() {
            return Err(SendError::Empty);
        }
        loop {
            self.check_closed()?;
            if !self.in_overload() {
                break;
            }
            if self.shared.config.on_overload == OnOverload::Refuse {
                return Err(self.refuse(payload.len()));
            }
            let relieved = self.shared.relieved.notified();
            if !self.in_overload() {
                break;
            }
            // The state is looked at again each interval, for an overload that ended while
            // nothing was queued.
            let _ = tokio::time::timeout(self.shared.config.interval, relieved).await;
        }
        let permits = self.permits(payload.len());
        let permit = self
            .shared
            .room
            .acquire_many(permits)
            .await
            .map_err(|_| self.closed_error())?;
        permit.forget();
        self.enqueue(payload, options, permits);
        Ok(())
    }

    /// Queue a message without waiting: [`SendError::Full`] where [`send`][Self::send] would
    /// wait for room, and [`SendError::Overloaded`] in overload, whatever
    /// [`DriverConfig::on_overload`] says.
    pub fn try_send(&self, payload: Vec<u8>, options: SendOptions) -> Result<(), SendError> {
        self.check_closed()?;
        if payload.is_empty() {
            return Err(SendError::Empty);
        }
        if self.in_overload() {
            return Err(self.refuse(payload.len()));
        }
        let permits = self.permits(payload.len());
        match self.shared.room.try_acquire_many(permits) {
            Ok(permit) => permit.forget(),
            Err(tokio::sync::TryAcquireError::NoPermits) => return Err(SendError::Full),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(self.closed_error()),
        }
        self.enqueue(payload, options, permits);
        Ok(())
    }

    /// The state of the queue and its counters.
    ///
    /// ```
    /// # use oxirush_sctp::{Socket, SocketToAssociation};
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # socket.bind("127.0.0.1:0".parse().unwrap())?;
    /// # let listener = socket.listen(5)?;
    /// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # let (client, _) = client.connect(listener.local_addr()?).await?;
    /// # let (server, _) = listener.accept().await?;
    /// use oxirush_sctp::driver::{self, DriverConfig};
    /// use oxirush_sctp::SendOptions;
    ///
    /// let (writer, _) = driver::spawn(client, DriverConfig::default())?;
    /// let (_, _reader) = driver::spawn(server, DriverConfig::default())?;
    /// writer.send(vec![1, 2, 3], SendOptions::new()).await?;
    /// let overload = writer.overload();
    /// assert!(!overload.overloaded);
    /// assert_eq!(overload.refused, 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn overload(&self) -> Overload {
        let shared = &self.shared;
        let window = *shared.window.lock().unwrap_or_else(PoisonError::into_inner);
        Overload {
            overloaded: self.in_overload(),
            min: window.min,
            p50: window.p50,
            p99: window.p99,
            queued: shared.length.load(Ordering::Relaxed),
            queued_octets: shared.octets.load(Ordering::Relaxed),
            sent: shared.sent.load(Ordering::Relaxed),
            refused: shared.refused.load(Ordering::Relaxed),
            refused_octets: shared.refused_octets.load(Ordering::Relaxed),
            failed: shared.failed.load(Ordering::Relaxed),
        }
    }

    // In overload, unless the queue is empty: an empty queue does not stand, and the task
    // that would end the overload has nothing to send meanwhile.
    fn in_overload(&self) -> bool {
        self.shared.overloaded.load(Ordering::Acquire)
            && self.shared.length.load(Ordering::Acquire) > 0
    }

    fn refuse(&self, octets: usize) -> SendError {
        self.shared.refused.fetch_add(1, Ordering::Relaxed);
        self.shared
            .refused_octets
            .fetch_add(octets as u64, Ordering::Relaxed);
        SendError::Overloaded
    }

    // The permits of a message: its octets, at least one, at most the room of the queue.
    fn permits(&self, octets: usize) -> u32 {
        let most = self
            .shared
            .config
            .max_queued
            .clamp(1, Semaphore::MAX_PERMITS);
        u32::try_from(octets.clamp(1, most)).unwrap_or(u32::MAX)
    }

    fn enqueue(&self, payload: Vec<u8>, options: SendOptions, permits: u32) {
        let shared = &self.shared;
        shared.octets.fetch_add(permits as usize, Ordering::Relaxed);
        shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(Queued {
                payload,
                options,
                since: Instant::now(),
                permits: permits as usize,
            });
        shared.length.fetch_add(1, Ordering::Release);
        shared.queued.notify_one();
    }

    fn check_closed(&self) -> Result<(), SendError> {
        if self
            .shared
            .closed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            return Err(self.closed_error());
        }
        Ok(())
    }

    fn closed_error(&self) -> SendError {
        let closed = self
            .shared
            .closed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (kind, text) = closed.clone().unwrap_or((
            std::io::ErrorKind::BrokenPipe,
            "the writer has stopped".into(),
        ));
        SendError::Closed(std::io::Error::new(kind, text))
    }
}

/// The whole messages and notifications of a socket, as its reader's task receives them, and
/// its errors; `None` once the association of a connected socket ended, or the reader stopped
/// on an error, which came before.
#[derive(Debug)]
pub struct Reader {
    received: mpsc::Receiver<std::io::Result<NotificationOrData>>,
}

impl Reader {
    /// The next message, notification or error.
    pub async fn recv(&mut self) -> Option<std::io::Result<NotificationOrData>> {
        self.received.recv().await
    }

    /// Up to `limit` of them, appended to `out`, waiting for the first: how many.
    ///
    /// ```
    /// # use oxirush_sctp::{Socket, SocketToAssociation};
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # socket.bind("127.0.0.1:0".parse().unwrap())?;
    /// # let listener = socket.listen(5)?;
    /// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # let (client, _) = client.connect(listener.local_addr()?).await?;
    /// # let (server, _) = listener.accept().await?;
    /// use oxirush_sctp::driver::{self, DriverConfig};
    /// use oxirush_sctp::SendOptions;
    ///
    /// let (writer, _) = driver::spawn(client, DriverConfig::default())?;
    /// let (_, mut reader) = driver::spawn(server, DriverConfig::default())?;
    /// for index in 0..10_u8 {
    ///     writer.send(vec![index], SendOptions::new()).await?;
    /// }
    /// // What has come, 64 messages a call at most
    /// let mut received = Vec::new();
    /// while received.len() < 10 {
    ///     reader.recv_many(&mut received, 64).await;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv_many(
        &mut self,
        out: &mut Vec<std::io::Result<NotificationOrData>>,
        limit: usize,
    ) -> usize {
        self.received.recv_many(out, limit).await
    }

    /// The receiver itself, for `tokio_stream` and the like.
    pub fn into_inner(self) -> mpsc::Receiver<std::io::Result<NotificationOrData>> {
        self.received
    }
}

/// Start the writer's task and the reader's task of `socket`, on the current Tokio runtime,
/// which needs its time driver for the overload.
///
/// It turns Nagle's algorithm off on the socket ([`SocketOptions::set_nodelay`]): the
/// writer bundles what is queued itself, and Nagle's algorithm would hold a batch back for
/// the acknowledgement of the last one, which a peer may delay by 200 ms.
///
/// # Errors
///
/// When the socket refuses the option.
///
/// # Panics
///
/// Outside a Tokio runtime, and for a [`DriverConfig::batch`] of 0.
///
/// ```
/// # use oxirush_sctp::{Socket, SocketToAssociation};
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// # socket.bind("127.0.0.1:0".parse().unwrap())?;
/// # let listener = socket.listen(5)?;
/// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
/// # let (client, _) = client.connect(listener.local_addr()?).await?;
/// # let (server, _) = listener.accept().await?;
/// use oxirush_sctp::driver::{self, DriverConfig};
/// use oxirush_sctp::{NotificationOrData, SendOptions};
///
/// // A writer for the client, a reader for the server
/// let (writer, _) = driver::spawn(client, DriverConfig::default())?;
/// let (_, mut reader) = driver::spawn(server, DriverConfig::default())?;
/// writer.send(b"NG SETUP REQUEST".to_vec(), SendOptions::new().ppid(60)).await?;
/// let Some(Ok(NotificationOrData::Data(message))) = reader.recv().await else {
///     unreachable!()
/// };
/// assert_eq!(message.ppid(), Some(60));
/// # Ok(())
/// # }
/// ```
pub fn spawn<S: Batched>(socket: S, config: DriverConfig) -> std::io::Result<(Writer, Reader)> {
    assert!(config.batch > 0, "a batch of 0 messages");
    socket.options().set_nodelay(true)?;
    let socket = Arc::new(socket);
    let shared = Arc::new(Shared {
        config,
        queue: Mutex::new(VecDeque::new()),
        queued: Notify::new(),
        room: Semaphore::new(config.max_queued.clamp(1, Semaphore::MAX_PERMITS)),
        relieved: Notify::new(),
        overloaded: AtomicBool::new(false),
        length: AtomicUsize::new(0),
        octets: AtomicUsize::new(0),
        sent: AtomicU64::new(0),
        refused: AtomicU64::new(0),
        refused_octets: AtomicU64::new(0),
        failed: AtomicU64::new(0),
        window: Mutex::new(Window::default()),
        closed: Mutex::new(None),
        writers: AtomicUsize::new(1),
    });
    let (tx, rx) = mpsc::channel(config.reader_capacity.max(1));
    tokio::spawn(read(socket.clone(), tx, config.batch));
    tokio::spawn(write(socket, shared.clone()));
    Ok((Writer { shared }, Reader { received: rx }))
}

async fn read<S: Batched>(
    socket: Arc<S>,
    tx: mpsc::Sender<std::io::Result<NotificationOrData>>,
    batch: usize,
) {
    let mut out = Vec::with_capacity(batch);
    loop {
        out.clear();
        // Until the reader is dropped, which ends the task and lets the socket go.
        let received = {
            let mut closed = std::pin::pin!(tx.closed());
            std::future::poll_fn(|cx| {
                if closed.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                socket.poll_recv_batch(cx, &mut out, batch).map(Some)
            })
            .await
        };
        match received {
            None => return,
            // The end of the association of a connected socket
            Some(Ok(0)) => return,
            Some(Ok(_)) => {
                for item in out.drain(..) {
                    if tx.send(Ok(item)).await.is_err() {
                        return;
                    }
                }
            }
            Some(Err(error)) => {
                // A message that was discarded leaves the socket readable: the others go on.
                let lasting = error.kind() != std::io::ErrorKind::InvalidData;
                if tx.send(Err(error)).await.is_err() || lasting {
                    return;
                }
            }
        }
    }
}

/// The key of the association of a message: all one on a connected socket.
type Key = i32;

/// Sojourns in powers of two of microseconds, from under 1 µs to over 2^39 µs.
const BUCKETS: usize = 40;

/// The sojourns of the current interval.
struct Interval {
    start: Instant,
    min: Option<Duration>,
    counts: [u64; BUCKETS],
    total: u64,
    /// Intervals in a row whose 99th percentile was above the ceiling.
    above_ceiling: u32,
}

impl Interval {
    fn new(start: Instant) -> Self {
        Self {
            start,
            min: None,
            counts: [0; BUCKETS],
            total: 0,
            above_ceiling: 0,
        }
    }

    fn record(&mut self, sojourn: Duration) {
        self.min = Some(self.min.map_or(sojourn, |min| min.min(sojourn)));
        let micros = sojourn.as_micros().max(1);
        let bucket = (u128::BITS - micros.leading_zeros()) as usize;
        self.counts[bucket.min(BUCKETS - 1)] += 1;
        self.total += 1;
    }

    // The upper end of the bucket of percentile `percent`.
    fn percentile(&self, percent: u64) -> Duration {
        let rank = (self.total * percent).div_ceil(100).max(1);
        let mut seen = 0;
        for (bucket, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Duration::from_micros(1 << bucket);
            }
        }
        Duration::MAX
    }
}

/// The messages of the writer's task that are not sent yet, each association's in its order,
/// and the associations in their turns.
#[derive(Default)]
struct Pending {
    queues: HashMap<Key, VecDeque<Queued>>,
    turns: VecDeque<Key>,
}

impl Pending {
    fn push_back(&mut self, key: Key, message: Queued) {
        let queue = self.queues.entry(key).or_default();
        if queue.is_empty() {
            self.turns.push_back(key);
        }
        queue.push_back(message);
    }

    // Back at the front of its association's queue, as the next of that association.
    fn push_front(&mut self, key: Key, message: Queued) {
        let queue = self.queues.entry(key).or_default();
        if queue.is_empty() {
            self.turns.push_front(key);
        }
        queue.push_front(message);
    }

    // A batch: the associations in turns, a few messages of each a turn.
    fn batch(&mut self, config: &DriverConfig, batch: &mut Vec<(Key, Queued)>) {
        while batch.len() < config.batch {
            let Some(key) = self.turns.pop_front() else {
                break;
            };
            let queue = self.queues.get_mut(&key).expect("a turn has its queue");
            let take = config
                .per_association
                .max(1)
                .min(config.batch - batch.len());
            for _ in 0..take {
                match queue.pop_front() {
                    Some(message) => batch.push((key, message)),
                    None => break,
                }
            }
            if queue.is_empty() {
                self.queues.remove(&key);
            } else {
                self.turns.push_back(key);
            }
        }
    }

    // Every message of association `key`, which is dropped.
    fn remove(&mut self, key: Key) -> VecDeque<Queued> {
        self.turns.retain(|turn| *turn != key);
        self.queues.remove(&key).unwrap_or_default()
    }

    fn drain(&mut self) -> impl Iterator<Item = Queued> + '_ {
        self.turns.clear();
        self.queues.drain().flat_map(|(_, queue)| queue)
    }
}

async fn write<S: Batched>(socket: Arc<S>, shared: Arc<Shared>) {
    let config = shared.config;
    let mut pending = Pending::default();
    let mut interval = Interval::new(Instant::now());
    let mut batch: Vec<(Key, Queued)> = Vec::with_capacity(config.batch);
    loop {
        // What is queued now: a batch never waits to fill.
        let taken =
            std::mem::take(&mut *shared.queue.lock().unwrap_or_else(PoisonError::into_inner));
        for message in taken {
            let key = if socket.one_association() {
                0
            } else {
                message.options.assoc_id()
            };
            pending.push_back(key, message);
        }
        if pending.turns.is_empty() {
            close_interval(&shared, &mut interval, Instant::now(), true);
            let queued = shared.queued.notified();
            if shared.length.load(Ordering::Acquire) == 0 {
                if shared.writers.load(Ordering::Acquire) == 0 {
                    return;
                }
                queued.await;
            }
            continue;
        }
        batch.clear();
        pending.batch(&config, &mut batch);
        let sent = loop {
            let messages: Vec<(&[u8], SendOptions)> = batch
                .iter()
                .map(|(_, message)| (message.payload.as_slice(), message.options))
                .collect();
            let send = std::future::poll_fn(|cx| socket.poll_send_batch(cx, &messages));
            let left = config
                .interval
                .saturating_sub(interval.start.elapsed())
                .max(Duration::from_millis(1));
            match tokio::time::timeout(left, send).await {
                Ok(sent) => break sent,
                // The socket takes nothing for an interval: the queue stands as long as its
                // first message waits, which the interval counts.
                Err(_) => {
                    let now = Instant::now();
                    if let Some(oldest) = batch.iter().map(|(_, message)| message.since).min() {
                        interval.record(now.saturating_duration_since(oldest));
                    }
                    close_interval(&shared, &mut interval, now, false);
                }
            }
        };
        let now = Instant::now();
        let (count, failure) = match sent {
            Ok(count) => (count, None),
            Err(error) => (0, Some(error)),
        };
        let mut rest = batch.split_off(count);
        let mut done = 0;
        for (_, message) in batch.drain(..) {
            interval.record(now.saturating_duration_since(message.since));
            done += message.permits;
        }
        shared.sent.fetch_add(count as u64, Ordering::Relaxed);
        let mut lost: Vec<Queued> = Vec::new();
        if let Some(error) = failure {
            let (key, _) = rest[0];
            if error.kind() == std::io::ErrorKind::InvalidInput {
                // This message alone: a stream that the association does not have, or a
                // message longer than the send buffer.
                lost.push(rest.remove(0).1);
            } else if socket.one_association() {
                // The association is done: what is left fails, and so do later sends.
                lost.extend(rest.drain(..).map(|(_, message)| message));
                lost.extend(pending.drain());
                *shared.closed.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((error.kind(), error.to_string()));
            } else {
                // The association of an endpoint that ended, or that it has not.
                let (gone, kept): (Vec<_>, Vec<_>) = rest.drain(..).partition(|(of, _)| *of == key);
                lost.extend(gone.into_iter().map(|(_, message)| message));
                lost.extend(pending.remove(key));
                rest = kept;
            }
        }
        for (key, message) in rest.into_iter().rev() {
            pending.push_front(key, message);
        }
        shared
            .failed
            .fetch_add(lost.len() as u64, Ordering::Relaxed);
        done += lost.iter().map(|message| message.permits).sum::<usize>();
        let finished = count + lost.len();
        let octets_done: usize = done;
        shared.length.fetch_sub(finished, Ordering::AcqRel);
        shared.octets.fetch_sub(octets_done, Ordering::Relaxed);
        shared.room.add_permits(octets_done);
        if interval.start.elapsed() >= config.interval {
            close_interval(&shared, &mut interval, Instant::now(), false);
        }
        // The other tasks of the thread, such as a reader of the same host, run between two
        // batches: one that falls behind by more than its receive buffer holds makes Linux
        // drop packets, which wait a retransmission timeout, a second or more.
        tokio::task::yield_now().await;
        if shared
            .closed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            // What was queued meanwhile fails too.
            let late =
                std::mem::take(&mut *shared.queue.lock().unwrap_or_else(PoisonError::into_inner));
            shared
                .failed
                .fetch_add(late.len() as u64, Ordering::Relaxed);
            shared.length.fetch_sub(late.len(), Ordering::AcqRel);
            let octets: usize = late.iter().map(|message| message.permits).sum();
            shared.octets.fetch_sub(octets, Ordering::Relaxed);
            shared.room.close();
            set_overload(&shared, false);
            return;
        }
    }
}

// The end of an interval: its shortest sojourn says whether the queue stands, and its 99th
// percentile counts toward the slower trigger. An empty queue stands no more.
fn close_interval(shared: &Shared, interval: &mut Interval, now: Instant, empty: bool) {
    let config = &shared.config;
    if interval.total > 0 {
        let window = Window {
            min: interval.min.unwrap_or_default(),
            p50: interval.percentile(50),
            p99: interval.percentile(99),
        };
        *shared.window.lock().unwrap_or_else(PoisonError::into_inner) = window;
        let above_ceiling = if window.p99 > config.ceiling {
            interval.above_ceiling + 1
        } else {
            0
        };
        let overloaded = !empty
            && (window.min > config.target || above_ceiling >= config.ceiling_intervals.max(1));
        set_overload(shared, overloaded);
        let carried = above_ceiling;
        *interval = Interval::new(now);
        interval.above_ceiling = carried;
    } else if empty {
        set_overload(shared, false);
        interval.start = now;
    }
}

fn set_overload(shared: &Shared, overloaded: bool) {
    let was = shared.overloaded.swap(overloaded, Ordering::AcqRel);
    if was && !overloaded {
        shared.relieved.notify_waiters();
    }
}
