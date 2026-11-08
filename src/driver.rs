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
//! let client = client.connect(listener.local_addr()?).await?;
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

use crate::{AssociationId, ConnectedSocket, NotificationOrData, OneToManyEndpoint, SendOptions};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, mpsc};

/// The sockets that a driver drives: [`ConnectedSocket`] and [`OneToManyEndpoint`].
pub trait Batched: sealed::Sealed + Send + Sync + 'static {}

impl Batched for ConnectedSocket {}
impl Batched for OneToManyEndpoint {}

mod sealed {
    // What a driver takes of its socket: its options, which reach the socket itself, and
    // whether it has one association.
    pub trait Sealed {
        const ONE_ASSOCIATION: bool;
        fn options(&self) -> crate::SocketOptions<'_>;
    }

    impl Sealed for crate::ConnectedSocket {
        const ONE_ASSOCIATION: bool = true;
        fn options(&self) -> crate::SocketOptions<'_> {
            crate::ConnectedSocket::options(self)
        }
    }

    impl Sealed for crate::OneToManyEndpoint {
        const ONE_ASSOCIATION: bool = false;
        fn options(&self) -> crate::SocketOptions<'_> {
            crate::OneToManyEndpoint::options(self)
        }
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

impl Shared {
    fn new(config: DriverConfig) -> Self {
        Self {
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
        }
    }

    // `sent` messages of `octets` sent and those `lost` have left the queue, and give their
    // room back.
    fn left(&self, sent: usize, octets: usize, lost: &[Queued]) {
        let octets = octets + lost.iter().map(|message| message.permits).sum::<usize>();
        self.sent.fetch_add(sent as u64, Ordering::Relaxed);
        self.failed.fetch_add(lost.len() as u64, Ordering::Relaxed);
        self.length.fetch_sub(sent + lost.len(), Ordering::AcqRel);
        self.octets.fetch_sub(octets, Ordering::Relaxed);
        self.room.add_permits(octets);
    }
}

struct Queued {
    /// 0 on a connected socket.
    assoc_id: AssociationId,
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

/// The queue of the messages of a socket of type `S`, which a task of its own writes in
/// batches. Clones share it; the task ends, after the queue is empty, when the last clone is
/// dropped. The writer of a [`OneToManyEndpoint`] names the association of each message, as
/// the endpoint's `send` does.
pub struct Writer<S> {
    shared: Arc<Shared>,
    socket: PhantomData<fn() -> S>,
}

impl<S> Clone for Writer<S> {
    fn clone(&self) -> Self {
        self.shared.writers.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
            socket: PhantomData,
        }
    }
}

impl<S> Drop for Writer<S> {
    fn drop(&mut self) {
        if self.shared.writers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.queued.notify_one();
        }
    }
}

impl<S> std::fmt::Debug for Writer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("overload", &self.overload())
            .finish()
    }
}

impl Writer<ConnectedSocket> {
    /// Queue a message: at once, unless the queue is in overload, where it fails with
    /// [`SendError::Overloaded`] or waits, as [`DriverConfig::on_overload`] says, or full,
    /// where it waits for room.
    ///
    /// ```
    /// # use oxirush_sctp::{Socket, SocketToAssociation};
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// # let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # socket.bind("127.0.0.1:0".parse().unwrap())?;
    /// # let listener = socket.listen(5)?;
    /// # let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// # let client = client.connect(listener.local_addr()?).await?;
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
        self.queue(0, payload, options).await
    }

    /// Queue a message without waiting: [`SendError::Full`] where [`send`][Self::send] would
    /// wait for room, and [`SendError::Overloaded`] in overload, whatever
    /// [`DriverConfig::on_overload`] says.
    pub fn try_send(&self, payload: Vec<u8>, options: SendOptions) -> Result<(), SendError> {
        self.try_queue(0, payload, options)
    }
}

impl Writer<OneToManyEndpoint> {
    /// Queue a message for association `assoc_id`, as the writer of a connected socket queues
    /// one.
    pub async fn send(
        &self,
        assoc_id: AssociationId,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> Result<(), SendError> {
        self.queue(assoc_id, payload, options).await
    }

    /// Queue a message for association `assoc_id` without waiting, as the writer of a
    /// connected socket queues one.
    pub fn try_send(
        &self,
        assoc_id: AssociationId,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> Result<(), SendError> {
        self.try_queue(assoc_id, payload, options)
    }
}

impl<S> Writer<S> {
    async fn queue(
        &self,
        assoc_id: AssociationId,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> Result<(), SendError> {
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
        self.enqueue(assoc_id, payload, options, permits);
        Ok(())
    }

    fn try_queue(
        &self,
        assoc_id: AssociationId,
        payload: Vec<u8>,
        options: SendOptions,
    ) -> Result<(), SendError> {
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
        self.enqueue(assoc_id, payload, options, permits);
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
    /// # let client = client.connect(listener.local_addr()?).await?;
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

    fn enqueue(
        &self,
        assoc_id: AssociationId,
        payload: Vec<u8>,
        options: SendOptions,
        permits: u32,
    ) {
        let shared = &self.shared;
        shared.octets.fetch_add(permits as usize, Ordering::Relaxed);
        shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(Queued {
                assoc_id,
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
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use oxirush_sctp::driver::{self, DriverConfig};
    /// use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
    ///
    /// let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// socket.bind("127.0.0.1:0".parse()?)?;
    /// let listener = socket.listen(5)?;
    /// let client = Socket::new_v4(SocketToAssociation::OneToOne)?;
    /// let client = client.connect(listener.local_addr()?).await?;
    /// let (server, _) = listener.accept().await?;
    /// let (_writer, mut reader) = driver::spawn(server, DriverConfig::default())?;
    ///
    /// // A message, then `None` once the peer has closed the association
    /// client.send(b"hello", SendOptions::new()).await?;
    /// drop(client);
    /// let Some(Ok(NotificationOrData::Data(message))) = reader.recv().await else {
    ///     unreachable!()
    /// };
    /// assert_eq!(message.payload, b"hello");
    /// assert!(reader.recv().await.is_none());
    /// # Ok(())
    /// # }
    /// ```
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
    /// # let client = client.connect(listener.local_addr()?).await?;
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
/// It turns Nagle's algorithm off on the socket
/// ([`SocketOptions::set_nodelay`][crate::SocketOptions::set_nodelay]): the
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
/// # let client = client.connect(listener.local_addr()?).await?;
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
/// assert_eq!(message.ppid, 60);
/// # Ok(())
/// # }
/// ```
pub fn spawn<S: Batched>(socket: S, config: DriverConfig) -> std::io::Result<(Writer<S>, Reader)> {
    assert!(config.batch > 0, "a batch of 0 messages");
    socket.options().set_nodelay(true)?;
    let socket = Arc::new(socket);
    let shared = Arc::new(Shared::new(config));
    let (tx, rx) = mpsc::channel(config.reader_capacity.max(1));
    tokio::spawn(read(socket.clone(), tx, config.batch));
    tokio::spawn(write(socket, shared.clone()));
    let writer = Writer {
        shared,
        socket: PhantomData,
    };
    Ok((writer, Reader { received: rx }))
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
                let core = socket.options().core;
                core.poll_recv_many(cx, &mut out, batch).map(Some)
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
type Key = AssociationId;

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
            pending.push_back(message.assoc_id, message);
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
            let messages: Vec<(AssociationId, &[u8], SendOptions)> = batch
                .iter()
                .map(|(_, message)| {
                    (
                        message.assoc_id,
                        message.payload.as_slice(),
                        message.options,
                    )
                })
                .collect();
            let core = socket.options().core;
            let send = std::future::poll_fn(|cx| core.poll_send_many(cx, &messages));
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
        let mut octets = 0;
        for (_, message) in batch.drain(..) {
            interval.record(now.saturating_duration_since(message.since));
            octets += message.permits;
        }
        let mut lost: Vec<Queued> = Vec::new();
        if let Some(error) = failure {
            let (key, _) = rest[0];
            if error.kind() == std::io::ErrorKind::InvalidInput {
                // This message alone: a stream that the association does not have, or a
                // message longer than the send buffer.
                lost.push(rest.remove(0).1);
            } else if S::ONE_ASSOCIATION {
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
        shared.left(count, octets, &lost);
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
            let mut late =
                std::mem::take(&mut *shared.queue.lock().unwrap_or_else(PoisonError::into_inner));
            shared.left(0, 0, late.make_contiguous());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn writer(config: DriverConfig) -> Writer<ConnectedSocket> {
        Writer {
            shared: Arc::new(Shared::new(config)),
            socket: PhantomData,
        }
    }

    fn queued(byte: u8) -> Queued {
        Queued {
            assoc_id: 0,
            payload: vec![byte],
            options: SendOptions::new(),
            since: Instant::now(),
            permits: 1,
        }
    }

    fn micros(sojourn: u64) -> Duration {
        Duration::from_micros(sojourn)
    }

    /// The percentiles of an interval are the upper ends of the powers of two of microseconds
    /// that hold them, from a sojourn under a microsecond to one past the last power.
    #[test]
    fn an_interval_has_the_percentiles_of_its_sojourns() {
        let mut interval = Interval::new(Instant::now());
        for _ in 0..98 {
            interval.record(micros(3));
        }
        for _ in 0..2 {
            interval.record(micros(1000));
        }
        assert_eq!((interval.total, interval.min), (100, Some(micros(3))));
        assert_eq!(interval.percentile(50), micros(4));
        assert_eq!(interval.percentile(99), micros(1024));

        let mut interval = Interval::new(Instant::now());
        interval.record(Duration::from_nanos(10));
        assert_eq!(interval.percentile(99), micros(2));
        interval.record(Duration::from_secs(1 << 40));
        assert_eq!(interval.percentile(100), micros(1 << 39));
        assert_eq!(interval.min, Some(Duration::from_nanos(10)));
    }

    /// The queue is in overload once the shortest sojourn of an interval is above the target,
    /// and not when it is at it or under it; an empty queue is in overload no more.
    #[test]
    fn the_shortest_sojourn_above_the_target_is_an_overload() {
        let shared = Shared::new(DriverConfig::default());
        for (min, empty, overloaded) in [
            (Duration::from_millis(1), false, false),
            (Duration::from_millis(5), false, false),
            (Duration::from_millis(6), false, true),
            (Duration::from_millis(6), true, false),
        ] {
            let mut interval = Interval::new(Instant::now());
            interval.record(min);
            interval.record(min * 2);
            close_interval(&shared, &mut interval, Instant::now(), empty);
            let state = shared.overloaded.load(Ordering::Acquire);
            assert_eq!(state, overloaded, "shortest {min:?}, empty {empty}");
            assert_eq!(shared.window.lock().unwrap().min, min);
            assert_eq!(interval.total, 0, "the next interval begins");
        }
    }

    /// The slower trigger: the 99th percentile above the ceiling, and not at it, for
    /// `ceiling_intervals` intervals in a row.
    #[test]
    fn the_99th_percentile_above_the_ceiling_for_intervals_in_a_row_is_an_overload() {
        let shared = Shared::new(DriverConfig {
            target: Duration::from_secs(1),
            ceiling: micros(131_072),
            ceiling_intervals: 3,
            ..Default::default()
        });
        let mut interval = Interval::new(Instant::now());
        let mut close = |sojourn| {
            interval.record(micros(sojourn));
            close_interval(&shared, &mut interval, Instant::now(), false);
            shared.overloaded.load(Ordering::Acquire)
        };
        // 100 000 µs is in the power of two of 131 072 µs, at the ceiling
        for _ in 0..5 {
            assert!(!close(100_000));
        }
        // 200 000 µs is in that of 262 144 µs, above it
        assert_eq!(
            [
                close(200_000),
                close(200_000),
                close(200_000),
                close(200_000)
            ],
            [false, false, true, true]
        );
        // An interval under it counts again from none
        assert_eq!([close(10), close(200_000)], [false, false]);
    }

    /// An interval without a sojourn changes neither the window nor the state of a queue that
    /// stands; it ends the overload of one that is empty.
    #[test]
    fn an_interval_without_sojourns_leaves_the_window() {
        let shared = Shared::new(DriverConfig::default());
        shared.overloaded.store(true, Ordering::Release);
        let mut interval = Interval::new(Instant::now());
        close_interval(&shared, &mut interval, Instant::now(), false);
        assert!(shared.overloaded.load(Ordering::Acquire));
        let window = *shared.window.lock().unwrap();
        assert_eq!(
            (window.min, window.p50, window.p99),
            (Duration::ZERO, Duration::ZERO, Duration::ZERO)
        );
        let start = Instant::now();
        close_interval(&shared, &mut interval, start, true);
        assert!(!shared.overloaded.load(Ordering::Acquire));
        assert_eq!(interval.start, start);
    }

    /// The writers that wait are woken when the overload ends, and only then.
    #[test]
    fn the_end_of_the_overload_alone_wakes_the_waiters() {
        let shared = Shared::new(DriverConfig::default());
        let woken = |overloaded| {
            let notified = std::pin::pin!(shared.relieved.notified());
            set_overload(&shared, overloaded);
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            notified.poll(&mut cx).is_ready()
        };
        assert!(!woken(false), "no overload ends");
        assert!(!woken(true), "an overload begins");
        assert!(!woken(true), "it goes on");
        assert!(woken(false), "it ends");
    }

    /// A batch takes the associations in turns, a few messages of each a turn, and no more
    /// than its size: one that went on past it would never end.
    #[test]
    fn a_batch_takes_the_associations_in_turns_up_to_its_size() {
        crate::bounded(|| async {
            let mut config = DriverConfig::default();
            (config.batch, config.per_association) = (5, 4);
            let mut pending = Pending::default();
            for byte in 0..4 {
                pending.push_back(1, queued(byte));
                pending.push_back(2, queued(10 + byte));
            }
            let mut batch = Vec::new();
            let mut taken = |pending: &mut Pending| {
                batch.clear();
                pending.batch(&config, &mut batch);
                batch
                    .iter()
                    .map(|(key, message)| (*key, message.payload[0]))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                taken(&mut pending),
                [(1, 0), (1, 1), (1, 2), (1, 3), (2, 10)]
            );
            assert_eq!(taken(&mut pending), [(2, 11), (2, 12), (2, 13)]);
            assert!(pending.turns.is_empty() && pending.queues.is_empty());
        });
    }

    /// A message put back is the next of its association, whose turn comes first when it had
    /// no other.
    #[test]
    fn a_message_put_back_is_the_next() {
        let config = DriverConfig {
            batch: 8,
            per_association: 8,
            ..Default::default()
        };
        let mut pending = Pending::default();
        pending.push_back(1, queued(1));
        pending.push_back(2, queued(10));
        pending.push_front(1, queued(0));
        pending.push_front(3, queued(20));
        let mut batch = Vec::new();
        pending.batch(&config, &mut batch);
        let taken: Vec<(Key, u8)> = batch
            .iter()
            .map(|(key, message)| (*key, message.payload[0]))
            .collect();
        assert_eq!(taken, [(3, 20), (1, 0), (1, 1), (2, 10)]);
    }

    /// What leaves the queue, sent or lost, gives back its place and its octets of room.
    #[test]
    fn what_leaves_the_queue_gives_its_room_back() {
        let config = DriverConfig {
            max_queued: 100,
            ..Default::default()
        };
        let writer = writer(config);
        for _ in 0..3 {
            writer.try_send(vec![0; 30], SendOptions::new()).unwrap();
        }
        let full = writer.try_send(vec![0; 30], SendOptions::new());
        assert!(matches!(full, Err(SendError::Full)), "{full:?}");
        let lost = Queued {
            permits: 30,
            ..queued(0)
        };
        writer.shared.left(1, 30, &[lost]);
        let overload = writer.overload();
        let counts = (overload.queued, overload.queued_octets);
        assert_eq!((counts, overload.sent, overload.failed), ((1, 30), 1, 1));
        writer.try_send(vec![0; 70], SendOptions::new()).unwrap();
    }

    /// The writer's task sends what is queued, and waits for more when it has sent all: one
    /// that looped instead would hold the thread of its runtime, which the test has of its
    /// own.
    #[test]
    fn the_writer_waits_for_more_after_it_has_sent_all() {
        crate::bounded(|| async {
            let socket = crate::Socket::new_v4(crate::SocketToAssociation::OneToOne).unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = socket.listen(1).unwrap();
            let client = crate::Socket::new_v4(crate::SocketToAssociation::OneToOne).unwrap();
            let client = client.connect(listener.local_addr().unwrap()).await;
            let (server, _) = listener.accept().await.unwrap();
            let (writer, _reader) = spawn(client.unwrap(), DriverConfig::default()).unwrap();
            for payload in [&b"first"[..], b"second"] {
                writer
                    .send(payload.to_vec(), SendOptions::new())
                    .await
                    .unwrap();
                match server.recv().await.unwrap() {
                    Some(NotificationOrData::Data(message)) => assert_eq!(message.payload, payload),
                    other => panic!("{other:?}"),
                }
                // The task finds the queue empty, and waits
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }

    /// The messages of an association that is dropped leave its turns and the others stay;
    /// draining takes every message.
    #[test]
    fn the_messages_of_an_association_leave_the_pending_ones() {
        let mut pending = Pending::default();
        for byte in 0..3 {
            pending.push_back(1, queued(byte));
            pending.push_back(2, queued(10 + byte));
        }
        let removed: Vec<u8> = pending.remove(1).iter().map(|m| m.payload[0]).collect();
        assert_eq!(removed, [0, 1, 2]);
        assert_eq!(pending.turns, [2]);
        assert!(pending.remove(1).is_empty());
        let mut drained: Vec<u8> = pending.drain().map(|m| m.payload[0]).collect();
        drained.sort();
        assert_eq!(drained, [10, 11, 12]);
        assert!(pending.turns.is_empty() && pending.queues.is_empty());
    }

    /// A queue in overload that has emptied is in overload for no writer: the next message is
    /// queued, and the one after it refused.
    #[test]
    fn an_empty_queue_is_in_overload_for_no_writer() {
        let writer = writer(DriverConfig::default());
        writer.shared.overloaded.store(true, Ordering::Release);
        assert!(!writer.overload().overloaded);
        writer.try_send(vec![1], SendOptions::new()).unwrap();
        assert!(writer.overload().overloaded);
        let refused = writer.try_send(vec![2, 3], SendOptions::new());
        assert!(matches!(refused, Err(SendError::Overloaded)), "{refused:?}");
        let overload = writer.overload();
        assert_eq!((overload.refused, overload.refused_octets), (1, 2));
    }

    /// A message takes its octets of the room of the queue: at most all of it, and one at
    /// least.
    #[test]
    fn a_message_takes_its_octets_of_the_room() {
        let config = DriverConfig {
            max_queued: 100,
            ..Default::default()
        };
        let writer = writer(config);
        writer.try_send(vec![0; 80], SendOptions::new()).unwrap();
        assert_eq!(writer.overload().queued_octets, 80);
        let full = writer.try_send(vec![0; 80], SendOptions::new());
        assert!(matches!(full, Err(SendError::Full)), "{full:?}");
        writer.try_send(vec![0; 20], SendOptions::new()).unwrap();
        assert_eq!(writer.overload().queued_octets, 100);

        let writer = self::writer(config);
        writer.try_send(vec![0; 500], SendOptions::new()).unwrap();
        assert_eq!(writer.overload().queued_octets, 100);
    }

    /// A writer that has stopped says so before anything else, also of an empty message.
    #[test]
    fn a_stopped_writer_says_so_first() {
        let writer = writer(DriverConfig::default());
        *writer.shared.closed.lock().unwrap() =
            Some((std::io::ErrorKind::ConnectionReset, "reset".into()));
        match writer.try_send(Vec::new(), SendOptions::new()) {
            Err(SendError::Closed(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                assert_eq!(error.to_string(), "reset");
            }
            other => panic!("{other:?}"),
        }
    }

    /// A writer prints the state of its queue.
    #[test]
    fn a_writer_prints_its_overload() {
        let printed = format!("{:?}", writer(DriverConfig::default()));
        assert!(
            printed.starts_with("Writer { overload: Overload { overloaded: false"),
            "{printed}"
        );
    }

    /// A send error says why, and one of a writer that stopped has the error of the socket as
    /// its source.
    #[test]
    fn a_send_error_says_why() {
        use std::error::Error;
        assert_eq!(
            SendError::Overloaded.to_string(),
            "the queue of the writer is in overload"
        );
        assert_eq!(
            SendError::Full.to_string(),
            "the queue of the writer is full"
        );
        assert_eq!(
            SendError::Empty.to_string(),
            "a message has one octet at least"
        );
        let closed = SendError::Closed(std::io::Error::from_raw_os_error(libc::ECONNRESET));
        let reset = std::io::Error::from_raw_os_error(libc::ECONNRESET);
        assert_eq!(
            closed.to_string(),
            format!("the writer has stopped: {reset}")
        );
        let source = closed.source().expect("the error of the socket");
        let source = source.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(source.raw_os_error(), Some(libc::ECONNRESET));
        assert!(SendError::Full.source().is_none());
    }
}
