# Changelog

## 0.1.1 - 2026-10-31

### Added

- `SocketOptions::association_stats` reads `SCTP_GET_ASSOC_STATS`: the
  per-association statistics the kernel keeps, as `AssociationStats`. It
  reports the chunks an association retransmitted, the gap-acknowledgement
  blocks and duplicate and out-of-sequence chunks it saw, the packets and
  SACKs it sent and received, and its highest measured retransmission
  timeout. A caller that watched only the socket-wide counters could not
  tell one association's loss from another's.

## 0.1.0 - 2026-10-24

First release, started as a fork of sctp-rs 0.3.1 whose API it does not keep.

- Async SCTP sockets for Tokio on the Linux kernel stack (RFC 6458): `Socket`,
  `Listener`, `ConnectedSocket` (one association) and `OneToManyEndpoint`
  (many), with `SocketOptions`, multihoming (`bindx`, `connectx`), typed
  notifications, the end of an association reported to every later call,
  `close(deadline)` that loses nothing that was sent, and batches of sends and
  receives (`send_many`, `recv_many`).
- The module `driver`: a writer and a reader of a socket, each a task that
  sends or receives in batches.
- Linux 5.0 or later, Rust 1.88 or later.
