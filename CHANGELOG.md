# Changelog

## 0.1.0 (unreleased)

First release of oxirush-sctp, a fork of sctp-rs 0.3.1. The API of
sctp-rs 0.3 is kept under the new crate name; the changes are fixes and
additions.

### Fixed

- Received control messages are parsed with the real `msghdr` and an
  aligned buffer that holds `SCTP_RCVINFO` and `SCTP_NXTINFO` together.
  0.3.1 computed the buffer bounds from the wrong header, always lost
  `RCVINFO` when both were requested, and spun forever on a control
  message of another level (such as `SO_TIMESTAMP`).
- Sockets own an `OwnedFd`. A dropped `Socket`, a failed bind, a
  cancelled or timed-out connect, and errors after accept or peel-off no
  longer leak the descriptor, and `Listener` and `ConnectedSocket` close
  it after Tokio has deregistered it.
- `sctp_recv` returns whole messages, reassembling partial deliveries
  until `MSG_EOR`; a partly received message is kept in the socket, so
  the future is cancel-safe. A message over 4 MiB is an `InvalidData`
  error. 0.3.1 returned at most 4096 bytes and parsed the rest of a long
  notification as a new one.
- `sctp_send` waits for room in the send buffer instead of returning
  `EWOULDBLOCK` and leaving stale write readiness, and sends with
  `MSG_NOSIGNAL`, so a closed association no longer raises `SIGPIPE`.
- A failed connect reports the kernel's error from `SO_ERROR`
  (`ETIMEDOUT`, `EHOSTUNREACH`, …) instead of always `ECONNREFUSED`.
- The `connect` and `sctp_connectx` futures are `Send`.
- `sctp_getladdrs` and `sctp_getpaddrs` grow their buffer as needed
  (0.3.1 failed with `ENOMEM` beyond 15 IPv4 or 8 IPv6 addresses) and
  parse it with bounds checks and without misaligned reads.
- Short notifications no longer panic.
- `accept` and the `SCTP_STATUS` option pass a `socklen_t` length, which
  64-bit big-endian targets need.
- Created, accepted and peeled-off sockets are close-on-exec.
- The crate builds for musl targets.

### Added

- `set_nodelay` and `nodelay` (`SCTP_NODELAY`) on `Socket`, `Listener`
  and `ConnectedSocket`. Accepted and peeled-off sockets inherit the
  option from their listener.
- `sctp_set_rto_info` and `sctp_get_rto_info` (`SCTP_RTOINFO`) with the
  `RtoInfo` type, and `Socket::set_reuseaddr` and `reuseaddr`.
- `AsRawFd` and `AsFd` for `Socket`, `Listener` and `ConnectedSocket`.

### Changed

- The crate is `oxirush-sctp` (library `oxirush_sctp`) and is the
  repository's root package.
- `ConnectedSocket::from_rawfd` rejects a closed descriptor with `EBADF`
  and closes the descriptor if registering it with Tokio fails.
- Peel-off needs Linux 4.13 or later; event subscription already needed
  5.0.
