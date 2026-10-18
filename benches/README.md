# Benchmarks of oxirush-sctp

A package of its own, which the published crate does not have. It times a
message through an association over the loopback interface with
[criterion](https://crates.io/crates/criterion), and profiles it with
[pprof](https://crates.io/crates/pprof).

```sh
cargo bench --bench messages                # every benchmark
cargo bench --bench messages -- 1400        # those whose name has it

# Ten seconds of one benchmark under the profiler, then where the time went
cargo bench --bench messages -- --profile-time 10 send-recv/65536
go tool pprof -top target/criterion/send-recv/65536/profile/profile.pb
```

The profile is one that Go's `pprof` reads. With `PROFILE=flamegraph` in
the environment it is `flamegraph.svg`, in the same directory.

## Messages a second

`throughput` sends many small messages, as NGAP has, from one task and
receives them in another, on two threads, and prints the messages a second
and the CPU time of the process a message, which counts the kernel's work
for the loopback interface:

```sh
cargo run --release --bin throughput -- pair 200 300000 1 64       # send_many and recv_many
cargo run --release --bin throughput -- to-endpoint 200 300000 100 1   # send and recv, 100 associations
CPUS=2,3 ROUNDS=3 OCTETS="100 200 400" ./throughput.sh             # every case, the median of three
```

`pair` is one association between two connected sockets, `to-endpoint`
one connected socket for each association that sends to one endpoint, and
`from-endpoint` an endpoint that sends to its associations in turns. A
batch of 1 is `send` and `recv`, a larger one `send_many` and `recv_many`,
`64:1` a batch of 64 for the sender and of 1 for the receiver, and
`driver:64` the writer of a driver, one message a `Writer::send`. The
senders keep at most 4,096 messages ahead of the receivers, and every socket
has a receive buffer of 4 MiB: past what it holds, Linux drops packets, and
a run would time retransmissions instead of calls.

## Against other implementations

`compare.sh` builds `compare/`, a package of its own that takes
[tokio-sctp](https://crates.io/crates/tokio-sctp) 0.2.0,
[async-sctp](https://crates.io/crates/async-sctp) 1.0.0,
[lksctp](https://crates.io/crates/lksctp) 0.1.0 and
[sctp-rs](https://crates.io/crates/sctp-rs) 0.3.1 at those versions, and
`compare/sctp.c`, the same loops in C on the kernel's sockets, and prints
the table of the README with the rows that it leaves out:

```sh
CPUS=2,3 ROUNDS=3 ./compare.sh     # perf stat through sudo -n, for the cycles
```

`compare/src/main.rs` says what each loop does and how each crate is
called. A counting allocator gives the allocations; the C program
allocates nothing in its loops.

## Figures

On an AMD Ryzen 7 5700G (16 threads) with Linux 7.0 and Rust 1.98.1, over the
loopback interface, on cores 2 and 3, at ea7c778, measured on a quiet
machine on 2026-10-18.

A message sent on one end of an association and received on the other, one
at a time on one thread: 8.0 µs for 200 octets and 7.8 µs with `recv_into`,
the ping-pong of `compare.sh` below, and 35 µs for 64 KiB and 33 µs with
`recv_into`, the lowest of five runs of `messages`. More than nine tenths of
that time is in the system calls.
The sender has `set_nodelay(true)`. Without it a message shorter than a
packet waits for the acknowledgement of the one before, which the peer may
delay by 200 ms: the benchmark then takes 200 ms a message.

Messages of 200 octets a second, from one task to another on two threads,
the median of three runs of `throughput.sh`, each the median of three
rounds, with the CPU time of the process a message:

| | One message a call | 64 a call | The driver's writer |
|---|---|---|---|
| One association | 162,996, 11.0 µs | 377,303, 4.2 µs | 359,377, 4.6 µs |
| 1 client to an endpoint | 159,198, 11.2 µs | 376,426, 4.2 µs | |
| 100 clients to an endpoint | 148,290, 12.0 µs | 362,827, 4.3 µs | |
| 1,000 clients to an endpoint | 184,566, 10.4 µs | 348,603, 4.5 µs | |
| An endpoint to 1 association | 156,750, 11.3 µs | 368,662, 4.2 µs | 350,239, 4.7 µs |
| An endpoint to 100 associations | 54,184, 33.7 µs | 55,967, 32.8 µs | |

The three runs were within 7 % of each other. An endpoint that sends to
many associations is held back by Linux, which wakes the socket once for
each of them when one has data acknowledged, unless `net.sctp.sndbuf_policy`
is 1. With `MSG_MORE` off, 64 messages a call were no faster than one,
which is why `send_many` always bundles.

All the rows of `compare.sh`, the median of three runs. The last column is
the time of each implementation over this crate's `send` and `recv`, "about
the same" within 10 %, which two builds of the same code can differ by. The
three runs of a loop differed by 11 % at most, for C with 100 associations.

| Loop | Implementation | ns a message | Cycles | Instructions | Allocations | Octets allocated | Peak memory | Other / this crate |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Ping-pong, 200 octets | C | 7,694 | 35,694 | 25,729 | 0 | 0 | 1.9 MiB | about the same |
| | oxirush-sctp | 7,999 | 37,624 | 27,981 | 1 | 200 | 3.8 MiB | |
| | oxirush-sctp, `recv_into` | 7,846 | 36,770 | 27,505 | 0 | 0 | 3.8 MiB | about the same |
| | tokio-sctp 0.2.0 | 8,283 | 38,903 | 27,877 | 0 | 0 | 3.7 MiB | about the same |
| | async-sctp 1.0.0 | 8,698 | 40,881 | 31,430 | 1 | 65,536 | 3.7 MiB | about the same |
| | lksctp 0.1.0 | 8,137 | 38,262 | 27,373 | 0 | 0 | 3.7 MiB | about the same |
| | sctp-rs 0.3.1 | 7,998 | 37,587 | 28,428 | 5 | 4,648 | 3.7 MiB | about the same |
| Ping-pong, 16 KiB | C | 10,094 | 47,833 | 31,380 | 0 | 0 | 1.9 MiB | about the same |
| | oxirush-sctp | 11,170 | 54,904 | 36,240 | 1 | 16,384 | 3.8 MiB | |
| | oxirush-sctp, `recv_into` | 10,882 | 53,625 | 34,110 | 0 | 0 | 3.7 MiB | about the same |
| | tokio-sctp 0.2.0 | 10,809 | 53,220 | 34,064 | 0 | 0 | 3.8 MiB | about the same |
| | async-sctp 1.0.0 | 11,724 | 57,761 | 37,994 | 1 | 65,536 | 3.8 MiB | about the same |
| | lksctp 0.1.0 | 10,814 | 53,420 | 33,617 | 0 | 0 | 3.8 MiB | about the same |
| | sctp-rs 0.3.1 | fails: EAGAIN | | | | | | |
| 200 octets, one association, two threads | C | 7,045 | 49,551 | 32,047 | 0 | 0 | 2.2 MiB | about the same |
| | C, `sendmmsg` and `recvmmsg` | 2,278 | 15,210 | 11,684 | 0 | 0 | 2.4 MiB | 0.32x |
| | oxirush-sctp | 7,118 | 51,647 | 36,053 | 1 | 200 | 4.3 MiB | |
| | oxirush-sctp, `send_many` and `recv_many` | 2,713 | 18,869 | 14,927 | 1 | 201 | 4.3 MiB | 0.38x |
| | tokio-sctp 0.2.0 | 7,306 | 52,740 | 36,026 | 0 | 0 | 4.3 MiB | about the same |
| | async-sctp 1.0.0 | 7,301 | 53,070 | 39,312 | 1 | 65,536 | 4.2 MiB | about the same |
| | lksctp 0.1.0 | 7,232 | 51,586 | 35,473 | 0 | 0 | 4.3 MiB | about the same |
| | sctp-rs 0.3.1 | 7,283 | 53,084 | 36,727 | 5 | 4,648 | 4.1 MiB | about the same |
| 200 octets, 100 associations, two threads | C | 7,808 | 53,464 | 34,948 | 0 | 0 | 2.2 MiB | about the same |
| | C, `sendmmsg` and `recvmmsg` | 2,388 | 15,699 | 11,889 | 0 | 0 | 2.4 MiB | 0.31x |
| | oxirush-sctp | 7,756 | 59,391 | 40,665 | 1 | 222 | 4.5 MiB | |
| | oxirush-sctp, `send_many` and `recv_many` | 2,805 | 19,489 | 15,067 | 1 | 202 | 5.3 MiB | 0.36x |
| | tokio-sctp 0.2.0 | 7,988 | 60,665 | 40,906 | 0 | 22 | 4.5 MiB | about the same |
| | async-sctp 1.0.0 | 7,313 | 65,934 | 48,033 | 1 | 65,558 | 16.9 MiB | about the same |
| | lksctp 0.1.0 | 7,530 | 57,662 | 39,983 | 0 | 22 | 16.8 MiB | about the same |
| | sctp-rs 0.3.1 | 7,814 | 59,630 | 41,317 | 5 | 4,670 | 5.4 MiB | about the same |
