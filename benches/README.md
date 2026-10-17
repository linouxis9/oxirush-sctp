# Benchmarks of oxirush-sctp

A package of its own, which the published crate does not have. It times a
message through an association over the loopback interface with
[criterion](https://crates.io/crates/criterion), and profiles it with
[pprof](https://crates.io/crates/pprof).

```bash
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

```bash
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
senders keep at most 4096 messages ahead of the receivers, and every socket
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

```bash
CPUS=2,3 ROUNDS=3 ./compare.sh     # perf stat through sudo -n, for the cycles
```

`compare/src/main.rs` says what each loop does and how each crate is
called. A counting allocator gives the allocations; the C program
allocates nothing in its loops.

## Figures

On an AMD Ryzen 7 5700G with Linux 7.0 and Rust 1.98.1, over the loopback
interface, a load of 7 to 11 from other work on the machine.

A message sent on one end of an association and received on the other, one
at a time on one thread, the fastest of five runs in turns: 8.9 µs for 200
octets, 8.8 µs with `recv_into`, 47 µs for 64 KiB and 45 µs with
`recv_into`. More than nine tenths of that time is in the system calls.
The sender has `set_nodelay(true)`. Without it a message shorter than a
packet waits for the acknowledgement of the one before, which the peer may
delay by 200 ms: the benchmark then takes 200 ms a message.

Messages of 200 octets a second, from one task to another on two threads,
the median of three rounds of `throughput.sh`, with the CPU time of the
process a message:

| | One message a call | 64 a call | The driver's writer |
|---|---|---|---|
| One association | 116 505, 14.3 µs | 343 488, 4.6 µs | 313 099, 5.2 µs |
| 1 client to an endpoint | 131 104, 13.7 µs | 334 789, 4.6 µs | |
| 100 clients to an endpoint | 162 918, 11.1 µs | 322 127, 4.9 µs | |
| 1000 clients to an endpoint | 92 544, 17.3 µs | 316 559, 4.9 µs | |
| An endpoint to 1 association | 130 573, 14.3 µs | 335 855, 5.0 µs | 227 344, 7.0 µs |
| An endpoint to 100 associations | 45 222, 40.2 µs | 47 320, 37.8 µs | |

The rates move by about a seventh between rounds at this load. An endpoint
that sends to many associations is held back by Linux, which wakes the
socket once for each of them when one has data acknowledged, unless
`net.sctp.sndbuf_policy` is 1. With `MSG_MORE` off, 64 messages a call
were no faster than one, which is why `send_many` always bundles.

All the rows of `compare.sh`, a load of 7 to 8:

| Loop | Implementation | ns a message | Cycles | Instructions | Allocations | Octets allocated | Peak memory | Time ÷ oxirush-sctp |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Ping-pong, 200 octets | C | 8 256 | 36 520 | 25 850 | 0.00 | 0 | 1.9 MiB | 0.92 |
|  | oxirush-sctp | 8 973 | 39 229 | 27 944 | 1.00 | 200 | 3.9 MiB |  |
|  | oxirush-sctp, `recv_into` | 8 818 | 38 779 | 27 569 | 0.00 | 0 | 3.7 MiB | 0.98 |
|  | tokio-sctp 0.2.0 | 8 927 | 39 249 | 28 041 | 0.00 | 0 | 3.8 MiB | 0.99 |
|  | async-sctp 1.0.0 | 9 331 | 41 349 | 31 501 | 1.00 | 65 536 | 3.9 MiB | 1.04 |
|  | lksctp 0.1.0 | 8 630 | 38 425 | 27 632 | 0.00 | 0 | 3.7 MiB | 0.96 |
|  | sctp-rs 0.3.1 | 8 794 | 38 517 | 28 726 | 5.00 | 4 648 | 3.7 MiB | 0.98 |
| Ping-pong, 16 KiB | C | 10 970 | 48 550 | 31 773 | 0.00 | 0 | 1.9 MiB | 0.90 |
|  | oxirush-sctp | 12 143 | 55 668 | 36 531 | 1.00 | 16 384 | 3.9 MiB |  |
|  | oxirush-sctp, `recv_into` | 11 813 | 55 347 | 34 324 | 0.00 | 0 | 3.8 MiB | 0.97 |
|  | tokio-sctp 0.2.0 | 11 798 | 55 279 | 34 318 | 0.00 | 0 | 3.8 MiB | 0.97 |
|  | async-sctp 1.0.0 | 12 594 | 58 850 | 38 244 | 1.00 | 65 536 | 3.8 MiB | 1.04 |
|  | lksctp 0.1.0 | 11 528 | 54 072 | 33 847 | 0.00 | 0 | 3.7 MiB | 0.95 |
|  | sctp-rs 0.3.1 | fails: EAGAIN | | | | | | |
| 200 octets, one association, two threads | C | 7 325 | 50 084 | 31 741 | 0.00 | 0 | 2.2 MiB | 1.06 |
|  | C, `sendmmsg` and `recvmmsg` | 2 423 | 15 364 | 11 714 | 0.00 | 0 | 2.4 MiB | 0.35 |
|  | oxirush-sctp | 6 898 | 51 905 | 32 381 | 1.00 | 200 | 4.2 MiB |  |
|  | oxirush-sctp, `send_many` and `recv_many` | 2 800 | 19 498 | 14 652 | 1.00 | 201 | 4.2 MiB | 0.41 |
|  | tokio-sctp 0.2.0 | 7 124 | 54 008 | 31 639 | 0.00 | 0 | 4.2 MiB | 1.03 |
|  | async-sctp 1.0.0 | 6 787 | 53 834 | 33 978 | 1.00 | 65 536 | 4.1 MiB | 0.98 |
|  | lksctp 0.1.0 | 7 153 | 51 771 | 33 958 | 0.00 | 0 | 4.3 MiB | 1.04 |
|  | sctp-rs 0.3.1 | 7 279 | 52 953 | 34 898 | 5.00 | 4 648 | 4.0 MiB | 1.06 |
| 200 octets, 100 associations, two threads | C | 8 389 | 57 915 | 33 027 | 0.00 | 0 | 2.2 MiB | 1.01 |
|  | C, `sendmmsg` and `recvmmsg` | 2 750 | 17 114 | 11 895 | 0.00 | 0 | 2.4 MiB | 0.33 |
|  | oxirush-sctp | 8 276 | 60 268 | 40 550 | 1.00 | 222 | 4.6 MiB |  |
|  | oxirush-sctp, `send_many` and `recv_many` | 3 076 | 22 026 | 14 652 | 1.00 | 202 | 5.1 MiB | 0.37 |
|  | tokio-sctp 0.2.0 | 8 335 | 60 852 | 41 026 | 0.00 | 22 | 4.4 MiB | 1.01 |
|  | async-sctp 1.0.0 | 7 858 | 68 371 | 46 187 | 1.00 | 65 558 | 16.8 MiB | 0.95 |
|  | lksctp 0.1.0 | 8 578 | 61 418 | 40 468 | 0.00 | 22 | 16.8 MiB | 1.04 |
|  | sctp-rs 0.3.1 | 8 910 | 64 783 | 41 348 | 5.00 | 4 670 | 5.3 MiB | 1.08 |
