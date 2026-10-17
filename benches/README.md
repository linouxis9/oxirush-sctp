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

## Figures

On one core of an AMD Ryzen 7 5700G with Linux 7.0 and Rust 1.98.1, a
message sent on one end of an association and received on the other takes
8.9 µs for 64 octets, 9.0 µs for 1400, 14.6 µs for 16 KiB and 40 µs for
64 KiB. More than nine tenths of that time is in the system calls.

The sender has `set_nodelay(true)`. Without it a message shorter than a
packet waits for the acknowledgement of the one before, which the peer may
delay by 200 ms: the benchmark then takes 200 ms a message.
