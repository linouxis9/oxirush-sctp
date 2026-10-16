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

## Figures

On one core of an AMD Ryzen 7 5700G with Linux 7.0 and Rust 1.98.1, a
message sent on one end of an association and received on the other takes
8.9 µs for 64 octets, 9.0 µs for 1400, 14.6 µs for 16 KiB and 40 µs for
64 KiB. More than nine tenths of that time is in the system calls.

The sender has `set_nodelay(true)`. Without it a message shorter than a
packet waits for the acknowledgement of the one before, which the peer may
delay by 200 ms: the benchmark then takes 200 ms a message.
