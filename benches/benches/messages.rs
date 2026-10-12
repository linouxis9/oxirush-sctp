//! The time of a message through an association over the loopback
//! interface, and its CPU profile. It needs the `sctp` module of the
//! kernel, as the tests do.
//!
//! ```bash
//! cargo bench --bench messages                   # every case
//! cargo bench --bench messages -- 1400           # those whose name has it
//! cargo bench --bench messages -- --profile-time 10 send-recv/65536
//! go tool pprof -top target/criterion/send-recv/65536/profile/profile.pb
//! ```
//!
//! A benchmark is `send-recv/octets`: one message of that many octets sent
//! on one end of an association and received on the other. A message of 64
//! or 1400 octets comes in one read; one of 16 KiB or 64 KiB takes several,
//! which `recv` assembles. With `--profile-time`, criterion runs the
//! benchmark for that many seconds under pprof, which samples the program a
//! thousand times a second and writes `profile.pb`, a profile that
//! `go tool pprof` reads. With `PROFILE=flamegraph` in the environment it
//! writes `flamegraph.svg` instead.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};
use pprof::criterion::{Output, PProfProfiler};
use std::hint::black_box;
use std::time::Duration;

const SIZES: [usize; 4] = [64, 1400, 16384, 65536];

fn send_recv(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .expect("a runtime");
    let (sender, receiver) = runtime.block_on(async {
        let socket = Socket::new_v4(SocketToAssociation::OneToOne).expect("an SCTP socket");
        socket.bind("127.0.0.1:0".parse().unwrap()).expect("bound");
        let listener = socket.listen(1).expect("listening");
        let client = Socket::new_v4(SocketToAssociation::OneToOne).expect("an SCTP socket");
        // Without it a message smaller than a packet waits for the
        // acknowledgement of the one before, which the peer delays by up to
        // 200 ms: the benchmark would time that delay
        client.options().set_nodelay(true).expect("no delay");
        let (sender, _) = client
            .connect(listener.local_addr().unwrap())
            .await
            .expect("connected");
        let (receiver, _) = listener.accept().await.expect("accepted");
        (sender, receiver)
    });

    let mut group = criterion.benchmark_group("send-recv");
    let options = SendOptions {
        stream_id: 1,
        ppid: 60,
        ..Default::default()
    };
    for octets in SIZES {
        let message = vec![0x5a; octets];
        group.throughput(Throughput::Bytes(octets as u64));
        group.bench_function(BenchmarkId::from_parameter(octets), |bencher| {
            bencher.iter(|| {
                runtime.block_on(async {
                    sender
                        .send(black_box(&message), options)
                        .await
                        .expect("sent");
                    match receiver.recv().await.expect("received") {
                        NotificationOrData::Data(data) => data.payload.len(),
                        NotificationOrData::Notification(_) => 0,
                    }
                })
            })
        });
    }
    group.finish();
}

fn configuration() -> Criterion {
    let output = match std::env::var("PROFILE").as_deref() {
        Ok("flamegraph") => Output::Flamegraph(None),
        _ => Output::Protobuf,
    };
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .with_profiler(PProfProfiler::new(1000, output))
}

criterion_group! {
    name = benches;
    config = configuration();
    targets = send_recv
}
criterion_main!(benches);
