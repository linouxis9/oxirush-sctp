//! An association between an AMF and a gNB as NGAP has it, with a writer and
//! a reader for each side, each a task, and the overload of a writer's queue.
//!
//! ```text
//! cargo run --example driver
//! ```

use oxirush_sctp::driver::{self, DriverConfig};
use oxirush_sctp::{InitParams, NotificationOrData, SendOptions, Socket, SocketToAssociation};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The AMF listens with four streams each way: NGAP has stream 0 for the
    // messages of no UE, and the others for those of the UEs
    let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
    socket.bind("127.0.0.1:0".parse()?)?;
    socket.options().set_nodelay(true)?;
    socket
        .options()
        .set_init_params(InitParams::streams(4, 4))?;
    let listener = socket.listen(5)?;

    // The gNB connects
    let gnb = Socket::new_v4(SocketToAssociation::OneToOne)?;
    gnb.options().set_nodelay(true)?;
    gnb.options().set_init_params(InitParams::streams(4, 4))?;
    let (gnb, _) = gnb.connect(listener.local_addr()?).await?;
    let (amf, _) = listener.accept().await?;
    let status = amf.status()?;
    println!("{:?}, {} streams each way", status.state, status.outstreams);

    // A writer, which every task that sends can clone, and a reader, for
    // each side. A writer sends what was queued in batches of 64 at most
    let (gnb_writer, mut gnb_reader) = driver::spawn(gnb, DriverConfig::default())?;
    let (amf_writer, mut amf_reader) = driver::spawn(amf, DriverConfig::default())?;

    // The gNB: a message of no UE on stream 0, then a thousand messages of
    // UEs on streams 1 to 3, each with the payload protocol identifier of NGAP
    let setup = SendOptions::new().stream(0).ppid(60);
    gnb_writer.send(b"NG SETUP REQUEST".to_vec(), setup).await?;
    for ue in 0..1000_u16 {
        let options = SendOptions::new().stream(1 + ue % 3).ppid(60);
        gnb_writer.send(ue.to_be_bytes().to_vec(), options).await?;
    }

    // The AMF: each message with its stream, then an answer on stream 0
    let mut streams = [0; 4];
    for _ in 0..1001 {
        if let Some(Ok(NotificationOrData::Data(message))) = amf_reader.recv().await {
            streams[usize::from(message.stream_id().ok_or("no stream")?)] += 1;
        }
    }
    println!("AMF: {streams:?} messages on streams 0 to 3");
    amf_writer
        .send(b"NG SETUP RESPONSE".to_vec(), setup)
        .await?;
    if let Some(Ok(NotificationOrData::Data(message))) = gnb_reader.recv().await {
        println!("gNB: {:?}", String::from_utf8_lossy(&message.payload));
    }

    // What the writer sent and refused, and whether its queue is in overload
    let overload = gnb_writer.overload();
    println!(
        "gNB writer: {} sent, {} refused, in overload {}",
        overload.sent, overload.refused, overload.overloaded
    );
    Ok(())
}
