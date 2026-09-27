//! A simple example demonstrating use of the APIs
//! Sends a Ping to the server
//!

use clap::{Arg, Command};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    eprintln!("ping");

    let app = Command::new("oxirush-sctp ping example")
        .author("Abhijit Gadgil <gabhijit@iitbombay.org>")
        .arg(Arg::new("server").num_args(1).required(true).long("server"));

    let matches = app.get_matches();

    eprintln!(
        "matches.server: {}",
        matches.get_one::<String>("server").unwrap()
    );
    let server_address = matches.get_one::<String>("server").unwrap();
    let server_address: std::net::SocketAddr = server_address.parse().unwrap();

    let client_socket = oxirush_sctp::Socket::new_v4(oxirush_sctp::SocketToAssociation::OneToOne)?;

    let (connected, assoc_id) = client_socket.sctp_connectx(&[server_address]).await?;
    eprintln!("conected: {:#?}, assoc_id: {}", connected, assoc_id);

    for i in 0..10 {
        let message = format!("oxirush-sctp ping : {}", i);
        let send_data = oxirush_sctp::SendData {
            payload: message.as_bytes().to_vec(),
            snd_info: None,
        };
        connected.sctp_send(send_data).await?;
        let received = connected.sctp_recv().await?;
        eprintln!("received: {:#?}", received);
    }

    Ok(())
}
