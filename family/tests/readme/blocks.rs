// The blocks of the README that use other crates of the family, each as a
// function. `tests/readme.rs` writes this file and says how: do not edit it.
#![allow(unused)]

use std::cell::RefCell;

thread_local!(static PRINTED: RefCell<String> = const { RefCell::new(String::new()) });

// What a block prints is kept, to be compared with what the README shows.
macro_rules! println {
    () => {
        PRINTED.with(|printed| printed.borrow_mut().push('\n'))
    };
    // The line is made first, where the block is: its arguments may have a
    // `?` or an `await`.
    ($($argument:tt)*) => {{
        let line = format!($($argument)*);
        PRINTED.with(|printed| {
            let mut printed = printed.borrow_mut();
            printed.push_str(&line);
            printed.push('\n');
        })
    }};
}

/// What `block` prints.
pub fn printed(block: fn()) -> String {
    PRINTED.with(|printed| printed.borrow_mut().clear());
    block();
    PRINTED.with(|printed| printed.take())
}

pub const BLOCKS: &[fn()] = &[
    block_1,
];

/// The block 1 of the README that uses other crates.
fn block_1() {
    use oxirush_ngap::helpers::{global_gnb_id, guami, plmn, s_nssai};
    use oxirush_ngap::{build_ngap, ngap::*};
    use oxirush_sctp::{NotificationOrData, SendOptions, Socket, SocketToAssociation};

    #[tokio::main(flavor = "current_thread")]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let ngap = SendOptions::new().stream(0).ppid(60);
        let socket = Socket::new_v4(SocketToAssociation::OneToOne)?;
        socket.bind("127.0.0.1:0".parse()?)?;
        let listener = socket.listen(5)?;
        let gnb = Socket::new_v4(SocketToAssociation::OneToOne)?;
        let (gnb, _) = gnb.connect(listener.local_addr()?).await?;
        let (amf, _) = listener.accept().await?;

        // The gNB: NG SETUP REQUEST, its identity and the slices of its tracking area
        let network = plmn("208", "93")?;
        let slices = || SliceSupportList(vec![SliceSupportItem::new(s_nssai(1, None), None)]);
        let plmns = BroadcastPLMNList(vec![BroadcastPLMNItem::new(network, slices(), None)]);
        let area = SupportedTAItem::new(TAC([0, 0, 1]), plmns, None);
        let request = build_ngap!(InitiatingMessage, NGSetup, NGSetupRequest,
            GlobalRANNodeID(GlobalRANNodeID::globalGNB_ID(global_gnb_id(network, 0x123456, 24)?)),
            SupportedTAList(SupportedTAList(vec![area])),
            DefaultPagingDRX(PagingDRX::v128),
        )?;
        gnb.send(&request.encode()?, ngap).await?;

        // The AMF: the PDU of each message, and an answer on the same stream
        if let Some(NotificationOrData::Data(message)) = amf.recv().await? {
            let pdu = NGAP_PDU::decode(&message.payload)?;
            let (stream, ppid) = (message.stream_id(), message.ppid());
            println!("AMF: {:?} on stream {stream:?}, PPID {ppid:?}", pdu.kind());
            let served = ServedGUAMIItem::new(guami(network, 1, 1, 0)?, None, None);
            let response = build_ngap!(SuccessfulOutcome, NGSetup, NGSetupResponse,
                AMFName("amf-1"),
                ServedGUAMIList(ServedGUAMIList(vec![served])),
                RelativeAMFCapacity(RelativeAMFCapacity(255)),
                PLMNSupportList(PLMNSupportList(vec![PLMNSupportItem::new(network, slices(), None)])),
            )?;
            amf.send(&response.encode()?, ngap).await?;
        }
        if let Some(NotificationOrData::Data(message)) = gnb.recv().await? {
            println!("gNB: {:?}", NGAP_PDU::decode(&message.payload)?.kind());
        }
        Ok(())
    }
    main().unwrap();
}
