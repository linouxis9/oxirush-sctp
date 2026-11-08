//! The packed addresses of a `struct sctp_getaddrs`, of any octets and any count, are read
//! within the octets: an address of another family than IPv4 and IPv6, a count of more
//! addresses than the octets hold, and octets shorter than the header are refused. What
//! is read, packed again as `bindx` packs addresses, reads back as the same addresses.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|returned: &[u8]| {
    let (read, again) = oxirush_sctp::fuzzing::addresses(returned);
    let Ok(addrs) = read else {
        return;
    };
    let packed = again.expect("addresses that are read are packed");
    // The header of what the kernel returns: the association ID and the count
    let mut whole = returned[..4].to_vec();
    whole.extend_from_slice(&u32::try_from(addrs.len()).expect("a count").to_ne_bytes());
    whole.extend_from_slice(&packed);
    let (read_again, _) = oxirush_sctp::fuzzing::addresses(&whole);
    assert_eq!(read_again.ok().as_ref(), Some(&addrs), "{whole:02x?}");
    assert!(
        8 + packed.len() <= returned.len(),
        "more octets than were returned"
    );
});
