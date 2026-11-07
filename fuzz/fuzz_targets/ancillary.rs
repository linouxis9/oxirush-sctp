//! The control messages of a `recvmsg` are read within their octets, whatever lengths their
//! headers claim.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|control: &[u8]| {
    // The parser reads the headers unaligned: an odd offset must give the same result.
    let mut shifted = vec![0];
    shifted.extend_from_slice(control);
    assert_eq!(
        oxirush_sctp::fuzzing::ancillary(control),
        oxirush_sctp::fuzzing::ancillary(&shifted[1..])
    );
});
