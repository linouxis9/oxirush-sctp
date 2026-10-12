//! The control messages of a `recvmsg` are read within their octets, whatever lengths their
//! headers claim.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    // The first octet says whether the kernel truncated the control messages.
    let Some((truncated, control)) = input.split_first() else {
        return;
    };
    let _ = oxirush_sctp::fuzzing::ancillary(control, truncated & 1 == 1);
    // The parser reads the headers unaligned: an odd offset must give the same result.
    let mut shifted = vec![0];
    shifted.extend_from_slice(control);
    assert_eq!(
        oxirush_sctp::fuzzing::ancillary(control, false),
        oxirush_sctp::fuzzing::ancillary(&shifted[1..], false)
    );
});
