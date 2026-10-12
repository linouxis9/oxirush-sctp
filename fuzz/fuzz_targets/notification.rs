//! A notification of any octets is read without a panic, and one that the crate does not
//! parse keeps its octets.
#![no_main]

use libfuzzer_sys::fuzz_target;
use oxirush_sctp::Notification;

fuzz_target!(|octets: &[u8]| {
    let notification = oxirush_sctp::fuzzing::notification(octets);
    if let Notification::Unsupported { data, .. } = &notification {
        assert_eq!(data, octets);
    }
    let _ = format!("{notification:?}");
});
