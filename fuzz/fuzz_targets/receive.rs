//! Messages are assembled from any sequence of parts, errors and ends without a panic, and
//! none is longer than the limit.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|script: &[u8]| {
    oxirush_sctp::fuzzing::receive(script);
});
