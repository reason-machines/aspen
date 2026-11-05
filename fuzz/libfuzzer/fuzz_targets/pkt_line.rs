#![no_main]
//! libFuzzer entry point for `aspen_fuzz::proto::pkt_line`; see that function for
//! what it checks.

libfuzzer_sys::fuzz_target!(|data: &[u8]| aspen_fuzz::proto::pkt_line(data));
