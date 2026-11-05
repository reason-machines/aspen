#![no_main]
//! libFuzzer entry point for `aspen_fuzz::pack::stitch`; see that function for
//! what it checks.

libfuzzer_sys::fuzz_target!(|data: &[u8]| aspen_fuzz::pack::stitch(data));
