#![no_main]
//! libFuzzer entry point for `aspen_fuzz::proto::v2_command`; see that function for
//! what it checks.

libfuzzer_sys::fuzz_target!(|data: &[u8]| aspen_fuzz::proto::v2_command(data));
