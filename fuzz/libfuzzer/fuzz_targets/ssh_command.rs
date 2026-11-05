#![no_main]
//! libFuzzer entry point for `aspen_fuzz::front::ssh_command`; see that function for
//! what it checks.

libfuzzer_sys::fuzz_target!(|data: &[u8]| aspen_fuzz::front::ssh_command(data));
