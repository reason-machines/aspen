#![no_main]
//! libFuzzer entry point for `aspen_fuzz::jwt::jwt`; see that function for
//! what it checks.

libfuzzer_sys::fuzz_target!(|data: &[u8]| aspen_fuzz::jwt::jwt(data));
