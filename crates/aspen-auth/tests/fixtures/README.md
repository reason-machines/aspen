# Test fixtures

`test-only-rsa-2048.pk8` is a 2048-bit RSA private key (PKCS#8, DER) made
with `openssl genpkey` for the JWT tests in this directory's parent. `ring`
can sign with RSA keys but cannot generate them, so the tests need one on
disk. It signs nothing outside these tests and grants nothing anywhere;
never trust it in configuration.
