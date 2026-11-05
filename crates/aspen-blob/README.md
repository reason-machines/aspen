# aspen-blob

Blob store backends for Aspen: where packs, their `.idx` files and entry
tables live. Both implement `aspen_core::blob::BlobStore`.

| Backend | URL (`ASPEN_BLOB_URL`) | Use |
|---|---|---|
| `LocalBlobStore` | `file:///var/lib/aspen/blobs` | `aspen dev`, single-host installs |
| `ObjectBlobStore` (S3) | `s3://<bucket>` or `s3://<bucket>/<prefix>` | S3, MinIO, R2, Ceph and other S3-compatible stores |

`aspen_blob::open(url)` turns the setting into an `AnyBlobStore`. Only the
composition roots (`aspen-server`, `aspen-cli`) depend on this crate; every
other crate uses the trait (`cargo xtask check-deps` enforces this).

## Behaviour every backend shares

- **Write once.** A key's bytes are fixed by the key (pack keys embed the
  pack checksum). Uploading the same file to a key again succeeds without
  sending it twice; uploading a different size is `StoreError::Conflict` and
  leaves the stored blob alone.
- **Durable on return.** `upload_file` returns once the blob is stored:
  `fsync`ed file and directories locally, a completed `PUT` or multipart
  upload on S3. Readers never see a partial blob. Locally, an upload that
  finds the blob already there (a retry, or the loser of a race) syncs the
  existing file and its directories before it returns, because the writer
  that created it may not have synced them yet.
- **Keys do not nest under blobs.** Uploading `a/b` when `a` is a blob is
  `StoreError::Conflict`, as is uploading `a` when keys below `a/` exist
  (locally; object stores allow both).
- **Range reads.** `read_range` returns exactly the asked bytes inside the
  blob, clamps a range that crosses the end, and returns nothing for a range
  at or past the end. Callers that know the blob's size from its `packs`
  row use `read_exact_range`, which reports any short read as
  `StoreError::Corrupt` (an alert), never as a short success.
- **Presigned URLs.** Every backend refuses a TTL of zero or over 7 days;
  callers should ask for at most an hour
  (`aspen_blob::RECOMMENDED_MAX_PRESIGN_TTL`), because SigV4 URLs signed with
  temporary credentials, such as an ECS task role, stop working when the
  credentials expire. S3 URLs are SigV4 links straight to the bucket. A bucket answers a range past
  the end with `416`, which git 2.55 and older cannot recover from, so the
  fetch path gives these only to git 2.56 and later (`docs/PLAN.md`, fetch
  path step 3). Local URLs are HMAC-signed links that Aspen's own HTTP
  server must serve (`LocalUrlSigner`); they answer a range past the end
  with `206` and an empty body. A local store without a signer does not
  presign, and fetches fall back to stitched packs.

## S3 configuration

`open("s3://…")` reads the standard AWS environment through
`object_store`'s `AmazonS3Builder::from_env`: `AWS_REGION`,
`AWS_ENDPOINT_URL` and `AWS_ALLOW_HTTP` (for MinIO), static keys
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), or the ECS task role and web
identity. Credentials never go in the URL.

Uploads up to 16 MiB are one `PUT`; larger files use multipart uploads of
16 MiB parts, four in flight (about `part_size * (concurrency + 2)`, so
96 MiB, of buffers per upload; packs up to 156 GiB). Part sizes must stay
within S3's 5 MiB to 5 GiB. A multipart upload that fails is aborted, but
the process can die before it does, so the bucket also needs a lifecycle
rule that aborts incomplete uploads: the AWS Terraform module sets one day
(`deploy/terraform/aws/storage.tf`, checked by its `tftest`).

TLS uses rustls with the `ring` provider and the system's root
certificates (the runtime image installs `ca-certificates`).
`ObjectBlobStore::s3` installs `ring` as the process default only if no
provider is installed yet; the server binary should install its provider
explicitly at startup.

## Tests

`tests/conformance.rs` is one suite, with each check naming the invariant
it pins, run against every backend: the local store, the `object_store`
wrapper over an in-memory store, and the S3 backend against a real MinIO.
`tests/object_faults.rs` wraps the in-memory store in one that fails
chosen operations, to check that failed reads are retryable errors and
failed multipart uploads are aborted.

```sh
cargo test -p aspen-blob                     # local and in-memory; S3 skipped
eval "$(crates/aspen-blob/scripts/test-minio.sh)"   # starts MinIO in Docker
cargo test -p aspen-blob                     # now S3 too
docker rm -f aspen-test-minio
```

Under CI (`CI` set) the S3 tests fail instead of skipping when
`ASPEN_TEST_S3_ENDPOINT` is missing, so the required `test` job always runs
them against MinIO (the same Chainguard image as `deploy/compose`).

## Not yet

- GCS and Azure Blob: `ObjectBlobStore::new` takes any `object_store`
  store, but `open` only builds S3, and only the `aws` parts of
  `object_store` are compiled in.
- Serving local presigned URLs is the HTTP server's job (B2).
