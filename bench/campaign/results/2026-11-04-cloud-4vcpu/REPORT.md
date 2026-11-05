## real-git

### clone (concurrency 4, clones 8, repository 777489f9e09c)

| Target | Memory | Errors | clone p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 16.94 GB | 0 of 8 | 29.4 s | 32.4 s | 32.4 s | 0.13 clones/s | 252.76 MB | 1.0000× |
| local | 16.94 GB | 0 of 8 | 32.9 s | 33.2 s | 33.2 s | 0.12 clones/s | 252.76 MB | 1.0000× |

### fetch-storm (jobs 1000, rounds 2, mode warm, repository 777489f9e09c)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 2000 | 13.4 s | 15.6 s | 15.8 s | 63.43 fetches/s | 16.9 kB | 0.9712× |
| local | 16.94 GB | 0 of 2000 | 39.6 s | 51.8 s | 53.0 s | 19.05 fetches/s | 17.4 kB | 1.0000× |

### fetch-storm-cold (jobs 8, rounds 2, mode cold, repository 777489f9e09c)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 16 | 61.0 s | 63.8 s | 63.9 s | 0.13 fetches/s | 252.78 MB | 1.0003× |
| local | 16.94 GB | 0 of 16 | 69.0 s | 69.5 s | 69.6 s | 0.12 fetches/s | 252.71 MB | 1.0000× |

### fetch-storm-depth1 (jobs 1000, rounds 2, mode cold, depth 1, repository 777489f9e09c)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 1039 of 2000 | 51.6 s | 70.5 s | 73.1 s | 7.47 fetches/s | 12.47 MB | 1.0288× |
| local | 16.94 GB | 0 of 2000 | 471.7 s | 783.5 s | 809.4 s | 1.24 fetches/s | 12.12 MB | 1.0000× |

### writers (writers 16, duration_secs 30.0, repository 777489f9e09c)

| Target | Memory | Errors | push→fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 378 | 198 ms | 451 ms | 624 ms | 12.38 pushes/s | - | - |
| local | 16.94 GB | 0 of 443 | 1563 ms | 1893 ms | 2174 ms | 14.24 pushes/s | - | - |

| Target | Memory | push p50 | p90 | p99 |
|---|---|---|---|---|
| aspen | 8.35 GB | 1031 ms | 2391 ms | 2983 ms |
| local | 16.94 GB | 1056 ms | 1222 ms | 1356 ms |

## real-git-threshold

### fetch-storm-depth1 (jobs 250, rounds 1, mode cold, depth 1, repository 777489f9e09c)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen-250-jobs | 16.94 GB | 0 of 250 | 20.6 s | 29.6 s | 30.1 s | 8.29 fetches/s | 12.47 MB | - |
| aspen-500-jobs | 16.94 GB | 0 of 500 | 51.6 s | 61.1 s | 62.2 s | 8.03 fetches/s | 12.47 MB | - |

## stale-branch-nightly

### stale-branch

| Target | Memory | N | visited | loaded versions | chain skips | planning p50 | fetch p50 | bytes received |
|---|---|---|---|---|---|---|---|---|
| aspen | 16.94 GB | 10 | 11 | 33 | 1 | 2.4 ms | 21.5 ms | 438 B |
| aspen | 16.94 GB | 100 | 28 | 225 | 6 | 13.1 ms | 32.0 ms | 437 B |
| aspen | 16.94 GB | 1000 | 49 | 63 | 17 | 5.7 ms | 27.5 ms | 411 B |
| aspen-no-compaction | 16.94 GB | 10 | 11 | 33 | 0 | 1.3 ms | 19.8 ms | 440 B |
| aspen-no-compaction | 16.94 GB | 100 | 101 | 225 | 0 | 12.3 ms | 31.3 ms | 441 B |
| aspen-no-compaction | 16.94 GB | 1000 | 1001 | 1002 | 0 | 55.8 ms | 79.1 ms | 410 B |
| local | 16.94 GB | 10 | - | - | - | - | 28.7 ms | 437 B |
| local | 16.94 GB | 100 | - | - | - | - | 34.3 ms | 443 B |
| local | 16.94 GB | 1000 | - | - | - | - | 51.2 ms | 410 B |

| Target | Memory | trunk pushes | push p50, first tenth | push p50, last tenth | p99, last tenth |
|---|---|---|---|---|---|
| aspen | 16.94 GB | 1000 | 19.4 ms | 33.9 ms | 36.2 ms |
| aspen-no-compaction | 16.94 GB | 1000 | 19.4 ms | 33.2 ms | 34.4 ms |
| local | 16.94 GB | 1000 | 25.2 ms | 25.3 ms | 28.2 ms |

## storm1000

### fetch-storm (jobs 1000, rounds 3, mode warm, synthetic 200 files, 20 commits)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 3000 | 4414 ms | 4968 ms | 5099 ms | 194.95 fetches/s | 2.4 kB | 1.0107× |
| local | 8.35 GB | 0 of 3000 | 27.3 s | 31.6 s | 32.4 s | 30.93 fetches/s | 2.4 kB | 1.0000× |

### fetch-storm-depth1 (jobs 1000, rounds 3, mode cold, depth 1, synthetic 200 files, 20 commits)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 3000 | 5051 ms | 6958 ms | 7281 ms | 137.17 fetches/s | 401.8 kB | 1.0000× |
| local | 8.35 GB | 0 of 3000 | 29.7 s | 35.1 s | 36.8 s | 27.64 fetches/s | 401.8 kB | 1.0000× |

## synthetic

### clone (concurrency 4, clones 12, synthetic 5000 files, 200 commits)

| Target | Memory | Errors | clone p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 12 | 1484 ms | 1671 ms | 1675 ms | 2.52 clones/s | 53.36 MB | 1.0000× |
| local | 8.35 GB | 0 of 12 | 1808 ms | 1958 ms | 1961 ms | 2.11 clones/s | 53.36 MB | 1.0000× |

### fetch-storm (jobs 64, rounds 3, mode warm, synthetic 200 files, 20 commits)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 192 | 264 ms | 295 ms | 308 ms | 210.82 fetches/s | 2.5 kB | 1.0037× |
| local | 8.35 GB | 0 of 192 | 539 ms | 598 ms | 611 ms | 105.15 fetches/s | 2.4 kB | 1.0000× |

### fetch-storm-cold (jobs 16, rounds 2, mode cold, synthetic 200 files, 20 commits)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 32 | 133 ms | 157 ms | 158 ms | 100.92 fetches/s | 782.0 kB | 1.0001× |
| local | 8.35 GB | 0 of 32 | 210 ms | 230 ms | 236 ms | 68.91 fetches/s | 782.0 kB | 1.0000× |

### fetch-storm-depth1 (jobs 64, rounds 2, mode cold, depth 1, synthetic 200 files, 20 commits)

| Target | Memory | Errors | fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 128 | 389 ms | 432 ms | 442 ms | 145.84 fetches/s | 401.8 kB | 1.0000× |
| local | 8.35 GB | 0 of 128 | 708 ms | 803 ms | 822 ms | 78.59 fetches/s | 401.7 kB | 1.0000× |

### stale-branch

| Target | Memory | N | visited | loaded versions | chain skips | planning p50 | fetch p50 | bytes received |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 10 | 11 | 33 | 1 | 15.1 ms | 82.4 ms | 440 B |
| aspen | 8.35 GB | 100 | 20 | 33 | 2 | 15.3 ms | 85.0 ms | 440 B |
| aspen | 8.35 GB | 1000 | 26 | 39 | 5 | 16.8 ms | 88.8 ms | 445 B |
| aspen | 8.35 GB | 10000 | 35 | 49 | 10 | 17.9 ms | 126 ms | 409 B |
| aspen-no-compaction | 8.35 GB | 10 | 11 | 33 | 0 | 1.4 ms | 67.3 ms | 438 B |
| aspen-no-compaction | 8.35 GB | 100 | 101 | 225 | 0 | 12.4 ms | 78.7 ms | 439 B |
| aspen-no-compaction | 8.35 GB | 1000 | 1001 | 2017 | 0 | 111 ms | 182 ms | 443 B |
| aspen-no-compaction | 8.35 GB | 10000 | 10001 | 10002 | 0 | 563 ms | 667 ms | 408 B |
| local | 8.35 GB | 10 | - | - | - | - | 32.8 ms | 439 B |
| local | 8.35 GB | 100 | - | - | - | - | 38.4 ms | 439 B |
| local | 8.35 GB | 1000 | - | - | - | - | 47.5 ms | 445 B |
| local | 8.35 GB | 10000 | - | - | - | - | 81.7 ms | 409 B |

| Target | Memory | trunk pushes | push p50, first tenth | push p50, last tenth | p99, last tenth |
|---|---|---|---|---|---|
| aspen | 8.35 GB | 10000 | 28.6 ms | 185 ms | 220 ms |
| aspen-no-compaction | 8.35 GB | 10000 | 27.7 ms | 180 ms | 202 ms |
| local | 8.35 GB | 10000 | 30.0 ms | 31.0 ms | 35.4 ms |

### writers (writers 16, duration_secs 30.0, synthetic 200 files, 20 commits)

| Target | Memory | Errors | push→fetch p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs local |
|---|---|---|---|---|---|---|---|---|
| aspen | 8.35 GB | 0 of 1925 | 202 ms | 282 ms | 384 ms | 63.69 pushes/s | - | - |
| local | 8.35 GB | 0 of 1116 | 758 ms | 1112 ms | 1386 ms | 36.53 pushes/s | - | - |

| Target | Memory | push p50 | p90 | p99 |
|---|---|---|---|---|
| aspen | 8.35 GB | 201 ms | 294 ms | 392 ms |
| local | 8.35 GB | 359 ms | 473 ms | 575 ms |

