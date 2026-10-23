# Validation gap-closure patch

This patch adds reusable Windows PowerShell validation tooling; it does **not** replace production Java code or prove a live multi-machine deployment by itself.

## Why the last three gaps cannot honestly be marked PASS here

- The current uploaded source bundle does not contain all files named by the latest Code/Codex log. In particular, it lacks the newer TLS/snapshot/soak scripts and tests reported in that log. Do not replace the latest working tree with this older source bundle. Copy only the new uniquely named `scripts/verify-*.ps1` tools and this document into the latest checkout.
- This execution environment has Java 21, no Maven executable, and no independently reachable peer host. Therefore it cannot recompile and verify the user's latest code on Java 17, run its full soak, or execute a physical-machine cluster test.
- No live deployment URL/token or production host certificate details were provided as part of the latest source tree, so deployed TLS/firewall/certificate-rotation verification was not run.

## Included tools

- `scripts/verify-long-soak.ps1`: sequential Java 17 long soak that runs the current targeted Raft/transaction/snapshot/TLS integration-test entry points, records per-test logs, can run a full suite periodically, and emits JSON/CSV summaries.
- `scripts/verify-live-security.ps1`: checks OS-validated HTTPS and RESP TLS handshakes, current certificate SHA-256 fingerprints, HTTP token rejection/acceptance, RESP `AUTH`/`PING`, plaintext rejection on a TLS port, optional Raft mTLS client-certificate handshake, and selected firewall ports from the caller's network vantage point.
- `scripts/verify-physical-cluster.ps1`: tests reachability for three or more named host endpoints and checks ports that should be unreachable from the current client. It is a preflight, not proof of Raft correctness or physical hardware identity.
- `scripts/verify-benchmark-repeatability.ps1`: runs standalone TCP/RESP and local Multi-Raft benchmark harnesses repeatedly and stores raw output, environment metadata, CSV and JSON. Optional thresholds are explicit operator inputs, not assumed SLOs.
- `scripts/PHYSICAL_CLUSTER_VALIDATION.md`: host setup, firewall review, certificate rotation, one-hour/24-hour soak commands, failure/partition sequence, and required evidence.

## Results actually observed in this sandbox (uploaded archive only)

The archive contains compiled `target/classes` and `target/test-classes`, but those class files may be newer than the source files in the bundle. These results are therefore evidence about the bundled compiled classes, **not a fresh build of the latest Windows working tree**.

| Test | Observed result |
|---|---|
| `RaftMultiProcessTest` | PASS: leader failure, quorum failover, restarted node catch-up |
| `RaftGroupConsensusTest` | PASS |
| `RaftDynamicMembershipTest` | PASS: learner catch-up, joint replacement, leader removal |
| `RaftRebalanceIntegrationTest` | PASS: 80 keys moved, 3 replicas, TTL preserved |
| `MultiRaftStoreTest` | PASS |
| `RpcTransportTest` | PASS |
| `WebServerIntegrationTest` | PASS |
| `MultiRaftBenchmark` | 3 local runs; no operation errors; results below |

Benchmark environment: Linux 6.18.55 container, 4 CPUs exposed, OpenJDK 21.0.12.1, 1 GiB max JVM heap; two Raft groups, three local replicas per group, four workers, 50% reads/50% writes, 512 seeded keys. This is a constrained local sandbox, not the user's Windows 11/JDK 17 machine and not a physical multi-host cluster.

| Run | Throughput | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|
| 1 | 432.1 ops/s | 7.534 ms | 22.373 ms | 28.621 ms | 0 |
| 2 | 434.3 ops/s | 7.114 ms | 22.002 ms | 29.273 ms | 0 |
| 3 | 380.2 ops/s | 8.056 ms | 22.749 ms | 28.833 ms | 0 |

These measurements are not an availability SLO and should not replace benchmarks on the target hardware.

## Still requires your environment

1. Copy the new verification scripts into the **latest Windows checkout**, not this extracted archive.
2. Run the one-hour soak first; only after it succeeds without failures, schedule the 24-hour soak.
3. Run `verify-live-security.ps1` against every deployed HTTPS, RESP TLS, and Raft/mTLS endpoint from both the intended management network and an untrusted-client network, with approved ports and current/previous certificate fingerprints.
4. Provide at least three independently identified reachable physical hosts and routable peer addresses, then follow `PHYSICAL_CLUSTER_VALIDATION.md` for leader kill, partition/heal, restart, catch-up, snapshot install, membership change, and migration/fencing checks.

Until items 2–4 are actually executed and evidence is preserved, mark the corresponding items PARTIAL/BLOCKED rather than PASS.

Additional TCP/RESP benchmark in this sandbox (8 clients, 750 measured operations per client, 100 warmup operations per client, 64-byte values):

| Run | Throughput | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|
| 1 | 4,652.4 ops/s | 1.997 ms | 3.683 ms | 5.958 ms | 0 |
| 2 | 5,191.9 ops/s | 2.097 ms | 3.267 ms | 4.563 ms | 0 |
| 3 | 5,455.0 ops/s | 2.196 ms | 3.062 ms | 3.322 ms | 0 |

The smaller 8-client benchmark is intentionally not compared with the earlier Windows/JDK 27 500-write benchmark: hardware, JVM, operation count, and concurrency differ. Do not turn either into a production throughput/latency claim.
