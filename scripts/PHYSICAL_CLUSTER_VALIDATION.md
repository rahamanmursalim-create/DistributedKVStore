# DistributedKVStore: physical-cluster, TLS, soak, and benchmark verification

This runbook separates **actual evidence** from a prepared test harness. Local JVMs and loopback TCP are useful integration tests, but they are not a physical-machine cluster test. A host test is `PASS` only after commands run on distinct machines and evidence is saved.

## 0. Preconditions and stop conditions

Use three independently reachable machines (ideally three physical hosts, not three JVMs or containers on one host). Every node must have a unique node ID, a unique data directory, a routable peer address, and compatible application/JDK versions.

Before testing:

1. Use a dedicated test cluster and fresh data directories. Never partition or kill nodes in a production cluster.
2. Confirm the same intended JAR/version is installed on every node; record its SHA-256.
3. Record OS, Java, CPU/RAM, node ID, bind address, advertised address, peer configuration, group count, and storage backend per node.
4. Sync host clocks using the site's supported time service. Record UTC timestamps.
5. Take a backup and agree on the test window and recovery/stop procedure.
6. Do not run multiple Maven builds or test suites simultaneously in the same checkout.

## 1. Firewall policy to review

Write down the intended policy before probing it:

- RESP/TLS client port: allowed only from approved application/client subnets.
- HTTPS/API port: allowed only from approved management/client subnets; all protected routes require a token.
- Raft peer RPC port(s): allowed only between configured cluster peers and administrators as required. Do not expose them to the public Internet.
- SSH/WinRM: restricted to the management subnet and key/certificate-based administrative access.
- All unrelated inbound ports: default deny.

From an untrusted client network, run `verify-physical-cluster.ps1` with the expected open ports in `-Ports` and private/admin ports in `-PortsExpectedClosedFromThisClient`. Run it again from a peer-network host: ports that should be peer-reachable must pass there. A single vantage point cannot prove the complete firewall policy.

For Windows nodes, save `Get-NetFirewallProfile` and relevant `Get-NetFirewallRule` output on each host. For Linux nodes, save the active firewall rules (`nft list ruleset`, `firewall-cmd --list-all`, or the site's equivalent). Review rule source/destination scopes, not only port numbers.

## 2. Host reachability preflight

From the management workstation, after putting real host addresses in the arguments:

```powershell
$nodes = @('n1=10.10.0.11', 'n2=10.10.0.12', 'n3=10.10.0.13')
.\scripts\verify-physical-cluster.ps1 -Nodes $nodes -Ports @(6379,7000,8080) `
  -PortsExpectedClosedFromThisClient @(7001,9000) `
  -OutputPath .\artifacts\physical-preflight.json
```

Adjust ports to the actual configuration. This is only a reachability/firewall preflight. It does not prove the hosts are physical hardware or that a cluster is consistent.

## 3. TLS deployment and certificate rotation

Set `KV_CLIENT_TOKEN` in the shell environment without printing it or passing it on the command line. Ensure the HTTPS endpoint certificate chains to a trusted CA and matches its DNS name. Do not disable certificate validation to make a test pass.

Example after adapting addresses/ports to the actual deployment:

```powershell
$secure = Read-Host 'Paste test-cluster client token' -AsSecureString
$ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
try { $env:KV_CLIENT_TOKEN = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) }
try { .\scripts\verify-live-security.ps1 `
  -HttpsHealthUrl 'https://node1.example.test:8443/api/health' `
  -ExpectedHttpsSha256 '<current SHA-256 fingerprint>' `
  -PreviousHttpsSha256 '<old fingerprint after a planned rotation>' `
  -RespTlsHost 'node1.example.test' -RespTlsPort 6380 `
  -ExpectedRespSha256 '<RESP endpoint fingerprint>' `
  -PortsExpectedClosedFromClient @(7000,9000) `
  -ReportPath .\artifacts\security-node1.json } finally { Remove-Item Env:KV_CLIENT_TOKEN -ErrorAction SilentlyContinue }
```

The script tests trusted TLS connection, HTTP unauthenticated rejection, authenticated HTTP success, RESP TLS `AUTH`/`PING`, plaintext rejection on the TLS RESP port, and selected ports that must be unreachable from this client. If the Raft listener is TLS/mTLS, provide its host/port and a test client PFX as supported by the script. Never store certificate passwords in the repository or logs.

Perform an actual certificate rotation as a controlled maintenance procedure:

1. Record the current peer/server certificate fingerprints and validate that all clients trust the issuing CA.
2. If changing CA, distribute the new CA certificate to the truststores first while the old CA remains trusted.
3. Rotate one node's leaf certificate at a time, using unique SANs that match the advertised peer hostname/IP. Restart only the intended test node.
4. Run the security verifier against every HTTPS, RESP TLS, and peer-RPC endpoint; record the new fingerprint and handshake result.
5. Verify cluster quorum remains healthy after each node rotation before continuing to the next node.
6. Once all nodes/clients use the new CA, remove the old CA from truststores if revocation is intended, then test that old-only trust no longer succeeds. Merely observing a new fingerprint does not prove that old credentials/certificates are revoked.

The verifier cannot rotate a remote certificate or firewall rule by itself. The operator must carry out the deployment change and then capture the real post-change evidence.

## 4. One-hour or 24-hour local-process soak

Run from the latest working tree after ensuring no other Maven build is running:

```powershell
# One hour, using Java 17
$env:JAVA_HOME = "$env:LOCALAPPDATA\Programs\Temurin17\PFiles64\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\scripts\verify-long-soak.ps1 -DurationHours 1 -RequireEveryTestPresent

# 24 hours (run only on a plugged-in, monitored test machine)
.\scripts\verify-long-soak.ps1 -DurationHours 24 -RequireEveryTestPresent -FullSuiteEveryCycles 6
```

Each soak cycle runs `mvn test` through Maven Surefire/JUnit Jupiter. Integration test classes run in separate JVM forks because several scenarios launch child Raft processes. Each cycle has a bounded runtime and its own log; the run also performs a final clean suite. Expected long-soak test classes are recorded as missing; use `-RequireEveryTestPresent` to make missing coverage fail closed. The script never launches concurrent Maven invocations.

A one-day run requires the workstation not to sleep/reboot and sufficient free disk for logs. For long runs, save system event logs, process exit data, and the complete `summary.json`/`results.csv`. A passed 24-hour local process soak remains local-host evidence, not a physical-cluster availability SLO.

## 5. Repeatable benchmark protocol

```powershell
.\scripts\verify-benchmark-repeatability.ps1 -RunCount 3 `
  -TcpClients 32 -TcpOperationsPerClient 5000 -TcpWarmupOpsPerClient 1000
```

It runs the standalone TCP/RESP and local Multi-Raft benchmark harnesses, records raw output and machine/JDK metadata, and exports CSV/JSON. Keep the two modes separate: the Multi-Raft harness uses local JVMs/loopback, while standalone TCP/RESP has a different durability/replication path. Do not copy a result to the README without hardware, value size, workload, concurrency, repetitions, error rate, percentiles, JDK, storage backend, and topology.

Set an SLO only if it is a deliberate project target. Passing a short benchmark does not establish 99.9% availability. For an availability target, define a measurement window, success criteria, client-visible error budget, and failover/maintenance exclusions first.

## 6. Physical-node failure/partition test plan

Run only on a dedicated test cluster and keep an observer/client outside the node being failed:

1. Start all nodes; verify each node's identity from its own startup log and confirm all configured peers are reachable.
2. Write uniquely identified keys through a client entrypoint, record each request ID, result, timestamp, and returned value. Read those keys through every documented consistency mode.
3. Stop the current leader process on one host. Confirm the surviving quorum elects exactly one leader and accepts writes; confirm no successful write is acknowledged without quorum.
4. Restart the old leader. Verify it catches up before accepting authoritative reads/writes and that committed keys remain identical.
5. On the test network/firewall, isolate one node bidirectionally from the other two for a bounded interval. The minority must not acknowledge conflicting committed writes; the majority should remain available. Heal the partition and verify convergence and catch-up.
6. Repeat with a different node and with controlled delay/connection reset where safe. Record every firewall rule change and remove temporary rules even when a test fails.
7. Force a follower behind the retained log if the implementation supports it; verify snapshot transfer, restart mid-transfer, subsequent replication, TTL/deletion retention, and checksum validation.
8. Add a learner, verify catch-up, complete a joint-consensus voter change, restart the leader during a transition, and remove a voter. Verify no conflicting history is committed under old/new configurations.
9. Start a distributed shard migration with concurrent writes, interrupt the source/destination process during copy and cutover, restart, and verify ownership epoch, stale-owner rejection, TTLs, deletes, transactions, idempotency records, and source/destination checksums.
10. Save logs and client operation histories. If linearizability is claimed, feed the history to the project's checker/reference model and preserve the output.

For the Multi-Raft ownership rebalance, set a unique `KV_CLUSTER_ADMIN_TOKEN` on all nodes and retain the normal `KV_CLIENT_TOKEN` for API authentication. Find the current leader for the first configured group from `/api/raft/status`, then send exactly one request to that node:

```powershell
$headers = @{
  'X-Cluster-Admin-Token' = $env:KV_CLUSTER_ADMIN_TOKEN
  Authorization = "Bearer $env:KV_CLIENT_TOKEN"
}
Invoke-RestMethod -Method Post -Uri 'https://kv1.example.test:8443/api/raft/rebalance' `
  -Headers $headers -ContentType 'text/plain' -Body 'g1,g2'
```

The body lists the active, already provisioned Raft groups. The request commits the migration intent, freeze, copy, ownership-epoch cutover and unfreeze through Raft; live routers adopt the committed ownership manifest on their next request. If the coordinator stops mid-transition, the newly elected first-group leader automatically retries the committed target. Inspect `/api/raft/status` if routing remains frozen; after confirming the target, an operator may submit that same target with `X-Resume-Rebalance: true` to trigger an immediate retry. Do not submit a different migration target while a committed transition is active.

For Windows host failure, use the approved process-control method from that host. For network partition, use a temporary, tightly scoped firewall rule for the peer port only. Do not use a broad firewall disable command. For Linux, use an equivalent host firewall/network namespace tool only on the test cluster.

## 7. Evidence checklist

For each run preserve:
- Timestamp, node ID, hostname, OS, JDK, app/JAR SHA-256 and configured backend.
- Network addresses, ports, firewall rules and TLS certificate SHA-256 fingerprints.
- Exact command line (redact tokens/passwords), exit code, stdout/stderr and process termination reason.
- Start/end state of each node and observed leader/configuration epoch.
- Client operations, timestamps, results, retries, timeouts and final state comparison.
- Benchmark workload/hardware metadata or soak JSON/CSV summary.

Do not mark physical testing `PASS` unless the evidence came from at least three independently identified physical hosts. If using VMs, report it as a multi-host VM test, not a physical-machine test.

## 8. Monitoring and alert validation

Scrape the authenticated Prometheus endpoint (`GET /api/metrics`) over HTTPS from the monitoring network. It exports `kv_process_up`, `kv_process_uptime_seconds`, `kv_store_keys` for standalone/local-shard mode, and per-group `kv_raft_group_local_keys`, `kv_raft_group_is_leader`, `kv_raft_group_term`, `kv_raft_group_commit_index`, and `kv_raft_group_ownership_epoch` for Raft mode. Raft key counts are local and replica-duplicated; scrape does not run a quorum read. The endpoint is protected by `KV_CLIENT_TOKEN`; store the token in the monitoring system's secret manager and never in a scrape URL or dashboard export.

Example Prometheus configuration (replace addresses and provide credentials from a secret-managed file):

```yaml
scrape_configs:
  - job_name: distributed-kvstore
    scheme: https
    metrics_path: /api/metrics
    scrape_interval: 15s
    static_configs:
      - targets: ['kv1.example.test:8443', 'kv2.example.test:8443', 'kv3.example.test:8443']
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/kv-client-token
```

Suggested initial alerts to exercise in a test environment:

```yaml
groups:
  - name: distributed-kvstore
    rules:
      - alert: KVNodeDown
        expr: up{job="distributed-kvstore"} == 0 or kv_process_up == 0
        for: 1m
      - alert: KVRaftGroupHasNoLeader
        expr: sum by (group) (kv_raft_group_is_leader) == 0
        for: 30s
      - alert: KVStoreNearKeyLimit
        expr: kv_store_keys > 80000 or kv_raft_group_local_keys > 80000
        for: 10m
```

These are starter rules, not a complete monitoring product. Before production, validate alert delivery and on-call acknowledgement, add disk/inode, JVM memory/GC, WAL/Raft-log growth, RPC failure, replication lag, backup, certificate-expiry, and request error/latency telemetry. The application endpoint does not currently export those host/runtime histograms, so obtain them from the host/JVM agent and a client-side synthetic probe. Test both alert firing and resolution and preserve Alertmanager notification evidence.

## 9. Capacity, scalability, and SLO acceptance

Known application limits are defaults, not measured cluster capacity: key 1 KiB, value 1 MiB, 100,000 live keys and 64 MiB live key/value bytes per `RedisStore` (`KV_MAX_*` settings). A deployment with multiple stores has a multiplied theoretical key budget, not a guaranteed capacity; WAL/RocksDB indexes, Raft logs, snapshots, replicas, transactions, compaction space, and operating-system reserve add overhead. There is no validated maximum node, shard, or Raft-group count.

For each production hardware/storage profile, benchmark 1/3/5/9 nodes and 1/2/4/8 groups with WAL and RocksDB; ramp key counts and value sizes to configured limits; include TTL churn, concurrent reads/writes, rebalance, restart and replica catch-up. Record throughput, p50/p95/p99 latency, errors, CPU, heap/GC, disk bytes/IOPS, log/snapshot size, recovery time and replication progress. Stop before 70% disk use or any agreed host safety threshold. Repeat on the target hardware; local benchmark values are not a capacity claim.

Proposed initial service objectives for a three-voter deployment, pending product/operator approval and measurement: monthly write/read API availability 99.9%; acknowledged committed writes lost after a single-node failure 0 (RPO 0); quorum-loss write rejection within 2 seconds; healthy-quorum leader recovery within 30 seconds; and recovery to full replica parity within 15 minutes for the tested dataset. Measure availability with an external probe at 15-second intervals and define maintenance/error-budget exclusions before treating these as an SLO. These targets are a starting proposal, not a demonstrated guarantee; change them if workload, topology or business requirements demand it.

The long-soak runner accepts up to 168 hours. A 24-hour run is:

```powershell
.\scripts\verify-long-soak.ps1 -DurationHours 24 -RequireEveryTestPresent -FullSuiteEveryCycles 6
```

Run on a monitored, powered host with no concurrent Maven builds. Retain the complete artifacts directory and OS event logs; a completed local soak is still not multi-host or production hardware evidence.
