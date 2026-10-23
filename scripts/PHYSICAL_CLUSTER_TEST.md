# Physical cluster verification procedure

The loopback and local-NIC tests are not physical-machine verification. This procedure requires three reachable hosts (or VMs on separate network endpoints), OpenSSH access, Java 17+, synchronized clocks, and bidirectional TCP connectivity for each Raft listener.

## Preflight

From the coordinator, run `scripts/test-physical-cluster.ps1` with a distinct `-NodeAddress` per host, the corresponding `-RaftPort` values, and a remote command that prints Java version and confirms the release artifact exists. Start all three nodes first so the peer listeners are reachable. Example:

```powershell
./scripts/test-physical-cluster.ps1 `
  -NodeAddress 10.0.0.11,10.0.0.12,10.0.0.13 `
  -RaftPort 7101,7102,7103 `
  -RemoteCommand 'java -version; test -f /opt/distributed-kvstore/app.jar'
```

The script rejects loopback addresses and fails on unreachable endpoints. Firewall policy must allow each node to reach all peer Raft ports; keep client ports limited to trusted networks and use mutual TLS plus client authentication for any untrusted network.

After deployment, run `scripts/test-deployment-security.ps1` from a trusted coordinator. Set `KV_CLIENT_TOKEN` and `KV_TLS_PASSWORD` in that PowerShell process first; provide the expected SHA-1 thumbprint of the deployed leaf certificate, HTTPS and RESP TLS ports, and a Raft peer address/port. It verifies the actual remote HTTPS certificate, bearer rejection/acceptance, plaintext rejection, RESP TLS plus AUTH/PING, and peer-port reachability. Run once before rotation and again with the new expected thumbprint after restarting nodes; the old thumbprint must fail in that second run.

Review Windows Defender Firewall (or the host's equivalent) on every node before exposure. Permit inbound Raft ports only from the other voter addresses, permit client ports only from the application subnets, and deny public sources. Record the effective profile and rule scopes. The verifier checks routing/reachability; it cannot prove a firewall rule is narrowly scoped, so capture and review the host firewall policy export as a separate artifact.

## Run and record

1. Deploy the exact same commit and Java 17+ runtime to all hosts. Record `git rev-parse HEAD`, `java -version`, OS/kernel, network path, and certificate fingerprints.
2. Configure each process with the same group ID and voter IDs, but its own bind/advertised address, peer address map, data directory, TLS identity and trusted CA. Never reuse a node data directory.
3. Start all three processes and wait for one stable leader. Write a unique marker and verify its committed value on all replicas.
4. Kill the leader process. Verify a new leader commits a second marker and the killed process catches up after restart.
5. On a disposable run, block traffic between one node and the other two using host firewall rules. Verify the majority continues, the isolated minority cannot commit, then remove the rule and verify catch-up. Record exact commands and timestamps; restore the original firewall policy immediately after each run.
6. Repeat with a majority partition (no side should claim successful writes), a process crash during durable writes, and certificate rotation. Verify recovery and compare committed markers on all replicas.
7. Retain application logs, firewall change log, host metrics, and a machine-readable result with pass/fail per step. Do not call this complete until these artifacts come from distinct hosts.

`RaftMultiProcessTest` currently launches three local JVMs and does not perform this remote orchestration. This document intentionally does not describe that as a physical-host test.
