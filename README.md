# Distributed Key-Value Store

## Overview

DistributedKVStore is a Java-based distributed key-value storage system featuring persistent storage, Raft/Multi-Raft replication, sharding, and distributed coordination. It combines Redis-style request handling, storage-engine components, RPC communication, and an HTTP dashboard.

The project explores database internals, fault recovery, and distributed-system reliability, with tests covering persistence, failover, snapshot recovery, and network partitions.

**Live Demo:** [Open DistributedKVStore Dashboard](https://distributedkvstore.onrender.com/)  
**Health Check:** [View API Health Status](https://distributedkvstore.onrender.com/api/health)  
**Source Code:** [GitHub Repository](https://github.com/rahamanmursalim-create/DistributedKVStore)

## Features

- **Raft/Multi-Raft Consensus:** Leader election, replication, failover, and snapshot recovery.
- **Sharding and Rebalancing:** Consistent hashing, shard migration, and ownership coordination.
- **Persistent Storage Engine:** WAL, MemTables, SSTables, manifests, Bloom filters, sparse indexes, and compaction.
- **Storage Technologies:** RocksDB integration and B+ Tree-based storage components.
- **Transaction Management:** MVCC-related components and transaction conflict handling.
- **Read/Write Optimizations:** ReadIndex, follower reads, and asynchronous write batching.
- **Networking and Protocols:** Concurrent TCP client handling, Redis-style RESP commands, and RPC transport.
- **HTTP/HTTPS API and Security:** Health checks, key-value operations, bearer-token authentication, and TLS support.
- **Fault-Tolerance Testing:** Tests for persistence, restart recovery, network partitions, snapshot installation, and multi-process failover.

## Architecture

![DistributedKVStore Architecture](docs/images/distributedkvstore-architecture.png)

The architecture connects runtime startup and operations, client request handling, key-value storage, Raft replication, RPC transport, cluster coordination, and shard management. The components enabled in a particular run depend on the selected entry point and runtime configuration.

## Performance and Benchmarks

A local single-node sequential benchmark using 5,000 writes of 128 bytes reported:

- **Throughput:** 555.7 operations/second
- **P95 latency:** 2.769 ms
- **P99 latency:** 3.512 ms

These measurements reflect the tested workload and local environment. They do not represent distributed-cluster throughput or production availability guarantees. Multi-node performance targets will be established after reproducible distributed-load testing.

## Independent Development

Independently designed and developed DistributedKVStore end-to-end, taking ownership of system architecture, implementation, testing, and validation.

- **Distributed Systems:** Implemented and extended Raft/Multi-Raft components for replication, leader election, failover, snapshot installation, and recovery.
- **Storage Engine:** Developed storage components involving WAL, MemTables, SSTables, B+ Trees, RocksDB integration, and compaction.
- **Sharding:** Built consistent hashing, shard migration, rebalancing, and ownership coordination.
- **Networking and Security:** Implemented TCP/RESP request handling, RPC communication, HTTP/HTTPS APIs, authentication, and TLS integration.
- **Testing and Reliability:** Developed tests and validation scripts for concurrency, persistence, crash recovery, network partitions, and multi-process failover.

## Technologies

- **Language:** Java
- **Build Tool:** Apache Maven
- **Distributed Systems:** Raft, Multi-Raft, replication, consensus, and sharding
- **Storage Technologies:** RocksDB, B+ Trees, WAL, MemTables, SSTables, Bloom filters, and compaction
- **Database Concepts:** Key-value storage, MVCC transaction components, snapshot management, and transaction conflict handling
- **Networking and Protocols:** TCP, RESP, RPC, HTTP, and HTTPS
- **Security:** TLS and bearer-token authentication
- **Tools:** Git, GitHub, and PowerShell

## Testing and Validation

Reported results from the local Windows 11 environment using Java 17.0.20.1 and Maven 3.10.0:

- Full custom test suite: **43 passed, 0 failed**
- Raft snapshot installation test: **20/20 repeated runs passed**
- Web server integration test: **20/20 repeated runs passed**
- One-hour soak test: **47/47 cycles passed**
- Final clean test suite: **43 passed, 0 failed**

These results describe the tested local environment and do not establish production readiness or physical multi-host reliability.

## Build and Test

Requirements for the reported validation setup: Java 17 and Maven 3.10.0.

Check the installed versions:

```powershell
java -version
mvn -version
```

Run the project's custom test suite from the repository root:

```powershell
mvn "-Daether.syncContext.named.factory=rwlock-local" clean test
```

Run the one-hour soak test:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\verify-long-soak.ps1" -DurationHours 1 -RequireEveryTestPresent
```

Review the cycle logs and final summary before treating a validation run as successful. Do not run multiple Maven builds concurrently against the same working tree.

## Current Limitations

Physical multi-host cluster validation, comprehensive firewall-scope review, and certificate rotation against a deployed test environment require separate verification. Local tests and single-host container tests are not substitutes for genuine multi-host validation.

## Project Layout

```text
DistributedKVStore/
├── .gitignore
├── Dockerfile
├── Dockerfile.verify
├── LICENSE
├── README.md
├── pom.xml
├── project-layout.txt
├── test-compile-errors.txt
├── docs/
│   ├── VALIDATION_GAP_CLOSURE.md
│   └── images/
│       └── distributedkvstore-architecture.png
├── scripts/
│   ├── PHYSICAL_CLUSTER_TEST.md
│   ├── PHYSICAL_CLUSTER_VALIDATION.md
│   ├── run-docker-cluster-check.ps1
│   ├── run-raft-benchmark.ps1
│   ├── run-raft-nic-check.ps1
│   ├── run-raft-soak.ps1
│   ├── test-deployment-security.ps1
│   ├── test-physical-cluster.ps1
│   ├── validation-common.ps1
│   ├── verify-benchmark-repeatability.ps1
│   ├── verify-live-security.ps1
│   ├── verify-long-soak.ps1
│   └── verify-physical-cluster.ps1
└── src/
    ├── main/java/
    │   ├── Main.java
    │   └── com/codecrafters/redis/
    │       ├── ClientHandler.java
    │       ├── Server/
    │       ├── Networks/
    │       │   ├── client/
    │       │   ├── handler/
    │       │   ├── protocol/
    │       │   └── server/
    │       ├── cluster/
    │       │   ├── ClusterManager.java
    │       │   ├── ClusterMembership.java
    │       │   ├── ClusterRuntimeConfig.java
    │       │   ├── NodeConfig.java
    │       │   └── NodeStatus.java
    │       ├── command/
    │       ├── common/util/
    │       ├── protocol/
    │       ├── raft/
    │       │   ├── MultiRaftStore.java
    │       │   ├── RaftGroup.java
    │       │   ├── RaftGroupManager.java
    │       │   ├── RaftLog.java
    │       │   ├── RaftLogEntry.java
    │       │   ├── RaftMetadataStore.java
    │       │   ├── RaftNode.java
    │       │   ├── RaftStorage.java
    │       │   ├── ReadIndex.java
    │       │   ├── Snapshot.java
    │       │   └── election/
    │       ├── rpc/
    │       │   ├── RpcClient.java
    │       │   ├── RpcCodec.java
    │       │   ├── RpcMessage.java
    │       │   ├── RpcServer.java
    │       │   ├── RpcTransportClient.java
    │       │   ├── RpcTransportServer.java
    │       │   └── TlsContext.java
    │       ├── sharding/
    │       ├── storage/
    │       │   ├── AsyncWriteBatcher.java
    │       │   ├── BPlusTree.java
    │       │   ├── BloomFilter.java
    │       │   ├── KeyValueAccess.java
    │       │   ├── MemTable.java
    │       │   ├── MVCCTransaction.java
    │       │   ├── RedisStore.java
    │       │   ├── RocksDBBPlusTreeStore.java
    │       │   ├── RocksDBEngine.java
    │       │   ├── SSTable.java
    │       │   ├── SSTableManifest.java
    │       │   ├── StorageEngine.java
    │       │   ├── StorageManager.java
    │       │   ├── TransactionConflictException.java
    │       │   ├── ValueEntry.java
    │       │   └── WriteAheadLog.java
    │       └── web/
    │           └── WebServer.java
    └── test/java/com/codecrafters/redis/
        ├── AllTestsRunner.java
        ├── RedisServerTest.java
        ├── TcpRespBenchmark.java
        ├── client/KVClientIntegrationTest.java
        ├── cluster/
        │   ├── ClusterMembershipTest.java
        │   └── ClusterRuntimeConfigTest.java
        ├── raft/
        │   ├── MultiRaftBenchmark.java
        │   ├── MultiRaftStoreTest.java
        │   ├── RaftBenchmark.java
        │   ├── RaftDynamicMembershipTest.java
        │   ├── RaftGroupConsensusTest.java
        │   ├── RaftLinearizabilityTest.java
        │   ├── RaftLogPersistenceTest.java
        │   ├── RaftLogTest.java
        │   ├── RaftMultiProcessTest.java
        │   ├── RaftNetworkPartitionTest.java
        │   ├── RaftProcessNode.java
        │   ├── RaftRebalanceIntegrationTest.java
        │   ├── RaftSlowDiskFaultTest.java
        │   └── RaftSnapshotInstallTest.java
        ├── rpc/
        │   ├── RpcCodecTest.java
        │   ├── RpcTransportTest.java
        │   └── TlsTransportTest.java
        ├── sharding/
        ├── storage/
        │   ├── AsyncWriteBatcherTest.java
        │   ├── BPlusTreeMemTableTest.java
        │   ├── BloomFilterTest.java
        │   ├── MVCCTransactionTest.java
        │   ├── RedisStoreBPlusTreeTest.java
        │   ├── RedisStoreRecoveryTest.java
        │   ├── RedisStoreRocksDBIntegrationTest.java
        │   ├── RocksDBBPlusTreeStoreTest.java
        │   ├── SSTableBenchmark.java
        │   ├── SSTableManifestTest.java
        │   ├── SSTableTest.java
        │   ├── StorageEngineCompactionTest.java
        │   ├── StorageEngineCompactionWalTest.java
        │   ├── StorageEngineConcurrencyTest.java
        │   ├── StorageEngineConcurrentDeleteTest.java
        │   ├── StorageEngineOrphanSSTableTest.java
        │   ├── StorageEngineTest.java
        │   ├── StorageEngineWalRecoveryTest.java
        │   └── WriteAheadLogTest.java
        └── web/WebServerIntegrationTest.java
```

Generated build output and local test artifacts are intentionally excluded from this layout.

## License

The repository contains a [`LICENSE`](LICENSE) file. Read the full terms
before using, modifying, or redistributing this project.

[View the LICENSE file on
GitHub](https://github.com/rahamanmursalim-create/DistributedKVStore/blob/main/LICENSE)
