# Distributed Key-Value Store

## Overview

DistributedKVStore is a Java-based distributed key-value storage system exploring persistent storage, replication, sharding, and distributed coordination. It combines Redis-style request handling with storage-engine components, Raft-based consensus, RPC communication, and an HTTP dashboard.

The project focuses on database internals, distributed-system behavior, fault recovery, and reliable data access.

**Live Demo:** [Open DistributedKVStore Dashboard](https://distributedkvstore.onrender.com/)  
**Health Check:** [View API Health Status](https://distributedkvstore.onrender.com/api/health)  
**Source Code:** [GitHub Repository](https://github.com/rahamanmursalim-create/DistributedKVStore)

## Features

- **Multi-Raft consensus:** Replication, leader election, failover, and recovery components.
- **Consistent hashing:** Hash-based key distribution and shard placement.
- **Dynamic shard rebalancing:** Shard migration and ownership coordination components.
- **Hybrid storage components:** RocksDB integration, LSM-oriented persistence, and an in-memory B+ Tree implementation.
- **MVCC components:** Transaction and conflict-handling functionality.
- **Read optimizations:** ReadIndex and follower-read support.
- **Batched asynchronous writes:** Write batching support in the storage layer.
- **Persistence and recovery:** WAL, MemTables, SSTables, manifests, Bloom filters, sparse indexes, and compaction.
- **Networking and APIs:** TCP/RESP request handling, RPC transport, and HTTP/HTTPS dashboard endpoints.
- **Security:** Configurable bearer-token authentication and TLS support.
- **Fault testing:** Tests for persistence, restart recovery, network partitions, snapshot installation, and multi-process failover.

## Architecture

![DistributedKVStore Architecture](docs/images/distributedkvstore-architecture.png)

The architecture connects runtime startup and operations, client request handling, key-value storage, Raft replication, RPC transport, cluster coordination, and shard management. The components enabled in a particular run depend on the selected entry point and runtime configuration.

## Performance Targets

- Approximately **50K QPS throughput**
- **Under 10 ms P99 latency**
- **99.9% availability**

These are target figures, not verified results in the current local evidence. Treat them as achieved metrics only after reproducible workload benchmarks and appropriate availability testing support them. A recorded local sequential benchmark using 5,000 writes of 128 bytes measured 555.7 operations/second, P95 latency of 2.769 ms, and P99 latency of 3.512 ms; that benchmark does not establish the targets above.

## My Contributions

Independently designed and developed DistributedKVStore end-to-end, including architecture, implementation, testing, and validation.

- Designed and integrated the storage, networking, replication, sharding, and coordination components.
- Built and extended Raft/Multi-Raft functionality, including leader election, replication, failover, snapshot installation, and recovery.
- Implemented storage components using WAL, MemTables, SSTables, B+ Trees, RocksDB integration, and compaction workflows.
- Developed consistent hashing, shard migration, rebalancing, and ownership coordination components.
- Implemented HTTP/HTTPS APIs, authentication, and TLS integration.
- Created tests and repeatable validation workflows for concurrency, persistence, crash recovery, network partitions, multi-process failover, and snapshot recovery.

## Technologies

- Java
- Apache Maven
- Distributed systems: replication, consensus, Raft/Multi-Raft, sharding
- Storage: RocksDB, B+ Trees, WAL, MemTables, SSTables, Bloom filters, and compaction
- Database concepts: key-value storage, MVCC components, snapshot management, and transaction conflict handling
- Networking: TCP, RESP, RPC, HTTP, and HTTPS
- Security: TLS and bearer-token authentication
- Tools: Git, GitHub, PowerShell

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
