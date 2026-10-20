# Distributed Key-Value Store

DistributedKVStore is a Java-based key-value storage project focused on persistent storage, Redis-style request handling, replication, sharding, and distributed coordination. It brings together storage-engine components, networking and protocol handling, Raft-related components, and a web interface to explore the design of a distributed storage system.

> **Project status:** This is an engineering and learning
> implementation. The presence of a component in the repository does
> not, by itself, prove production readiness, a particular consistency
> guarantee, or a specific performance level.

**Live Demo:** [Open DistributedKVStore Dashboard](https://distributedkvstore.onrender.com/)

**Health Check:** [View API Health Status](https://distributedkvstore.onrender.com/api/health)

**Source Code:** [GitHub Repository](https://github.com/rahamanmursalim-create/DistributedKVStore)

## Table of Contents

-   [Overview](#overview)
-   [Architecture](#architecture)
-   [Components](#components)
-   [Technology Stack](#technology-stack)
-   [Build and Test](#build-and-test)
-   [Run](#run)
-   [Performance and Reliability](#performance-and-reliability)
-   [Project Layout](#project-layout)
-   [License](#license)

## Overview

The project explores key-value database internals and distributed-system concepts, including persistent storage, client-server networking, protocol handling, replication, sharding, and cluster coordination. It also includes a web interface for interacting with the system.

## Features

### TCP Server and Redis-Style Protocol
- Concurrent TCP client handling with RESP request parsing and response serialization
- Support for fragmented input and pipelined requests
- Redis-style commands, including `PING`, `ECHO`, `SET`, `GET`, `DEL`, and `EXISTS`
- Key expiration using `EX` and `PX` options
- Additional commands for key listing, server information, and command metadata

### Persistent Storage Engine
- Write-Ahead Logging (WAL) with checksums and recovery
- In-memory MemTable and sorted, immutable SSTables
- Manifest-based SSTable tracking and recovery
- Sparse-index lookups and Bloom filters
- Bounded LRU record caching
- Flush and compaction workflows
- Tests for concurrent operations, deletion, restart recovery, and interrupted operations

### HTTP Dashboard and REST API
- Embedded web dashboard for interacting with stored key-value data
- JSON health and key-list endpoints
- REST operations for creating, reading, updating, and deleting keys
- Search and filtering with loading feedback, notifications, and delete confirmation
- Shared key-value access between the TCP service and HTTP API

### Database and Storage Concepts
- Key-value data access and expiration management
- MVCC-related transaction components
- RocksDB integration components
- Separate storage-engine implementation for exploring LSM-style persistence

### Distributed-System Components
- Raft log and consensus-related components
- Multi-Raft and replication-related code
- RPC messaging, encoding, and validation
- Cluster membership and node-management components
- Consistent hashing, shard metadata, and data-migration components

### Testing and Reliability
- Unit and integration tests for protocol handling, networking, storage, and the HTTP API
- WAL replay, checksum validation, and recovery tests
- Storage concurrency, compaction, and deletion-recovery tests
- Tests for Raft logs, RPC codecs, cluster membership, and shard metadata

## Architecture

![DistributedKVStore
architecture](docs/images/distributedkvstore-architecture.png)

The diagram illustrates the relationships between the runtime startup,
Redis-style TCP service, request parsing/dispatch, in-memory key-value
store, persistent LSM-oriented storage components, and HTTP
dashboard/API.

### Conceptual request flow

1.  A client connects to the TCP service.
2.  A connection handler processes the client connection.
3.  Protocol parsing and command dispatch interpret the request.
4.  The key-value store processes the operation and interacts with
    persistence components as configured.
5.  The web component offers a separate HTTP dashboard/API entry point.

The actual startup path and enabled components depend on the entry point
and configuration. Check `pom.xml` and the source code before assuming
every component is enabled in a default run.

## Components

The repository contains the following major components and supporting modules.

| Area | Components and Responsibilities |
|---|---|
| **Persistent Storage** | Write-Ahead Log (WAL), MemTable, SSTables, manifest, Bloom filters, compaction, and storage manager |
| **Key-Value Operations** | Redis-style store, key-value access, and expiration state |
| **Transactions** | MVCC-related code and transaction conflict handling |
| **Networking** | TCP/Netty client and server, connection handling, and request processing |
| **Protocols** | RESP reader/writer and value types, RPC transport, and message codecs |
| **Replication and Consensus** | Raft nodes, logs, snapshots, ReadIndex, and Multi-Raft components |
| **Sharding** | Consistent hashing, shard metadata, shard management, migration, and rebalancing components |
| **Cluster Management** | Node configuration and status, cluster membership, and runtime configuration |
| **Observability** | Metrics and metrics registry |
| **HTTP Interface** | Web server, dashboard, and API |
| **Validation** | Unit tests, integration tests, and benchmark-oriented classes |

**Note:** These entries describe components represented in the repository. Their presence alone does not establish that every feature is fully integrated or production-ready.
  -----------------------------------------------------------------------

## Technology Stack

- **Programming Language:** Java
- **Build Tool:** Apache Maven
- **Distributed Systems:** Replication, Raft consensus, Multi-Raft, distributed coordination
- **Sharding and Partitioning:** Consistent hashing, shard management, shard migration, and rebalancing
- **Storage Technologies:** RocksDB, B+ Tree, LSM-tree-oriented storage, MemTable, SSTables, Write-Ahead Log (WAL), and storage manifests
- **Database Concepts:** Key-value storage, MVCC, snapshot management, and transaction conflict handling
- **Read/Write Optimization:** Asynchronous write batching, ReadIndex, and follower reads
- **Networking:** TCP, Netty-related client/server components, connection handling, and RPC transport
- **Protocols and Serialization:** RESP-style protocol, RPC messaging, message codecs, and Protobuf integration
- **Cluster Management:** Cluster membership, node configuration, node status, and runtime configuration
- **Observability:** Metrics, metrics registry, and HTTP dashboard/API
- **Testing and Benchmarking:** Unit tests, integration tests, concurrency and recovery tests, and benchmark-oriented classes

See [`pom.xml`](pom.xml) for configured dependencies, Java version, and
build settings.

## Build and Test

### Prerequisites

-   A JDK compatible with the Java version configured in `pom.xml`
-   Apache Maven
-   Git

Check installed versions:

``` bash
java -version
mvn -version
git --version
```

### Clone and build

``` bash
git clone https://github.com/rahamanmursalim-create/DistributedKVStore.git
cd DistributedKVStore
mvn clean package
```

### Run tests

``` bash
mvn test
```

If Maven does not discover or execute all test classes, inspect
`pom.xml` and the project's configured test runner. A successful package
alone does not prove that every test ran.

## Run

The repository contains multiple `Main.java` files and several server
components, so the correct launch command depends on the intended entry
point and the JAR configuration.

1.  Inspect `pom.xml` for the configured main class and JAR manifest.
2.  Build with `mvn clean package`.
3.  If an executable JAR is configured, run the generated JAR:

``` bash
java -jar target/<generated-jar-name>.jar
```

Replace the placeholder with the actual generated filename. If no
executable JAR is configured, run the intended main class from your IDE
or configure the Maven packaging plugin.

The codebase contains RESP-related classes, but confirm supported
commands, port configuration, and protocol compatibility before assuming
full Redis compatibility.

## Performance and Reliability

- **Efficient Storage:** WAL, MemTables, SSTables, Bloom filters, and compaction.
- **Optimized Reads/Writes:** Asynchronous write batching, ReadIndex, and follower reads.
- **Data Consistency:** Raft-based replication and MVCC transactions.
- **Scalability:** Consistent hashing and dynamic shard rebalancing.
- **Reliability:** Recovery, persistence, concurrency, and integration tests.

## Project Layout

The tree below is the repository layout provided for this README. Keep
it synchronized with the actual checkout when files change. Generated
runtime data and build output are omitted.

``` text
DistributedKVStore/
├── .gitignore
├── Dockerfile
├── LICENSE
├── README.md
├── pom.xml
├── project-layout.txt
├── test-compile-errors.txt
├── docs/
│   └── images/
│       └── distributedkvstore-architecture.png
└── src/
    ├── main/
    │   └── java/
    │       ├── Main.java
    │       └── com/
    │           └── codecrafters/
    │               └── redis/
    │                   ├── ClientHandler.java
    │                   ├── Networks/
    │                   │   ├── client/
    │                   │   │   ├── KVClient.java
    │                   │   │   ├── KVClientImpl.java
    │                   │   │   ├── NettyClient.java
    │                   │   │   ├── RpcClient.java
    │                   │   │   └── Session.java
    │                   │   ├── handler/
    │                   │   │   ├── ClientHandler.java
    │                   │   │   └── ServerHandler.java
    │                   │   ├── protocol/
    │                   │   │   ├── AppendEntriesRequestMessage.java
    │                   │   │   ├── AppendEntriesResponseMessage.java
    │                   │   │   ├── InstallSnapshotRequestMessage.java
    │                   │   │   ├── InstallSnapshotResponseMessage.java
    │                   │   │   ├── KVMessage.java
    │                   │   │   ├── Message.java
    │                   │   │   ├── RaftMessage.java
    │                   │   │   ├── ReadIndexRequestMessage.java
    │                   │   │   ├── ReadIndexResponseMessage.java
    │                   │   │   ├── Request.java
    │                   │   │   ├── Response.java
    │                   │   │   ├── VoteRequestMessage.java
    │                   │   │   ├── VoteResponseMessage.java
    │                   │   │   └── codec/
    │                   │   │       ├── MessageCodec.java
    │                   │   │       └── ProtobufCodec.java
    │                   │   └── server/
    │                   │       ├── NettyServer.java
    │                   │       └── RpcServer.java
    │                   ├── Server/
    │                   │   ├── ClusterManager.java
    │                   │   ├── KVServer.java
    │                   │   ├── NodeManager.java
    │                   │   └── Server.java
    │                   ├── cluster/
    │                   │   ├── ClusterManager.java
    │                   │   ├── ClusterMembership.java
    │                   │   ├── ClusterRuntimeConfig.java
    │                   │   ├── NodeConfig.java
    │                   │   └── NodeStatus.java
    │                   ├── command/
    │                   │   ├── Command.java
    │                   │   └── CommandHandler.java
    │                   ├── common/
    │                   │   └── util/
    │                   │       ├── ConcurrentHashSet.java
    │                   │       ├── ConsistentHash.java
    │                   │       ├── Metrics.java
    │                   │       ├── MetricsRegistry.java
    │                   │       ├── Result.java
    │                   │       ├── Status.java
    │                   │       └── ThreadPoolExecutor.java
    │                   ├── protocol/
    │                   │   ├── RespReader.java
    │                   │   ├── RespType.java
    │                   │   ├── RespValue.java
    │                   │   └── RespWriter.java
    │                   ├── raft/
    │                   │   ├── AsyncApplier.java
    │                   │   ├── FollowerRead.java
    │                   │   ├── KVStateMachine.java
    │                   │   ├── Log.java
    │                   │   ├── LogEntry.java
    │                   │   ├── MultiRaftStore.java
    │                   │   ├── RaftGroup.java
    │                   │   ├── RaftGroupManager.java
    │                   │   ├── RaftLog.java
    │                   │   ├── RaftLogEntry.java
    │                   │   ├── RaftMessage.java
    │                   │   ├── RaftMetadataStore.java
    │                   │   ├── RaftNode.java
    │                   │   ├── RaftPeer.java
    │                   │   ├── RaftStorage.java
    │                   │   ├── ReadIndex.java
    │                   │   ├── Snapshot.java
    │                   │   ├── StateMachine.java
    │                   │   └── election/
    │                   │       ├── ElectionTimer.java
    │                   │       └── LeaderState.java
    │                   ├── rpc/
    │                   │   ├── RpcClient.java
    │                   │   ├── RpcCodec.java
    │                   │   ├── RpcMessage.java
    │                   │   ├── RpcServer.java
    │                   │   ├── RpcTransportClient.java
    │                   │   └── RpcTransportServer.java
    │                   ├── sharding/
    │                   │   ├── ConsistentHash.java
    │                   │   ├── DataMigrator.java
    │                   │   ├── Shard.java
    │                   │   ├── ShardAdminService.java
    │                   │   ├── ShardManager.java
    │                   │   ├── ShardMetadata.java
    │                   │   └── ShardedStore.java
    │                   ├── storage/
    │                   │   ├── AsyncWriteBatcher.java
    │                   │   ├── BPlusTree.java
    │                   │   ├── BloomFilter.java
    │                   │   ├── KeyValueAccess.java
    │                   │   ├── Main.java
    │                   │   ├── MemTable.java
    │                   │   ├── MVCCTransaction.java
    │                   │   ├── RedisStore.java
    │                   │   ├── RocksDBBPlusTreeStore.java
    │                   │   ├── RocksDBEngine.java
    │                   │   ├── SSTable.java
    │                   │   ├── SSTableManifest.java
    │                   │   ├── StorageEngine.java
    │                   │   ├── StorageManager.java
    │                   │   ├── TransactionConflictException.java
    │                   │   ├── ValueEntry.java
    │                   │   └── WriteAheadLog.java
    │                   └── web/
    │                       └── WebServer.java
    └── test/
        └── java/
            └── com/
                └── codecrafters/
                    └── redis/
                        ├── AllTestsRunner.java
                        ├── RedisServerTest.java
                        ├── TcpRespBenchmark.java
                        ├── client/
                        │   └── KVClientIntegrationTest.java
                        ├── cluster/
                        │   ├── ClusterMembershipTest.java
                        │   └── ClusterRuntimeConfigTest.java
                        ├── raft/
                        │   ├── MultiRaftBenchmark.java
                        │   ├── MultiRaftStoreTest.java
                        │   ├── RaftGroupConsensusTest.java
                        │   ├── RaftLogPersistenceTest.java
                        │   ├── RaftLogTest.java
                        │   └── RaftRebalanceIntegrationTest.java
                        ├── rpc/
                        │   ├── RpcCodecTest.java
                        │   └── RpcTransportTest.java
                        ├── sharding/
                        │   ├── ConsistentHashTest.java
                        │   ├── ShardAdminIntegrationTest.java
                        │   ├── ShardMetadataTest.java
                        │   ├── ShardedCommandRoutingTest.java
                        │   ├── ShardedStoreRebalanceTest.java
                        │   └── ShardedStoreRestartTest.java
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
                        │   ├── StorageEngineCrashRecoveryRunner.java
                        │   ├── StorageEngineOrphanSSTableTest.java
                        │   ├── StorageEngineTest.java
                        │   ├── StorageEngineWalRecoveryTest.java
                        │   └── WriteAheadLogTest.java
                        └── web/
                            └── WebServerIntegrationTest.java
```

## License

The repository contains a [`LICENSE`](LICENSE) file. Read the full terms
before using, modifying, or redistributing this project.

[View the LICENSE file on
GitHub](https://github.com/rahamanmursalim-create/DistributedKVStore/blob/main/LICENSE)
