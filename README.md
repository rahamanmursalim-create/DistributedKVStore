# Distributed Key-Value Store

A Java-based key-value storage project exploring persistent storage,
Redis-style request handling, replication, sharding, and distributed
coordination. The repository contains storage-engine components,
network/protocol code, Raft-related components, shard management, a web
API, and automated tests.

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

DistributedKVStore is a Java project for exploring the design of a
persistent key-value database and related distributed-system components.
Its code is organized around storage, client/server networking, protocol
handling, replication/consensus, sharding, cluster management, and a web
interface.

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

  -----------------------------------------------------------------------
  Area                                Components represented in the
                                      repository
  ----------------------------------- -----------------------------------
  Persistent storage                  Write-ahead log (WAL), memtable,
                                      SSTables, manifest, Bloom filter,
                                      compaction, storage manager

  Key-value operations                Redis-style store, value/expiry
                                      state, key-value access

  Transactions                        MVCC transaction-related code and
                                      conflict exception

  Networking                          TCP/Netty client and server
                                      classes, connection handlers

  Protocols                           RESP reader/writer/value/type
                                      classes, RPC transport and message
                                      codecs

  Replication/consensus               Raft nodes/logs, snapshots,
                                      read-index and multi-Raft classes

  Sharding                            Consistent hashing, shard
                                      metadata/manager, migration and
                                      rebalance components

  Cluster management                  Node configuration/status,
                                      membership and runtime
                                      configuration

  Observability                       Metrics and metrics registry

  HTTP interface                      Web server/dashboard API

  Validation                          Unit/integration tests and
                                      benchmark-oriented classes
  -----------------------------------------------------------------------

## Technology Stack

-   **Language:** Java
-   **Build:** Apache Maven
-   **Storage:** custom storage classes and RocksDB integration classes
-   **Networking/protocol:** TCP/Netty-related code, RESP-style protocol
    classes, RPC/message codecs
-   **Testing:** test classes under `src/test/java`

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
