# DistributedKVStore

A Java 17+ key-value server with a Redis-style RESP-over-TCP interface, an HTTP dashboard/API, and a separately implemented LSM-style storage engine.

- **Live Dashboard:** https://distributedkvstore.onrender.com
- **Health Endpoint:** https://distributedkvstore.onrender.com/api/health
- **GitHub Repository:** https://github.com/rahamanmursalim-create/DistributedKVStore

> **Implementation status:** The current live server is single-process. The TCP server and HTTP API share `RedisStore`, an in-memory map backed by a Write-Ahead Log (WAL). The standalone `StorageEngine` implements LSM-style storage components. Although the repository contains Raft, RPC, cluster-membership, and sharding-related code and tests, a fully operational multi-node distributed cluster with replication, consensus, and automatic failover has not yet been verified.

## Features

### TCP Server and RESP Protocol
- Concurrent TCP client handling.
- RESP request parsing and response serialization.
- Support for fragmented input and pipelined requests.
- Redis-style commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, and `EXISTS`.
- Key expiration using `EX` and `PX`.
- Additional commands including `PUT`, `KEYS`, `INFO`, `CONFIG`, and `COMMAND`.

### HTTP Dashboard and REST API
- Embedded web dashboard.
- JSON health and key-list endpoints.
- Create, read, update, and delete operations.
- Search/filtering, loading feedback, toast notifications, and delete confirmation.
- No external frontend framework or CDN required by the embedded dashboard.

### Storage Engine
- In-memory `MemTable` with deletion tombstones.
- Append-only Write-Ahead Log with checksums and recovery.
- Sorted, immutable SSTable files.
- Manifest-based SSTable tracking and recovery.
- Sparse-index lookups and Bloom filters.
- Bounded LRU record cache.
- Flush, compaction, and recovery workflows.
- Tests covering concurrency, deletion, restart recovery, and interrupted operations.

### Distributed-System Learning Components
The repository also contains components and tests related to:
- Raft log handling.
- RPC message encoding and validation.
- Cluster membership.
- Shard metadata and sharding concepts.
- RocksDB-related storage integration.

Passing unit tests for these components does not establish that multi-node consensus, replication, or dynamic shard rebalancing works end to end.

## Architecture

### Live TCP Request Path

1. The server accepts a TCP connection.
2. `ClientHandler` reads RESP messages through `RespReader`.
3. `Command` and `CommandHandler` parse and execute commands.
4. `RespWriter` serializes responses.
5. Commands read or update the shared `RedisStore`.

### HTTP Request Path

1. `WebServer` serves the dashboard and REST API.
2. HTTP handlers read or update the same `RedisStore` used by the TCP server.
3. The API returns JSON responses and HTTP status codes.

### Persistence Paths

**Live server:** `RedisStore` maintains values in memory and records mutations in its WAL.

**Standalone storage engine:** `StorageEngine` uses a `MemTable`, WAL files, SSTables, and a manifest to implement storage, recovery, flush, and compaction operations.

These are currently separate storage paths. The live TCP/HTTP request path does not yet use the standalone `StorageEngine`.

## Implemented TCP Commands

| Command | Purpose |
|---|---|
| `PING` | Check connectivity |
| `ECHO <message>` | Return a message |
| `SET <key> <value>` | Create or replace a value |
| `SET <key> <value> EX <seconds>` | Set a value with expiration in seconds |
| `SET <key> <value> PX <milliseconds>` | Set a value with expiration in milliseconds |
| `PUT <key> <value>` | Custom write command |
| `GET <key>` | Read a value |
| `DEL <key> [key ...]` | Delete one or more keys |
| `EXISTS <key>` | Check whether a key exists |
| `KEYS [pattern]` | List matching keys |
| `COMMAND` | Limited command metadata response |
| `INFO` | Limited server information |
| `CONFIG` | Limited configuration response |

This is a Redis-style command subset, not a complete Redis replacement.

## HTTP API

| Method | Endpoint | Behavior |
|---|---|---|
| `GET` | `/` | Serve the dashboard |
| `GET` | `/api/health` | Return service status and key count |
| `GET` | `/api/keys` | List keys |
| `GET` | `/api/keys/{key}` | Read a key |
| `PUT` | `/api/keys/{key}` | Create or replace a key |
| `DELETE` | `/api/keys/{key}` | Delete a key |

Example health response:

```json
{"status":"ok","keys":0}
```

For the `PUT` endpoint, the request body is stored as text.

## Technology Stack

- **Language:** Java 17+
- **Build:** Apache Maven
- **Networking:** Java TCP sockets and `HttpServer`
- **Protocol:** RESP-compatible subset
- **Persistence:** WAL-backed in-memory map for the live server
- **Storage-engine components:** MemTable, WAL, SSTables, manifest, Bloom filter, sparse index, LRU cache
- **Distributed-system learning components:** Raft-related code, RPC codec, cluster membership, sharding metadata
- **Packaging:** Docker
- **Hosting:** Render

## Run Locally

### Prerequisites
- JDK 17 or later
- Apache Maven 3.8+
- Git
- Docker Desktop (optional)

### Build and Test

Run from the repository root:

```bash
mvn clean test
```

Build the JAR:

```bash
mvn clean package
```

Expected artifact:

```text
target/redis-java-clone-1.0.0.jar
```

Start the server:

```bash
java -jar target/redis-java-clone-1.0.0.jar
```

Default ports:
- HTTP dashboard/API: `8080`
- RESP TCP server: `6379`

The `PORT` and `TCP_PORT` environment variables can override these values.

### Windows PowerShell

If Maven is not on your PATH, configure it first:

```powershell
$env:MAVEN_HOME = "$env:USERPROFILE\Downloads\apache-maven-3.10.0-bin\apache-maven-3.10.0"
$env:Path = "$env:MAVEN_HOME\bin;$env:Path"
```

Then run:

```powershell
mvn clean test
mvn clean package
java -jar .\target\redis-java-clone-1.0.0.jar
```

Open http://localhost:8080 to access the dashboard.

## Testing

The latest reported local run completed with **18 test groups passed and 0 failed** using the project's custom `AllTestsRunner`.

Coverage includes:
- RESP parsing, serialization, and server behavior.
- Fragmented TCP input and concurrent clients.
- WAL replay, checksum validation, and recovery.
- SSTable lookups, sparse indexes, Bloom filters, and LRU caching.
- Manifest validation, compaction, and orphan SSTables.
- Concurrent storage operations and deletion recovery.
- HTTP dashboard and REST API integration.
- Raft log, RPC codec, cluster membership, and shard metadata unit tests.

Run the tests again to verify the current checkout:

```powershell
mvn clean test
```

**Testing caveat:** The latest Maven output reported `Tests are skipped` in Surefire before the custom runner executed the test groups. The custom runner passed, but standard Maven test reporting should be reviewed and corrected. Unit tests for Raft-related classes do not prove multi-node consensus or replication.

## Project Layout

```text
## Project Layout

DistributedKVStore/
├── .gitignore
├── Dockerfile
├── LICENSE
├── pom.xml
├── README.md
├── docs/
│   └── images/
│       └── distributedkvstore-architecture.png
└── src/
    ├── main/
    │   └── java/
    │       ├── Main.java
    │       └── com/codecrafters/redis/
    │           ├── Server.java
    │           ├── ClientHandler.java
    │           ├── command/
    │           │   ├── Command.java
    │           │   └── CommandHandler.java
    │           ├── protocol/
    │           │   ├── RespReader.java
    │           │   ├── RespType.java
    │           │   ├── RespValue.java
    │           │   └── RespWriter.java
    │           ├── cluster/
    │           │   ├── ClusterManager.java
    │           │   ├── NodeConfig.java
    │           │   ├── NodeStatus.java
    │           │   └── ClusterMembership.java
    │           ├── rpc/
    │           │   ├── RpcServer.java
    │           │   ├── RpcClient.java
    │           │   ├── RpcMessage.java
    │           │   └── RpcCodec.java
    │           ├── sharding/
    │           │   ├── Shard.java
    │           │   ├── ShardManager.java
    │           │   ├── ConsistentHash.java
    │           │   ├── DataMigrator.java
    │           │   └── ShardMetadata.java
    │           ├── raft/
    │           │   ├── RaftNode.java
    │           │   ├── RaftGroup.java
    │           │   ├── RaftGroupManager.java
    │           │   ├── RaftLog.java
    │           │   ├── RaftLogEntry.java
    │           │   ├── RaftStorage.java
    │           │   ├── RaftMessage.java
    │           │   └── Snapshot.java
    │           ├── storage/
    │           │   ├── BloomFilter.java
    │           │   ├── MemTable.java
    │           │   ├── RedisStore.java
    │           │   ├── RocksDBEngine.java
    │           │   ├── StorageEngine.java
    │           │   ├── StorageManager.java
    │           │   ├── SSTable.java
    │           │   ├── SSTableManifest.java
    │           │   ├── ValueEntry.java
    │           │   └── WriteAheadLog.java
    │           └── web/
    │               └── WebServer.java
    └── test/
        └── java/
            └── com/codecrafters/redis/
                ├── AllTestsRunner.java
                ├── RedisServerTest.java
                ├── cluster/
                │   └── ClusterMembershipTest.java
                ├── raft/
                │   └── RaftLogTest.java
                ├── rpc/
                │   └── RpcCodecTest.java
                ├── sharding/
                │   └── ShardMetadataTest.java
                ├── web/
                │   └── WebServerIntegrationTest.java
                └── storage/
                    ├── BloomFilterTest.java
                    ├── RedisStoreRecoveryTest.java
                    ├── SSTableBenchmark.java
                    ├── SSTableManifestTest.java
                    ├── SSTableTest.java
                    ├── StorageEngineCompactionTest.java
                    ├── StorageEngineCompactionWalTest.java
                    ├── StorageEngineConcurrencyTest.java
                    ├── StorageEngineConcurrentDeleteTest.java
                    ├── StorageEngineCrashRecoveryRunner.java
                    ├── StorageEngineOrphanSSTableTest.java
                    ├── StorageEngineTest.java
                    ├── StorageEngineWalRecoveryTest.java
                    └── WriteAheadLogTest.java
```

The tree above includes the production files and test files identified in the current project information. Verify it against the actual repository before treating it as an exhaustive file listing.

## Docker

Build the image:

```bash
docker build -t distributedkvstore .
```

Run it:

```bash
docker run --rm \
  -p 8080:8080 \
  -p 6379:6379 \
  -e PORT=8080 \
  -e TCP_PORT=6379 \
  -e DATA_DIR=/var/data \
  -v "${PWD}/data:/var/data" \
  distributedkvstore
```

Ensure Docker Desktop is running. The volume preserves the live server's WAL in the local `data` directory.

## Deploy to Render

1. Push the latest code to GitHub.
2. Create a Render Web Service and select this repository.
3. Select Docker as the deployment environment.
4. Set the Dockerfile path to `./Dockerfile`.
5. Leave the root directory blank.
6. Deploy and inspect the build/runtime logs.
7. Test `/api/health` and the key CRUD endpoints.

### Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP dashboard/API port |
| `TCP_PORT` | `6379` | RESP TCP port |
| `DATA_DIR` | `./data` in the application | Directory for the live server's WAL |

**Hosting limitations:**
- Render free services have an ephemeral filesystem and do not provide persistent disks.
- A public HTTP URL does not automatically expose the RESP TCP port.
- The `DATA_DIR` setting applies to the live server's WAL; it does not connect the separate `StorageEngine` to the live request path.
- The first request after inactivity may be delayed.

Review Render's current plan and storage limitations before deploying anything that holds important data.

## Security and Persistence Caveats

- The public demo's write/delete endpoints do not provide authentication.
- Do not store credentials, API keys, or sensitive personal data in the public demo.
- Add authentication, authorization, rate limiting, stricter validation, monitoring, and backups before production use.
- The current architecture is single-instance and does not provide verified distributed replication or automatic failover.
- Do not use the free deployment as the only copy of important data.

## Roadmap

- Integrate the standalone `StorageEngine` into the live server's request path.
- Correct and verify Maven/Surefire test reporting.
- Add automated CI for builds, tests, and Docker image validation.
- Add authentication, authorization, and request limits.
- Document persistence, backup, and restore procedures.
- Add end-to-end multi-node testing before claiming replication or consensus.
- Implement and verify replication, leader election, partitioning, and failover if extending the project into a distributed database.
- Add metrics and structured operational logging.

## License

See `LICENSE` for the license and copyright notice included with this repository.
