# DistributedKVStore

A Java 17+ key-value server with a Redis-style RESP-over-TCP interface, an HTTP dashboard/API, and a separately implemented LSM-style storage engine.

- **Live Dashboard:** https://distributedkvstore.onrender.com
- **Health Endpoint:** https://distributedkvstore.onrender.com/api/health
- **GitHub Repository:** https://github.com/rahamanmursalim-create/DistributedKVStore

> **Implementation status:** The default TCP/HTTP server remains single-process. Set the `KV_*` cluster environment variables below to run one or more static Raft groups; consistent hashing routes keys to groups, and TCP/HTTP writes and consistency-sensitive reads use the selected group. Each group runs election timers and heartbeats, persists term/vote/commit state and log entries, and commits writes after majority acknowledgement. Follower requests forward to the known leader. Tests cover automatic election, failover, leader forwarding, ReadIndex, follower reads, multi-group writes, and committed-state recovery. Group membership is static; RocksDB is an optional live persistence backend.

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
- In-memory B+ Tree indexes the live `RedisStore` and the LSM `MemTable`; the LSM memtable also uses deletion tombstones.
- Selectable durable backend: checksummed append-only WAL by default, or RocksDB LSM with synchronous writes and a B+ Tree index via `KV_STORAGE_BACKEND=rocksdb`.
- Snapshot-isolated `RedisStore` transactions with repeatable reads and write-write conflict detection; commits are durable batches in either backend.
- Sorted, immutable SSTable files.
- Manifest-based SSTable tracking and recovery.
- Sparse-index lookups and Bloom filters.
- Bounded LRU record cache.
- Flush, compaction, and recovery workflows.
- Tests covering concurrency, deletion, restart recovery, and interrupted operations.

The transaction API is available to Java callers through `RedisStore.beginTransaction()`; it is not exposed as RESP `MULTI`/`EXEC` commands. The standalone SSTable engine remains separate from the live persistence path.

### Batched asynchronous writes
- The live TCP server submits non-expiring `SET` and `PUT` writes to a bounded asynchronous queue.
- Writes are grouped into WAL or RocksDB batches and forced before their RESP success is returned.
- Shutdown drains accepted writes before closing the store.
- Expiring `SET` commands use the selected backend's synchronous direct write path.

### Distributed-System Learning Components
The repository also contains components and tests related to:
- Raft log handling.
- Optional file-backed Raft log with checksummed, synced appends and restart recovery.
- Active per-group Raft implementation with persisted term/vote/commit metadata, randomized election timers, heartbeats, majority-acknowledged writes, and group-scoped RPC messages.
- ReadIndex quorum fences and opt-in follower reads with a quorum-confirmed commit-index lag limit; these reads are bounded by Raft log entries, not by wall-clock time.
- Optional `Server` constructor wiring for replicated `SET`, `PUT`, expiry writes, `GET`, `DEL`, and `KEYS`.
- Three-node integration coverage for election, command routing, majority writes, failover, reads, and committed-state restart recovery.
- RPC message encoding and validation.
- Length-framed TCP request/reply transport for the active `RpcMessage`/`RpcCodec` types.
- Cluster membership.
- Consistent hashing with virtual nodes and a `ShardedStore` that routes to node-local `RedisStore` instances and rebalances keys on membership changes.
- `KV_STORAGE_BACKEND=rocksdb` selects RocksDB LSM persistence with synchronous writes and TTL metadata for standalone, local-sharded, and Raft-group stores.
- Shard metadata and sharding concepts.
- RocksDB-related storage integration.

`ShardedStore` can be enabled in the live TCP and HTTP request paths with `KV_SHARD_NODES`; it opens one store per configured node ID, routes keys by the virtual-node ring, reconciles persisted ownership on restart, and copies changed ownership before publishing membership updates. These stores still run in the same process, so this is not cross-machine shard placement or transfer. Set `KV_SHARD_ADMIN_TOKEN` to enable authenticated live membership operations: `GET /api/shards` lists members, `POST /api/shards/<id>` adds a store and rebalances, and `DELETE /api/shards/<id>` removes a store after moving its keys. Send `Authorization: Bearer <token>`; the endpoints are disabled when no token is configured. This management endpoint applies to local sharding only and cannot be combined with Raft mode. Multi-Raft startup configures a static set of replicated groups per application process; every group currently uses the same peer membership with a separate Raft port and log. Keys are assigned by a consistent-hash ring. `MultiRaftStore.rebalanceGroups` can move keys between groups already provisioned on that process: it quorum-commits a write freeze, copies values with absolute TTLs through the Raft logs, changes the local routing ring, and removes source copies. It is not exposed through an admin endpoint, its active ring is not persisted, and separate application processes do not coordinate their routing changes. Do not use it as a live cluster-wide membership change while other application processes continue serving writes. Configure the same static `KV_RAFT_GROUPS` on all nodes; dynamic Raft peer membership and snapshot installation are not implemented. Network fault-injection coverage and production operational hardening remain unfinished.

The shard membership manifest also records whether each shard uses the legacy root store or its own `DATA_DIR/shards/<id>` path. Restarting with the manifest preserves the locations selected when a shard was added, even after the original shard is removed.

### Starting with local shards

Configure a comma-separated list of distinct logical store IDs. With the default WAL backend, each gets its own WAL under `DATA_DIR/shards/<node-id>`; the first configured ID uses `DATA_DIR/wal.log` for compatibility with existing standalone data. With RocksDB, stores live under `DATA_DIR/rocksdb` and `DATA_DIR/shards/<node-id>/rocksdb`. Writes are asynchronously batched and grouped by owning store. Membership changes performed through `ShardedStore.addNode` or `removeNode` copy moved values, including absolute TTL expiry, before changing the ring.

```powershell
$env:KV_SHARD_NODES = "shard-a,shard-b,shard-c"
$env:KV_SHARD_VIRTUAL_NODES = "128"
$env:KV_SHARD_ADMIN_TOKEN = "replace-with-a-long-random-secret" # enables live add/remove endpoints
$env:KV_STORAGE_BACKEND = "rocksdb" # optional; defaults to wal
$env:DATA_DIR = "data"
java -jar target/redis-java-clone-1.0.0.jar
```

### Starting a Raft cluster

Set the same complete peer list on every process, with a unique node ID, Raft port, and data directory per process. The address assigned to each node ID must be reachable by every other node. `KV_NODE_ID` unset keeps standalone mode.

```powershell
$env:KV_NODE_ID = "node-a"
$env:KV_RAFT_GROUPS = "shard-a,shard-b,shard-c"
$env:KV_RAFT_BIND_HOST = "0.0.0.0"
$env:KV_RAFT_PORT = "7101"
$env:KV_RAFT_PEERS = "node-a=10.0.0.1:7101,node-b=10.0.0.2:7111,node-c=10.0.0.3:7121"
$env:DATA_DIR = "data-node-a"
$env:KV_STORAGE_BACKEND = "rocksdb" # optional; defaults to wal
$env:TCP_PORT = "6379"
$env:PORT = "8080"
mvn package
java -jar target/redis-java-clone-1.0.0.jar
```

When `KV_RAFT_GROUPS` is set, each group uses its list index as an offset from `KV_RAFT_PORT`; peer ports use the same offset from that peer's base port. Reserve a consecutive port range per node. Without `KV_RAFT_GROUPS`, `KV_RAFT_GROUP_ID` selects one group. A batched request that touches multiple groups commits one entry per group and is not atomic across those groups. `KV_FOLLOWER_READS=true` enables follower reads only after the known leader confirms a ReadIndex with a quorum. `KV_FOLLOWER_MAX_COMMIT_LAG` sets the maximum allowed difference between the follower applied index and that quorum-confirmed commit index (default `0`); reads fail closed when the leader cannot confirm or the follower exceeds this log-index bound. This is not a wall-clock staleness guarantee. Linearizable reads and writes forward to the known group leader and require a quorum; a 3-node group tolerates one unavailable node. TCP and HTTP ports can differ from the Raft range.

## Architecture

### Live TCP Request Path

1. The server accepts a TCP connection.
2. `ClientHandler` reads RESP messages through `RespReader`.
3. `Command` and `CommandHandler` parse and execute commands.
4. `RespWriter` serializes responses.
5. In standalone mode, non-expiring `SET` and `PUT` pass through the bounded batcher; in sharded mode each batch is grouped by its destination store. When a `RaftGroup` is supplied, supported writes and consistency-sensitive reads use the replicated group instead.

### HTTP Request Path

1. `WebServer` serves the dashboard and REST API.
2. HTTP handlers access the local store in standalone mode, the shard router in sharded mode, or the Raft group in cluster mode.
3. The API returns JSON responses and HTTP status codes.

### Persistence Paths

**Live server:** `RedisStore` maintains a B+ Tree index and persists mutations through the checksummed WAL by default, or through RocksDB's LSM when `KV_STORAGE_BACKEND=rocksdb` is selected.

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

The latest verified Maven run compiled **44 production sources** and **41 test sources**. Surefire's own test goal is skipped by configuration; the Maven-bound custom `AllTestsRunner` discovered and ran **36 test classes**, with **36 passed and 0 failed**. Coverage includes storage and RocksDB restart recovery, local sharding, multi-node Raft consensus and rebalance, RESP/TCP behavior, and HTTP integration. These are local functional tests, not performance or production-readiness evidence.

Coverage includes:
- RESP parsing, serialization, and server behavior.
- Fragmented TCP input and concurrent clients.
- WAL replay, checksum validation, and recovery.
- SSTable lookups, sparse indexes, Bloom filters, and LRU caching.
- Manifest validation, compaction, and orphan SSTables.
- Concurrent storage operations and deletion recovery.
- Batched write persistence across restart and live concurrent TCP writes.
- HTTP dashboard and REST API integration.
- Raft log, RPC codec, cluster membership, and shard metadata unit tests.
- Sharded-store migration coverage for node addition/removal, ownership changes, and value preservation.
- RocksDB/B+ Tree batch-write and restart recovery coverage.

Run the tests again to verify the current checkout:

```powershell
mvn clean test
```

**Testing caveat:** Surefire is configured to skip its own runner; Maven then executes the project's custom `AllTestsRunner`. The runner includes Maven dependency jars in its forked test classpath. This is local functional coverage, not a performance, fault-injection, or production availability benchmark.

## Performance

Benchmark harnesses are available for the `MultiRaftStore` API and the persistent TCP/RESP server. No performance figures are asserted here; run the commands below against the current checkout to collect measurements. These local benchmarks do not establish production capacity or availability.

Reproduce after compiling tests with:

```powershell
mvn test-compile
mvn exec:java -Dexec.mainClass=com.codecrafters.redis.raft.MultiRaftBenchmark -Dexec.classpathScope=test
```

The write batcher settings can be tuned with JVM properties: `redis.write.queue.capacity` (default `4096`), `redis.write.batch.maxSize` (default `128`), and `redis.write.batch.maxDelayMillis` (default `2`). Reproduce the TCP benchmark after `mvn test-compile`:

```powershell
mvn exec:java -Dexec.mainClass=com.codecrafters.redis.TcpRespBenchmark -Dexec.classpathScope=test -Dbench.clients=32 -Dbench.warmup=2000 -Dbench.operations=5000
```

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
  -e KV_STORAGE_BACKEND=rocksdb \
  -v "${PWD}/data:/var/data" \
  distributedkvstore
```

Ensure Docker Desktop is running. The volume preserves the live server's WAL by default; set `KV_STORAGE_BACKEND=rocksdb` to persist through RocksDB instead.

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
| `DATA_DIR` | `./data` in the application | Root directory for the selected live storage backend |
| `KV_STORAGE_BACKEND` | `wal` | Live persistence backend: `wal` or `rocksdb` |
| `KV_SHARD_NODES` | unset | Enables local sharding with comma-separated node IDs |
| `KV_SHARD_VIRTUAL_NODES` | `128` | Virtual nodes per local shard |
| `KV_SHARD_ADMIN_TOKEN` | unset | Enables bearer-token protected live local shard membership operations |

**Hosting limitations:**
- Render free services have an ephemeral filesystem and do not provide persistent disks.
- A public HTTP URL does not automatically expose the RESP TCP port.
- The standalone SSTable `StorageEngine` remains separate from the live request path.
- The first request after inactivity may be delayed.

Review Render's current plan and storage limitations before deploying anything that holds important data.

## Security and Persistence Caveats

- The public demo's write/delete endpoints do not provide authentication.
- Do not store credentials, API keys, or sensitive personal data in the public demo.
- Add authentication, authorization, rate limiting, stricter validation, monitoring, and backups before production use.
- The public demo deployment is single-instance. Static Raft replication and failover have local multi-node integration coverage, but that is not production availability evidence.
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
