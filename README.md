# DistributedKVStore — Distributed Redis-Compatible Server & LSM Storage Engine

A Java-based Redis-compatible server and persistent LSM-tree storage engine with a distributed replication and key-routing layer. It implements the Redis Serialization Protocol (RESP), virtual-thread concurrent TCP client handling, key expiration (TTL), an LSM storage engine (Write-Ahead Log with CRC32 checksums, MemTable, immutable SSTables with sparse indexing, Bloom filter, and bounded LRU caching, manifest-based crash recovery, and multi-table compaction), and a distributed layer featuring consistent hashing and primary-replica TCP replication.

Built with **Java 17+** and the Java Standard Library — **zero external dependencies** at runtime.

---

## Architecture Overview

```text
               +---------------------------------------+
               |         Redis TCP Clients             |
               +---------------------------------------+
                                  | RESP (port 6379)
                                  v
+----------------------------------------------------------------------+
|  Primary Node (Server)                                               |
|                                                                      |
|  [RESP Reader / Writer] <---> [ClientHandler (Virtual Threads)]     |
|                                        |                             |
|                                        v                             |
|                             [CommandHandler]                         |
|                          (ReplicationMode: SYNC/ASYNC)               |
|                                /          \                          |
|                               v            v                         |
|                    [StorageEngine]     [ReplicationClient]           |
|                    /             \              |                    |
|                   v               v             | Binary TCP         |
|            [MemTable]           [WAL]           | Protocol           |
|                | (Flush)          |             v                    |
|                v                  |   +--------------------------+   |
|           [SSTables] <------------+   | Replica (ReplicationServer) |
|        (Sparse Index,                 |  StorageEngine + WAL     |   |
|         Bloom Filter,                 +--------------------------+   |
|         LRU Cache)                                                   |
+----------------------------------------------------------------------+

                      +--------------------------+
                      | Distributed Routing      |
                      |  - ConsistentHashRing    |
                      |  - DistributedNodeReg    |
                      |  - DistributedKeyRouter  |
                      |  - ReplicationManager    |
                      +--------------------------+
```

---

## Features

### 1. Redis Server and RESP Protocol
- **High-concurrency TCP server:** Java virtual threads (`newVirtualThreadPerTaskExecutor`) with graceful fallback to cached thread pool.
- **Protocol coverage:** RESP serialization and parsing for Simple Strings (`+`), Errors (`-`), Integers (`:`), Bulk Strings (`$`), Null Bulk Strings (`$-1`), and Arrays (`*`).
- **Command pipelining:** Full support for TCP stream fragmentation, multi-command pipelining, and persistent keep-alive connections.
- **Supported commands:** `PING` (with optional message), `ECHO`, `SET` (with `PX`/`EX` TTL options), `PUT`, `GET`, `DEL`/`DELETE`, `EXISTS`, `KEYS` (glob pattern matching), `INFO`, `CONFIG GET`, `COMMAND`.

### 2. Persistent LSM-Tree Storage Engine
- **MemTable:** Thread-safe sorted TreeMap maintaining live entries and deletion tombstones, with live byte size estimation.
- **Write-Ahead Log (WAL):** Sequential append-only log with CRC32 checksums (`PUT2`/`DEL2` record format) for corruption detection, atomic `fsync` on every write, backward compatibility with legacy records, and partial torn-tail truncation recovery on restart.
- **Immutable SSTables** with three read acceleration layers:
  - **Sparse Index:** Samples byte offsets every 128 keys for O(log n) disk seek resolution.
  - **Bloom Filter:** Eliminates unnecessary disk reads on key misses.
  - **LRU Cache:** Access-ordered bounded cache (default 1,000 entries) caching both hits and negative misses.
- **TTL Persistence:** Absolute expiry timestamps propagate through all storage levels: MemTable, WAL, SSTables (encoded as `T:expiry:value`), compaction pruning, and restart recovery.
- **Manifest-based State Tracking:** Atomic `MANIFEST` file listing active SSTable IDs, ensuring crash-safe recovery and ignoring unreferenced orphan SSTables.
- **Compaction Strategies:**
  - `compact()` — merges all active SSTables into one.
  - `compactBatch(int maxTables)` — compacts the `k` most recent SSTables into one, preserving older tables.
  - `sizeTieredCompact(int threshold, int maxTables)` — triggers `compactBatch` only when the table count meets the threshold.

### 3. Distributed Layer & Replication
- **Consistent Hashing (`ConsistentHashRing`):** SHA-256 virtual nodes (configurable count, e.g., 100-128 per physical node) for balanced key distribution across dynamic node sets.
- **Clockwise Ring Failover (`DistributedKeyRouter`):** Routes keys to designated nodes; falls back clockwise along the preference list if the primary node is inactive.
- **TCP Replication Protocol (`ReplicationServer` / `ReplicationClient`):** Binary frame protocol with opcodes:
  - `0x01` PUT (key + value + expiry TTL)
  - `0x02` DELETE
  - `0x03` GET (returns value or `null` for absent/expired/deleted keys)
- **Replication Modes (`ReplicationMode`):**
  - `SYNC` — client write blocks until the replica acknowledges the frame.
  - `ASYNC` — client write is immediately acknowledged; frames are queued in a `LinkedBlockingQueue` and delivered by a background daemon worker.
  - **Lag tracking:** `getPendingReplicationCount()` exposes queue depth. `flushReplicationQueue(timeoutMillis)` synchronously waits for all queued frames to be acknowledged.
- **Retry & Timeout Semantics:** Configurable retries with exponential backoff, 3,000 ms connect timeout, 5,000 ms read timeout.
- **Multi-Node Orchestration (`ReplicationManager`):** Quorum replication factor across both local `StorageEngine` endpoints and remote TCP `ReplicationClient` endpoints.

---

## Requirements

- **JDK 17 or later** (code compiled with `--release 17`; tested on JDK 27)
- **Apache Maven 3.6+** for Maven build commands (optional — direct `javac` commands also work)
- **PowerShell** (Windows) for the test runner script
- No runtime dependencies beyond the JDK

---

## Project Structure

```text
DistributedKVStore/
├── pom.xml                        Maven build + test configuration
├── scripts/
│   └── run-tests.ps1              PowerShell test runner (auto-discovered)
├── src/
│   ├── main/java/
│   │   ├── Main.java              Entry point — starts standalone server on :6379
│   │   └── com/codecrafters/redis/
│   │       ├── Server.java        TCP accept loop, lifecycle, shutdown
│   │       ├── ClientHandler.java Per-connection RESP read/write loop
│   │       ├── ReplicationMode.java  SYNC / ASYNC enum
│   │       ├── PrimaryMain.java   Starts primary + replica replication
│   │       ├── ReplicaMain.java   Starts ReplicationServer (binary protocol receiver)
│   │       ├── VerifyReplica.java Quick replica data check
│   │       ├── command/
│   │       │   ├── Command.java
│   │       │   └── CommandHandler.java  Command dispatch + replication
│   │       ├── distributed/
│   │       │   ├── ConsistentHashRing.java
│   │       │   ├── StorageNode.java
│   │       │   ├── DistributedNodeRegistry.java
│   │       │   ├── DistributedKeyRouter.java
│   │       │   ├── ReplicationClient.java
│   │       │   ├── ReplicationServer.java
│   │       │   └── ReplicationManager.java
│   │       ├── protocol/
│   │       │   ├── RespReader.java
│   │       │   ├── RespType.java
│   │       │   ├── RespValue.java
│   │       │   └── RespWriter.java
│   │       └── storage/
│   │           ├── StorageEngine.java
│   │           ├── MemTable.java
│   │           ├── SSTable.java
│   │           ├── SSTableManifest.java
│   │           ├── WriteAheadLog.java
│   │           ├── BloomFilter.java
│   │           ├── StorageRecord.java
│   │           ├── ValueEntry.java
│   │           └── RedisStore.java
│   └── test/java/                 31 test classes (plain main() runners)
└── README.md
```

---

## Build & Test

### Using Maven (recommended)

Maven 3.6+ must be on `PATH`. If Maven is in a non-standard location (e.g., `C:\Users\mursa\tools\apache-maven-3.10.0\bin`), add it to your `PATH` first:

```powershell
# Windows — add Maven to PATH for this session
$env:PATH = "C:\Users\mursa\tools\apache-maven-3.10.0\bin;" + $env:PATH
```

**Compile, run all tests, and report pass/fail:**
```powershell
mvn clean test
```

**Build the executable JAR (includes tests):**
```powershell
mvn clean package
# JAR: target/redis-java-clone-1.0.0.jar
```

**Build JAR without running tests:**
```powershell
mvn clean package -DskipTests
```

### Running Tests Directly (without Maven)

Compile all sources to `out/`:
```powershell
javac --release 17 -d out (Get-ChildItem .\src -Recurse -Filter *.java | ForEach-Object { $_.FullName })
```

Run any individual test class (assertions must be enabled with `-ea`):
```powershell
java -ea -cp out com.codecrafters.redis.storage.StorageEngineTest
java -ea -cp out com.codecrafters.redis.distributed.MultiNodeIntegrationTest
# ... etc.
```

Run the full suite via the PowerShell script (compiles and runs all 31 test classes):
```powershell
powershell -ExecutionPolicy Bypass -File scripts\run-tests.ps1
```

---

## Starting the Server

### Standalone Mode (single node, port 6379)

```powershell
# Using Maven-built JAR
java -jar target/redis-java-clone-1.0.0.jar

# Or directly from compiled classes
java -cp out Main
```

The server binds to `0.0.0.0:6379` and stores data in `data/storage-engine/`.

### Connect with redis-cli

```bash
redis-cli -p 6379 PING
# +PONG

redis-cli -p 6379 SET greeting "hello world"
# +OK

redis-cli -p 6379 GET greeting
# "hello world"

redis-cli -p 6379 SET temp "expires soon" EX 30
# +OK  (expires in 30 seconds)

redis-cli -p 6379 EXISTS greeting
# (integer) 1

redis-cli -p 6379 KEYS "g*"
# 1) "greeting"

redis-cli -p 6379 DEL greeting
# (integer) 1
```

### Persistence & Restart Recovery

All writes are first flushed to the Write-Ahead Log (WAL) before being acknowledged. Data survives a server restart automatically:

```powershell
# Start server, write a key
java -jar target/redis-java-clone-1.0.0.jar &
redis-cli -p 6379 SET persistent "survives restart"

# Stop server (Ctrl+C), restart
java -jar target/redis-java-clone-1.0.0.jar &

# Data is recovered
redis-cli -p 6379 GET persistent
# "survives restart"
```

On startup, the engine:
1. Reads the `MANIFEST` to load active SSTables.
2. Replays any WAL file not yet flushed to an SSTable.
3. Any partial (torn) WAL tail from a crash is safely truncated.

---

## Multi-Node Setup (Primary + Replica)

### Step 1 — Start the Replica (port 6380)

```powershell
java -cp out com.codecrafters.redis.ReplicaMain 6380
```

The replica runs a `ReplicationServer` on port 6380 and stores data in `data/replica-6380/`.

### Step 2 — Start the Primary (port 6379)

```powershell
java -cp out com.codecrafters.redis.PrimaryMain
```

The primary runs a full Redis server on port 6379, stores data in `data/primary/`, and forwards all `SET`/`DEL` mutations over TCP to `127.0.0.1:6380`.

### Step 3 — Verify Replication

```bash
redis-cli -p 6379 SET replicated "hello from primary"
```

```powershell
java -cp out com.codecrafters.redis.VerifyReplica
```

### Node Failure and Reactivation

`StorageNode.setActive(false)` marks a node inactive. `DistributedKeyRouter.routeKey()` automatically falls back clockwise along the consistent hash ring to the next active node. When the node is reactivated, routing resumes normally.

### Async Replication Mode

To use asynchronous replication (writes acknowledged before replica confirms):

```java
Server server = new Server(6379, Path.of("data/primary"),
    "127.0.0.1", 6380, ReplicationMode.ASYNC);
```

Monitor pending frames:
```java
server.getCommandHandler().getPendingReplicationCount(); // queue depth
server.getCommandHandler().flushReplicationQueue(5000);  // drain with 5s timeout
```

---

## Replication and Consistency Guarantees

- **Write Ordering:** Mutations (`SET`, `DEL`) are committed to the primary's WAL and MemTable before being replicated.
- **SYNC failure semantics:** If replication fails after retries, the client receives `-ERR storage or replication operation failed`. The primary's write is durable; the replica's state is behind.
- **ASYNC failure semantics:** Failures are logged to stderr; the queue continues processing subsequent operations.
- **Ring failover:** `DistributedKeyRouter` navigates clockwise along the preference list to the next active node when a node is inactive.
- **Independent durability:** Both primary and replica maintain their own WAL + SSTables. Restart on either side replays all acknowledged data from disk.

---

## Architectural Enhancements

1. **Remote Cross-Node TCP Queries:** Opcode `0x03` (GET) in the replication protocol enables `ReplicationClient.replicateGet(key)` and `ReplicationManager.readFromNode(nodeId, key)` for cross-process distributed reads with connection timeouts and retry backoff.
2. **Sync & Async Replication with Lag Tracking:** `ReplicationMode.SYNC` (blocking) and `ReplicationMode.ASYNC` (background `LinkedBlockingQueue` + daemon worker), with `getPendingReplicationCount()` and `flushReplicationQueue()` for backpressure control.
3. **Size-Tiered Batch Compaction (STCS):** `compactBatch(int k)` compacts the `k` most recent SSTables into one while preserving older tables; `sizeTieredCompact(int threshold, int k)` triggers compaction automatically when the table count reaches `threshold`. Full `compact()` delegates to `compactBatch` for backward compatibility.

---

## Limitations

- **No full Raft/consensus:** Distributed consistency is primary-replica replication, not a consensus protocol. Split-brain is possible if the primary fails mid-replication.
- **No cluster-mode RESP3:** The TCP server implements RESP2 only.
- **Single-writer compaction:** `compact()` and `compactBatch()` hold the `StorageEngine` lock for their duration; large SSTable sets will stall concurrent reads/writes temporarily.
- **Replication is loopback-only by default:** `ReplicationServer` binds to `InetAddress.getLoopbackAddress()`. Cross-host replication requires changing the bind address in `ReplicationServer`.

---

## Repository

GitHub: https://github.com/rahamanmursalim-create/DistributedKVStore
