# DistributedKVStore — Java Redis-Compatible Server & Storage Engine

A Java-based Redis-compatible server and educational persistent storage engine. The project implements the Redis Serialization Protocol (RESP), concurrent TCP client handling, key expiration, and an LSM-style storage engine with a Write-Ahead Log (WAL), MemTable, SSTables, manifest-based recovery, and compaction.

Built with Java 17+ and the Java Standard Library.

## Features

### 1. Redis Server and RESP Protocol

- TCP server with concurrent client handling.
- `PING` command with optional argument support.
- `ECHO` command.
- `SET` and `GET` commands.
- Key expiration using `PX` and `EX`.
- Passive and background active expiration in the in-memory store.
- RESP parsing and serialization for arrays, bulk strings, simple strings, integers, and errors.
- Support for fragmented TCP input and pipelined commands.
- Handling of concurrent clients.

### 2. Persistent Storage Engine

The project also contains a storage engine implemented in Java.

- **MemTable:** Maintains sorted in-memory entries and deletion tombstones.
- **Write-Ahead Log (WAL):** Records PUT and DELETE operations and supports replay during recovery.
- **SSTables:** Stores sorted-table data on disk.
- **Manifest:** Tracks active SSTable IDs for storage-engine recovery.
- **Flush:** Writes MemTable contents to an SSTable and updates the manifest.
- **Compaction:** Merges active SSTables and retains the latest values and deletion state.
- **Restart recovery:** Recovers persisted state from active SSTables and replays eligible WAL records.
- **Orphan SSTable handling:** Avoids treating unreferenced SSTables as active storage tables.

**Important:** The storage engine is a separate component from the in-memory Redis store. The current storage-engine API does not itself implement TTL-aware persistence. This project does not claim distributed replication or consensus.

## Technology Stack

- Java 17+
- Java Standard Library
- TCP sockets and Java concurrency utilities
- RESP protocol
- File-based persistence using WAL and SSTables
- Git and GitHub

## Project Structure

```text
DistributedKVStore/
├── src/
│   ├── main/
│   │   └── java/
│   │       ├── Main.java
│   │       └── com/codecrafters/redis/
│   │           ├── Server.java
│   │           ├── ClientHandler.java
│   │           ├── command/
│   │           │   ├── Command.java
│   │           │   └── CommandHandler.java
│   │           ├── protocol/
│   │           │   ├── RespReader.java
│   │           │   ├── RespType.java
│   │           │   ├── RespValue.java
│   │           │   └── RespWriter.java
│   │           └── storage/
│   │               ├── MemTable.java
│   │               ├── RedisStore.java
│   │               ├── SSTable.java
│   │               ├── SSTableManifest.java
│   │               ├── StorageEngine.java
│   │               ├── ValueEntry.java
│   │               └── WriteAheadLog.java
│   └── test/
│       └── java/
│           └── com/codecrafters/redis/
│               ├── RedisServerTest.java
│               └── storage/
│                   └── WriteAheadLogTest.java
├── README.md
├── pom.xml
└── spawn_redis_server.sh
```

The storage-engine-specific test classes are currently located under `src/main/java/com/codecrafters/redis/storage/`:

- `StorageEngineTest.java`
- `StorageEngineCompactionTest.java`
- `StorageEngineOrphanSSTableTest.java`
- `StorageEngineWalRecoveryTest.java`

## Build and Run

### Prerequisites

- JDK 17 or later
- PowerShell on Windows, or a compatible terminal on Linux/macOS

### Compile all Java sources

Run from the project root in Windows PowerShell:

```powershell
javac --release 17 -d out (Get-ChildItem .\src -Recurse -Filter *.java | ForEach-Object { $_.FullName })
```

If compilation succeeds, the command produces no output.

### Run the Redis Server Test Suite

```powershell
java -cp out com.codecrafters.redis.RedisServerTest
```

### Run the Storage Engine Test Suites

```powershell
java -cp out com.codecrafters.redis.storage.StorageEngineTest
java -cp out com.codecrafters.redis.storage.StorageEngineCompactionTest
java -cp out com.codecrafters.redis.storage.StorageEngineOrphanSSTableTest
java -cp out com.codecrafters.redis.storage.StorageEngineWalRecoveryTest
java -cp out com.codecrafters.redis.storage.WriteAheadLogTest
```

### 3. Dark Neumorphic Web Dashboard & REST API

The application includes an embedded HTTP web dashboard and RESTful JSON API running alongside the Redis TCP server:

- **Dark Neumorphic Dashboard:** Modern `#0b0f17` dark canvas with `#121824` raised cards, dual soft convex/inset shadows, and glowing cyan/emerald accents.
- **Micro-Interactions & Spring Physics:** Natural fluid easing (`cubic-bezier(0.16, 1, 0.3, 1)`), staggered entrance animations, card hover lift, and tactile button presses.
- **Fluid Modal Dialogs:** Native `<dialog>` confirmation modal for key deletion with blurred backdrop (`backdrop-filter: blur(8px)`), backdrop click dismiss, and `@starting-style` transitions.
- **Loading Skeletons & Toasts:** Shimmer loading state during key catalog retrieval and floating toast notification alerts.
- **Search & Filtering:** Real-time client-side search filtering across stored keys with instantaneous counter updates.
- **Developer Utilities:** In-browser JSON payload formatting and multi-format clipboard copy (with fallback for non-secure HTTP contexts).
- **Accessibility & Motion Preference:** WCAG AAA color contrast, full keyboard navigation, and `@media (prefers-reduced-motion: reduce)` support.

#### REST API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Serves the interactive Dark Neumorphic Web Dashboard |
| `GET` | `/api/health` | Healthcheck returning `{ "status": "ok", "keys": <count> }` |
| `GET` | `/api/keys` | Returns a JSON array of all stored keys |
| `GET` | `/api/keys/{key}` | Retrieves key metadata and value payload |
| `PUT` | `/api/keys/{key}` | Stores or updates a key with request body content |
| `DELETE` | `/api/keys/{key}` | Purges key from store and appends tombstone |

## Deployment & Configuration (Render / Cloud Platforms)

DistributedKVStore is optimized for cloud deployment on platforms like Render, Railway, or Fly.io with zero third-party runtime dependencies.

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP port for the Web Dashboard and REST API (automatically provided by Render). |
| `TCP_PORT` | `6379` | Port for the Redis RESP TCP server. |
| `DATA_DIR` | `./data` | Directory where WAL, SSTables, and manifest files are stored. |

### Render Deployment & Persistent Storage Requirements

> [!IMPORTANT]
> **Render Persistent Disk Mounting**
> Render Web Services use an ephemeral filesystem by default; files written to `./data` will be erased whenever the service restarts, scales down, or redeploys.
> To persist WAL records and SSTable data across restarts:
> 1. In your Render Dashboard, navigate to your Web Service -> **Disks** -> **Add Disk**.
> 2. Set the **Mount Path** to `/var/data` (e.g. 1 GB size).
> 3. In the **Environment Variables** tab, add:
>    - `DATA_DIR` = `/var/data`
>    - `PORT` = `8080` (or Render's automatic `$PORT`)
> 4. Set the **Build Command** to:
>    ```bash
>    mvn clean package -DskipTests
>    ```
> 5. Set the **Start Command** to:
>    ```bash
>    java -jar target/redis-java-clone-1.0.0.jar
>    ```

## Build and Run

### Prerequisites

- JDK 17 or later
- Apache Maven 3.8+ (or compile with `javac`)

### Maven Workflow (Recommended)

1. **Run Full Test Suite:**
   ```bash
   mvn clean test
   ```
   Executes all 14 test classes (Redis RESP protocol, storage engine, manifest recovery, WAL corruption recovery, multi-threaded concurrency, and HTTP integration) with JVM assertions (`-ea`) enabled.

2. **Package Standalone JAR:**
   ```bash
   mvn clean package
   ```
   Builds the executable JAR at `target/redis-java-clone-1.0.0.jar`.

3. **Run the Server:**
   ```bash
   java -jar target/redis-java-clone-1.0.0.jar
   ```

### Manual Compilation (Alternative)

Run from the project root in Windows PowerShell:

```powershell
javac --release 17 -d out (Get-ChildItem .\src -Recurse -Filter *.java | ForEach-Object { $_.FullName })
java -cp out Main
```

## Example Redis Commands

Connect via standard `redis-cli` or any Redis client library:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 ECHO "Hello World"
redis-cli -p 6379 SET user:1001 '{"name":"Alice","role":"admin"}'
redis-cli -p 6379 GET user:1001
redis-cli -p 6379 SET session:temp "active" PX 5000
redis-cli -p 6379 GET session:temp
```

## Test Results

The full test suite execution verified via `mvn clean test`:

| Test Suite | Result |
|---|---|
| `RedisServerTest` | 37 tests passed, 0 failed (RESP, TCP streams, TTL expiry, 50 parallel clients) |
| `BloomFilterTest` | Passed (100% true-positive accuracy for existing keys) |
| `RedisStoreRecoveryTest` | Passed (Survives restart, expired keys passively evicted) |
| `SSTableManifestTest` | 8 tests passed (Manifest round-trip, invalid IDs, corrupted entry, duplicate detection) |
| `SSTableTest` | Passed (1,000-key sparse-index lookups + LRU cache capacity and eviction) |
| `StorageEngineCompactionTest` | Passed (Compaction restart recovery with single-table merge) |
| `StorageEngineCompactionWalTest` | Passed (Compaction preserves unflushed WAL records) |
| `StorageEngineConcurrencyTest` | Passed (8 concurrent workers, 800 keys, parallel writes/reads/flushes) |
| `StorageEngineConcurrentDeleteTest` | Passed (400 concurrent deletes, flushes, and restart recovery) |
| `StorageEngineOrphanSSTableTest` | Passed (Orphan SSTable recovery and subsequent flush) |
| `StorageEngineTest` | 5 tests passed (Restart recovery, delete & snapshot, tombstone flush/recovery) |
| `StorageEngineWalRecoveryTest` | Passed (Unflushed PUT and DELETE recovery) |
| `WriteAheadLogTest` | 7 tests passed (Replay, restart recovery, incomplete tail, CRC32 corruption detection) |
| `WebServerIntegrationTest` | Passed (HTML served, CSS/JS verified, health API, PUT, GET, keys list, DELETE) |
| **Overall Summary** | **14 / 14 Passed (0 Failures, 100% Pass Rate)** |

## Current Limitations

This is an educational Java server and storage-engine project:

- Distributed consensus algorithms (e.g. Raft/Paxos) are not implemented.
- Storage engine compaction is currently single-generation rather than tiered/leveled LSM.
- Full power-loss durability across every filesystem condition requires hardware-level write barriers (`fsync` tuning).

## Repository

GitHub: https://github.com/rahamanmursalim-create/DistributedKVStore

