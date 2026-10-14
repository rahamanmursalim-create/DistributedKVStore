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

### Start the Server

The repository includes `spawn_redis_server.sh`. On a compatible Bash environment, the existing runner can be used:

```bash
./spawn_redis_server.sh
```

Check the server's configured port in the implementation before connecting.

## Example Redis Commands

If the server is running on port `6379`, a Redis CLI client can be used to try:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 ECHO "Hello World"
redis-cli -p 6379 SET mykey myvalue
redis-cli -p 6379 GET mykey
redis-cli -p 6379 SET temporary value PX 5000
redis-cli -p 6379 GET temporary
```

The final command examples assume the server is listening on port `6379`; use the actual configured port if different.

## Test Results

The following results were verified locally:

| Test suite | Result |
|---|---|
| Redis Server and RESP | 37 passed, 0 failed |
| WriteAheadLog | 5 tests passed |
| SSTable Manifest | 8 tests passed |
| Compaction manifest crash checkpoint | Recovery passed; exit code 73 |
| Flush manifest crash checkpoint | Recovery passed; exit code 75 |
| Old SSTable deletion crash checkpoint | Recovery passed; exit code 74 |

The crash-recovery checks cover specific checkpoints, not every possible crash or power-loss scenario. These results do not establish production-level reliability.
## Current Limitations

This is an educational Java server and storage-engine project, not a production-ready distributed database.

- Distributed replication and consensus are not implemented as claimed features.
- TTL-aware persistence is not provided by the current storage-engine API.
- Full power-loss durability across every filesystem and failure point has not been formally established.
- Performance benchmarks and production-scale reliability testing have not been documented here.

## Repository

GitHub: https://github.com/skmursalim9779-create/DistributedKVStore
