# Build Your Own Redis (Java)

A lightweight, concurrent Redis server implementation written in pure Java (JDK 17+) adhering strictly to the Redis Serialization Protocol (RESP) specification and CodeCrafters challenge requirements.

---

## 🚀 Features & Stages Implemented

- **Pure Java Standard Library**: Zero external third-party dependencies (`java.net`, `java.io`, `java.util.concurrent`).
- **Stage 1 & 2 (Server Initialization & PING)**:
  - Binds to port `6379` with `setReuseAddress(true)`.
  - Responds to `PING` with simple string `+PONG\r\n` or custom arguments as bulk strings.
- **Stage 3 & 4 (Concurrent Clients & ECHO)**:
  - Multi-threaded client handling using virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) with fallback to cached thread pool.
  - Implements `ECHO <message>` command returning Bulk String format (`$<length>\r\n<arg>\r\n`).
- **Stage 5 (SET & GET Commands)**:
  - Thread-safe storage with `ConcurrentHashMap<String, ValueEntry>`.
  - Implements `SET <key> <value>` replying with `+OK\r\n`.
  - Implements `GET <key>` replying with Bulk String or Null Bulk String (`$-1\r\n`) if key is absent.
- **Stage 6 (Key Expiry with PX)**:
  - Supports `SET <key> <value> [PX <expiry_in_millis>]` and `[EX <expiry_in_seconds>]`.
  - Passive eviction: On `GET`, evaluates if TTL has expired and evicts atomically.
  - Active eviction: Background daemon executor periodically cleans expired keys.
- **Robust RESP Protocol Engine**:
  - Handles streaming TCP input, fragmented packets, and pipelining.
  - Supports RESP Arrays (`*`), Bulk Strings (`$`), Simple Strings (`+`), Integers (`:`), and Errors (`-`).
  - Supports raw inline command input from telnet / netcat.

---

---

## 💾 Persistent Storage Engine

The project includes an LSM-style storage engine with the following components:

- **MemTable:** Maintains sorted in-memory key-value entries and deletion tombstones.
- **Write-Ahead Log (WAL):** Records PUT and DELETE operations and replays complete records during recovery.
- **SSTables:** Stores immutable sorted-table data on disk.
- **Manifest:** Tracks active SSTable IDs during restart recovery.
- **Flush:** Persists MemTable contents to an SSTable and updates the manifest.
- **Compaction:** Merges active SSTables and preserves the latest values and deletion state.
- **Recovery tests:** Cover WAL replay, restart recovery, compaction recovery, and orphan SSTable handling.

### Storage Engine Test Commands

Run these commands from the project root after compiling the Java sources:

```powershell
java -cp out com.codecrafters.redis.storage.StorageEngineTest
java -cp out com.codecrafters.redis.storage.StorageEngineCompactionTest
java -cp out com.codecrafters.redis.storage.StorageEngineOrphanSSTableTest
java -cp out com.codecrafters.redis.storage.StorageEngineWalRecoveryTest
java -cp out com.codecrafters.redis.storage.WriteAheadLogTest
```

The storage engine and the Redis server have separate test suites. The current storage engine API does not itself implement TTL-aware persistence.


## 📁 Project Structure

```
redis-java-clone/
├── src/
│   ├── main/
│   │   └── java/
│   │       ├── Main.java                                # CodeCrafters entry point
│   │       └── com/codecrafters/redis/
│   │           ├── Server.java                          # TCP Server & connection accept loop
│   │           ├── ClientHandler.java                   # Per-connection read/eval/write loop
│   │           ├── protocol/
│   │           │   ├── RespType.java                    # RESP type definitions
│   │           │   ├── RespValue.java                   # Immutable RESP value model & encoder
│   │           │   ├── RespReader.java                  # Streaming RESP parser
│   │           │   └── RespWriter.java                  # Low-overhead RESP output writer
│   │           ├── storage/
│   │           │   ├── ValueEntry.java                  # Key value + TTL metadata
│   │           │   └── RedisStore.java                  # Thread-safe in-memory store
│   │           └── command/
│   │               ├── Command.java                     # Parsed command representation
│   │               └── CommandHandler.java              # Command dispatcher & executor
│   └── test/
│       └── java/
│           └── com/codecrafters/redis/
│               └── RedisServerTest.java                 # Comprehensive test suite (37+ tests)
├── spawn_redis_server.sh                                # CodeCrafters runner script
└── pom.xml                                              # Standard Maven POM
```

---

## 🛠️ Building and Running

### Using the CodeCrafters runner script:
```bash
./spawn_redis_server.sh [--port <port>]
```

### Running the built-in test suite:
```bash
mkdir -p /tmp/test-classes
javac -d /tmp/test-classes $(find src -name "*.java")
java -cp /tmp/test-classes com.codecrafters.redis.RedisServerTest
```

### Connecting with `redis-cli`:
```bash
redis-cli -p 6379 PING
redis-cli -p 6379 ECHO "Hello World"
redis-cli -p 6379 SET mykey myvalue PX 5000
redis-cli -p 6379 GET mykey
```
