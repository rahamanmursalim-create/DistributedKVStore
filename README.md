# DistributedKVStore

A Java 17 key-value server with a Redis-style RESP-over-TCP interface, an HTTP dashboard/API, and a separately implemented LSM-style storage-engine component.

**Live dashboard:** <https://distributedkvstore.onrender.com>  
**Health endpoint:** <https://distributedkvstore.onrender.com/api/health>  
**Repository:** <https://github.com/rahamanmursalim-create/DistributedKVStore>

> **Scope note:** Despite the project name, the current implementation does **not** provide a multi-node distributed cluster, replication, consensus, sharding, or automatic failover. It is a single-process key-value server and storage-engine learning project. The TCP server and HTTP API currently share `RedisStore` (an in-memory map backed by a WAL). The `StorageEngine` LSM-style component is implemented separately and is not yet wired into the live server's request path.

## Contents

- [Highlights](#highlights)
- [Architecture](#architecture)
- [Implemented commands](#implemented-tcp-commands)
- [HTTP API](#http-api)
- [Technology stack](#technology-stack)
- [Run locally](#run-locally)
- [Test the API](#test-the-api)
- [Docker](#docker)
- [Deploy to Render](#deploy-to-render)
- [Storage and persistence caveats](#storage-and-persistence-caveats)
- [Security notes](#security-notes)
- [Project layout](#project-layout)
- [Tests](#tests)
- [Roadmap](#roadmap)
- [References](#references)
- [License](#license)

## Highlights

### TCP server and RESP protocol

- TCP server with concurrent client handling.
- RESP request parsing and response serialization.
- Handles fragmented input and pipelined requests.
- Implements a useful Redis-style command subset, including `PING`, `ECHO`, `SET`, `GET`, `DEL`, and `EXISTS`.
- `SET` supports expiration with `EX` (seconds) and `PX` (milliseconds).
- Supports background and access-triggered expiration in `RedisStore`.

### HTTP dashboard and REST API

- Embedded dashboard served at `/`.
- JSON health and key-list endpoints.
- CRUD endpoints for creating/updating, reading, and deleting keys.
- Dark neumorphic UI with motion effects, search/filtering, loading skeletons, toast feedback, clipboard helpers, JSON formatting, and a delete-confirmation dialog.
- No external frontend framework or CDN is required by the embedded dashboard.

### Storage-engine learning components

The separate `StorageEngine` package demonstrates several LSM-style storage ideas:

- Sorted in-memory `MemTable` with deletion tombstones.
- Append-only WAL records with checksum verification and replay.
- Immutable, sorted SSTable files.
- Manifest-based tracking and recovery of active SSTables.
- Sparse-index lookups, a Bloom filter, and a bounded LRU record cache.
- Flush and compaction workflows, with tests for restart recovery and interrupted-operation scenarios.

The standalone storage engine and the live server store are currently separate components. See [Architecture](#architecture) and [Storage and persistence caveats](#storage-and-persistence-caveats).

## Architecture

![DistributedKVStore architecture diagram](docs/images/distributedkvstore-architecture.png)

*Architecture image provided with this project.*

### Runtime request path

**TCP client**

1. `Server` accepts a TCP connection.
2. `ClientHandler` reads RESP messages using `RespReader`.
3. `Command` and `CommandHandler` parse and execute the command.
4. `RespWriter` serializes the response.
5. The command handler reads or updates the shared `RedisStore`.

**Dashboard / HTTP client**

1. `WebServer` serves the dashboard and REST API.
2. The HTTP handlers read or update the same `RedisStore` used by the TCP server.
3. The API returns JSON responses and HTTP status codes.

**Live-server storage path**

- `RedisStore` keeps current values in a concurrent in-memory map.
- `WriteAheadLog` appends and syncs mutations before publishing the corresponding in-memory update.
- At startup, `Main` reads `DATA_DIR` and passes the selected `wal.log` path to `RedisStore`.

**Separate LSM-style storage path**

- `StorageEngine` uses a `MemTable`, per-generation WAL files, SSTables, and a `MANIFEST`.
- It supports recovery, flush, and compaction through its own API.
- The current `Main` wiring does not use this `StorageEngine` for the live TCP/HTTP requests. The provided diagram is therefore a conceptual component view, not proof that every pictured arrow represents an active runtime call.

## Implemented TCP commands

The server implements this command subset; it is **not** a complete Redis replacement.

| Command | Purpose | Notes |
|---|---|---|
| `PING` | Check connectivity | Optional single argument is supported |
| `ECHO <message>` | Return a message | One argument |
| `SET <key> <value>` | Create or replace a value | Also supports `EX <seconds>` and `PX <milliseconds>` |
| `PUT <key> <value>` | Create or replace a value | Alias-style custom command |
| `GET <key>` | Read a value | Missing values return a RESP null bulk string |
| `DEL <key> [key ...]` | Delete keys | `DELETE` is also accepted |
| `EXISTS <key>` | Check if a key exists | |
| `KEYS [pattern]` | List matching keys | Supports `*` and `?` glob characters |
| `COMMAND` | Command metadata/help response | Limited implementation |
| `INFO` | Server information | Limited implementation |
| `CONFIG` | Configuration command | Limited implementation |

Example interactive commands:

```text
PING
ECHO hello
SET user:1 Mursalim
GET user:1
SET session:demo active EX 60
EXISTS user:1
KEYS user:*
DEL user:1
```

## HTTP API

The web dashboard and API are served by the same Java HTTP server.

| Method | Endpoint | Behavior |
|---|---|---|
| `GET` | `/` | Serves the dashboard |
| `GET` | `/api/health` | Returns service status and current key count |
| `GET` | `/api/keys` | Returns a JSON array of keys |
| `GET` | `/api/keys/{key}` | Returns a key and its value; returns `404` if missing |
| `PUT` | `/api/keys/{key}` | Creates/replaces a key using the raw request body as the value |
| `DELETE` | `/api/keys/{key}` | Deletes a key |

### Example responses

Health check:

```json
{"status":"ok","keys":0}
```

Read a key:

```json
{"key":"user:1","value":"Mursalim"}
```

For `PUT`, the request body is stored as text. JSON can be stored as a value by sending the JSON document as the raw body; the API does not require the body itself to be a JSON object.

## Technology stack

- **Language:** Java 17+
- **Build:** Apache Maven
- **Networking:** Java TCP sockets and `com.sun.net.httpserver.HttpServer`
- **Protocol:** RESP-compatible subset over TCP
- **Persistence used by the live server:** Append-only WAL + in-memory map
- **Separate storage-engine components:** MemTable, WAL, SSTables, manifest, Bloom filter, sparse index, LRU cache
- **Packaging / deployment:** Docker
- **Hosting:** Render Web Service

## Run locally

### Prerequisites

- JDK 17 or later
- Apache Maven 3.8+
- Git
- Optional: Docker Desktop to run the container locally

### Build and test

From the repository root:

```bash
mvn clean test
```

The Maven test phase runs the project's custom `AllTestsRunner`, which discovers `*Test` classes and runs them with JVM assertions enabled.

Build the executable JAR:

```bash
mvn clean package
```

The expected artifact is:

```text
target/redis-java-clone-1.0.0.jar
```

Run the server:

```bash
java -jar target/redis-java-clone-1.0.0.jar
```

By default, the HTTP dashboard/API listens on port `8080` and the RESP TCP server listens on port `6379`. `PORT` and `TCP_PORT` can override these values through environment variables.

Open the dashboard at <http://localhost:8080>.

### Windows PowerShell

```powershell
$env:MAVEN_HOME = "$env:USERPROFILE\Downloads\apache-maven-3.10.0-bin\apache-maven-3.10.0"
$env:Path = "$env:MAVEN_HOME\bin;$env:Path"

mvn clean test
mvn clean package
java -jar .\target\redis-java-clone-1.0.0.jar
```

If Maven is already on your `PATH`, you can skip the first two lines.

## Test the API

The examples below use `curl`. In Windows PowerShell, use `curl.exe` to call the real curl executable.

Set a value:

```bash
curl -i -X PUT "http://localhost:8080/api/keys/user%3A1" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "Mursalim"
```

Read it:

```bash
curl -i "http://localhost:8080/api/keys/user%3A1"
```

List keys:

```bash
curl -i "http://localhost:8080/api/keys"
```

Delete it:

```bash
curl -i -X DELETE "http://localhost:8080/api/keys/user%3A1"
```

Check health:

```bash
curl -i "http://localhost:8080/api/health"
```

Replace `http://localhost:8080` with the live Render URL to test the deployed HTTP API. The first request after inactivity may take longer on a free Render instance because free web services spin down when idle.

## Docker

The repository includes a multi-stage `Dockerfile` that builds the application using Maven/Java 17 and runs it using a Java 17 JRE image.

Build the image:

```bash
docker build -t distributedkvstore .
```

Run it locally:

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

The volume mount above preserves data in the local `./data` directory on your Docker host. Ensure Docker Desktop is running before executing these commands.

## Deploy to Render

This project uses **Docker** because Render's native language list does not include Java/JVM runtimes. Render supports Dockerfiles for Java-based services.

1. Push the latest code to the `main` branch on GitHub.
2. In Render, create a **Web Service** and select the `DistributedKVStore` repository.
3. Set **Language** to `Docker`.
4. Set **Dockerfile Path** to `./Dockerfile`.
5. Leave **Root Directory** blank.
6. Select the compute plan you intend to use and review its limitations before deploying.
7. Deploy and review the build/runtime logs.
8. Verify the deployment by opening `/api/health`, then test `PUT`, `GET`, list, and `DELETE` operations.

### Render environment variables

| Variable | Default in application / image | Purpose |
|---|---|---|
| `PORT` | Image sets `8080`; app fallback is `8080` | HTTP dashboard/API port; Render supplies the service port |
| `TCP_PORT` | `6379` | TCP RESP server port |
| `DATA_DIR` | Image sets `/var/data`; app fallback is `./data` | Directory used for the live server's `wal.log` |

**Important Render limitations:**

- Render services have an ephemeral filesystem by default. On Free web services, files can be lost when the service restarts, redeploys, or spins down; Free instances do not support persistent disks.
- Persistent disks are available on eligible paid services. If you add one, mount it at `/var/data` and keep `DATA_DIR=/var/data`.
- The current `DATA_DIR` setting directs the live `RedisStore` WAL to `DATA_DIR/wal.log`. It does **not** automatically migrate the separate `StorageEngine` files or integrate that engine into the live server.
- A Render Web Service's public URL serves HTTP/HTTPS. `EXPOSE 6379` in the Dockerfile does not, by itself, make the RESP TCP port publicly reachable through that URL. Use an appropriate TCP-capable hosting/networking setup if remote Redis-protocol clients are a requirement.
- Free web services spin down after inactivity, so the first request after a quiet period may be delayed.

See the [Render Docker docs](https://render.com/docs/docker), [Render Free instance limitations](https://render.com/docs/free), and [Render persistent disk docs](https://render.com/docs/disks) before choosing a deployment plan.

## Storage and persistence caveats

Understand these distinctions before using the project for real data:

1. **Live TCP/HTTP store:** `Main` constructs `RedisStore` with a WAL path. Keys are held in memory while the process runs, and mutations are recorded in that WAL.
2. **Separate LSM storage engine:** `StorageEngine` has its own MemTable, generation WALs, SSTable files, manifest, recovery, flush, and compaction logic. It is not currently the backing store for the HTTP/TCP server.
3. **Hosting persistence:** Writing to `/var/data` only survives deployment/restart if the hosting platform preserves that directory. Render Free web services do not provide persistent disks.
4. **Single-instance design:** The application does not implement distributed replication, consensus, sharding, or multi-node data synchronization.

Do not use the current free deployment as the only copy of important data.

## Security notes

- The public demo exposes write and delete endpoints without authentication. Anyone who discovers the URL may be able to read, create, update, or delete keys.
- Do not store passwords, API keys, personal data, or other secrets in the public demo.
- Before using this beyond a demonstration, add authentication/authorization, request limits, stricter input validation, logging/monitoring, and a persistence/backup strategy.

## Project layout

```text

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
    │           ├── storage/
    │           │   ├── BloomFilter.java
    │           │   ├── MemTable.java
    │           │   ├── RedisStore.java
    │           │   ├── SSTable.java
    │           │   ├── SSTableManifest.java
    │           │   ├── StorageEngine.java
    │           │   ├── ValueEntry.java
    │           │   └── WriteAheadLog.java
    │           └── web/
    │               └── WebServer.java
    └── test/
        └── java/
            └── com/codecrafters/redis/
                ├── AllTestsRunner.java
                ├── RedisServerTest.java
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

## Tests

**Verification: 14/14 test suites passed · 0 failures**

The test suite covers:

- **Protocol & Server:** RESP parsing, serialization, pipelining, fragmented input, and concurrent clients.
- **Write-Ahead Log:** Writes, replay, incomplete-tail recovery, and checksum validation.
- **Storage:** TTL, recovery, SSTable lookups, sparse indexes, Bloom filters, and LRU caching.
- **Storage Reliability:** Manifest validation, compaction, orphan SSTables, and concurrent operations.
- **Web API:** Dashboard delivery, health checks, and REST API CRUD integration.

**Run tests locally:**

```bash
mvn clean test
```

*Test results reflect the verified local run; run the command again to verify the current codebase.*

Run the suite locally with `mvn clean test`; use the actual command output as the source of truth for pass/fail status.

## Roadmap

Potential next steps (not yet claimed as implemented):

- Integrate the standalone LSM `StorageEngine` behind the live TCP and HTTP interfaces.
- Add API authentication, authorization, and request limits.
- Add persistent storage and a documented backup/restore workflow.
- Add automated CI for build, tests, and Docker image validation.
- Add replication, partitioning, and coordination only if the project is intentionally extended into a distributed system.
- Add metrics and structured operational logs.

## References

- [Render: Docker deployments](https://render.com/docs/docker)
- [Render: Free instance limitations](https://render.com/docs/free)
- [Render: Persistent disks](https://render.com/docs/disks)
- [GitHub Docs: README files and relative image paths](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes)

## License

See [LICENSE](LICENSE) for the license and copyright notice included with this repository.
