
package com.codecrafters.redis.web;

import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.storage.KeyValueAccess;
import com.codecrafters.redis.raft.RaftGroup;
import com.codecrafters.redis.raft.MultiRaftStore;
import com.codecrafters.redis.sharding.ShardAdminService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLContext;

public final class WebServer implements AutoCloseable {
    private final RedisStore store;
    private final KeyValueAccess access;
    private final RaftGroup raftGroup;
    private final boolean followerReads;
    private final ShardAdminService shardAdmin;
    private final String clientToken;
    private final HttpServer server;
    private final ExecutorService httpExecutor;

    public WebServer(RedisStore store, int port) throws IOException {
        this(store, port, null, false);
    }

    public WebServer(RedisStore store, int port, RaftGroup raftGroup, boolean followerReads) throws IOException {
        this(store, store, port, raftGroup, followerReads);
    }

    public WebServer(RedisStore store, KeyValueAccess access, int port, RaftGroup raftGroup,
                     boolean followerReads) throws IOException {
        this(store, access, port, raftGroup, followerReads, null, null);
    }

    public WebServer(RedisStore store, KeyValueAccess access, int port, RaftGroup raftGroup,
                     boolean followerReads, ShardAdminService shardAdmin) throws IOException {
        this(store, access, port, raftGroup, followerReads, shardAdmin, null);
    }

    public WebServer(RedisStore store, KeyValueAccess access, int port, RaftGroup raftGroup,
                     boolean followerReads, ShardAdminService shardAdmin, String clientToken) throws IOException {
        this(store, access, port, raftGroup, followerReads, shardAdmin, clientToken,
                com.codecrafters.redis.rpc.TlsContext.configured()
                        ? com.codecrafters.redis.rpc.TlsContext.fromEnvironment() : null);
    }

    /** Explicit TLS context constructor for embedded deployments and certificate rotation. */
    public WebServer(RedisStore store, KeyValueAccess access, int port, RaftGroup raftGroup,
                     boolean followerReads, ShardAdminService shardAdmin, String clientToken,
                     SSLContext tlsContext) throws IOException {
        this.store = store;
        this.access = access;
        this.raftGroup = raftGroup;
        this.followerReads = followerReads;
        this.shardAdmin = shardAdmin;
        this.clientToken = clientToken == null || clientToken.isBlank() ? null : clientToken;
        String bindHost = System.getenv().getOrDefault("KV_BIND_HOST", "127.0.0.1");
        if (tlsContext != null) {
            HttpsServer https = HttpsServer.create(new InetSocketAddress(bindHost, port), 0);
            https.setHttpsConfigurator(new HttpsConfigurator(tlsContext));
            this.server = https;
        } else this.server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);

        server.createContext("/api/health", this::health);
        server.createContext("/api/raft/status", this::raftStatus);
        server.createContext("/api/keys", this::keys);
        server.createContext("/api/shards", this::shards);
        server.createContext("/", this::dashboard);

        this.httpExecutor = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "kv-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(httpExecutor);
    }

    public void start() {
        server.start();
        System.out.println("HTTP dashboard listening on port "
                + server.getAddress().getPort());
    }

    private void health(HttpExchange exchange) throws IOException {
        if (!authorized(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "text/plain", "Method not allowed");
            return;
        }
        send(exchange, 200, "application/json",
                "{\"status\":\"ok\",\"keys\":" + access.size() + "}");
    }

    private void raftStatus(HttpExchange exchange) throws IOException {
        if (!authorized(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "text/plain", "Method not allowed");
            return;
        }
        if (access instanceof MultiRaftStore multi) {
            StringBuilder json = new StringBuilder("{\"groups\":{");
            boolean first = true;
            for (var entry : multi.leadershipStatus().entrySet()) {
                if (!first) json.append(',');
                first = false;
                var status = entry.getValue();
                json.append('\"').append(escape(entry.getKey())).append("\":{\"leader\":\"")
                        .append(escape(status.leaderId() == null ? "" : status.leaderId()))
                        .append("\",\"localLeader\":").append(status.localLeader())
                        .append(",\"term\":").append(status.term())
                        .append(",\"commitIndex\":").append(status.commitIndex())
                        .append(",\"ownershipEpoch\":").append(status.ownershipEpoch()).append('}');
            }
            send(exchange, 200, "application/json", json.append("}}").toString());
            return;
        }
        if (raftGroup == null) {
            send(exchange, 404, "application/json", "{\"error\":\"Raft is not enabled\"}");
            return;
        }
        send(exchange, 200, "application/json", "{\"groups\":{\"" + escape(raftGroup.getGroupId())
                + "\":{\"leader\":\"" + escape(raftGroup.getLeaderId() == null ? "" : raftGroup.getLeaderId())
                + "\",\"localLeader\":" + raftGroup.isLeader() + ",\"term\":" + raftGroup.getTerm()
                + ",\"commitIndex\":" + raftGroup.getCommitIndex() + ",\"ownershipEpoch\":"
                + raftGroup.getOwnershipEpoch() + "}}}");
    }

    private void keys(HttpExchange exchange) throws IOException {
        if (!authorized(exchange)) return;
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getRawPath();

        if ("/api/keys".equals(path) || "/api/keys/".equals(path)) {
            if (!"GET".equals(method)) {
                send(exchange, 405, "text/plain", "Method not allowed");
                return;
            }

            List<String> keys;
            try { keys = readKeys("*"); }
            catch (IOException e) { send(exchange, 503, "application/json", errorJson(e)); return; }
            StringBuilder json = new StringBuilder("[");
            for (int i = 0; i < keys.size(); i++) {
                if (i > 0) json.append(',');
                json.append('"').append(escape(keys.get(i))).append('"');
            }
            json.append(']');
            send(exchange, 200, "application/json", json.toString());
            return;
        }

        String encodedKey = path.substring("/api/keys/".length());
        String key = URLDecoder.decode(
                encodedKey.replace("+", "%2B"), StandardCharsets.UTF_8);

        if (key.isBlank()) {
            send(exchange, 400, "text/plain", "Key cannot be empty");
            return;
        }

        switch (method) {
            case "GET": {
                String value;
                try { value = readValue(key); }
                catch (IOException e) { send(exchange, 503, "application/json", errorJson(e)); break; }
                if (value == null) {
                    send(exchange, 404, "application/json",
                            "{\"error\":\"Key not found\"}");
                } else {
                    send(exchange, 200, "application/json",
                            "{\"key\":\"" + escape(key)
                                    + "\",\"value\":\"" + escape(value) + "\"}");
                }
                break;
            }
            case "PUT": {
                String value = new String(
                        exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8);
                try {
                    if (raftGroup == null) access.set(key, value);
                    else raftGroup.put(key, value.getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) { send(exchange, 503, "application/json", errorJson(e)); break; }
                send(exchange, 200, "application/json",
                        "{\"status\":\"saved\",\"key\":\""
                                + escape(key) + "\"}");
                break;
            }
            case "DELETE": {
                int deleted;
                try {
                    if (raftGroup == null) deleted = access.delete(key);
                    else deleted = raftGroup.delete(key) ? 1 : 0;
                } catch (IOException e) { send(exchange, 503, "application/json", errorJson(e)); break; }
                if (deleted == 0) {
                    send(exchange, 404, "application/json",
                            "{\"error\":\"Key not found\"}");
                } else {
                    send(exchange, 200, "application/json",
                            "{\"status\":\"deleted\"}");
                }
                break;
            }
            default:
                send(exchange, 405, "text/plain", "Method not allowed");
        }
    }

    private void shards(HttpExchange exchange) throws IOException {
        if (!authorized(exchange)) return;
        if (shardAdmin == null) {
            send(exchange, 404, "application/json", "{\"error\":\"Shard administration is disabled\"}");
            return;
        }
        if (!shardAdmin.isAuthorized(exchange.getRequestHeaders().getFirst("Authorization"))) {
            exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
            send(exchange, 401, "application/json", "{\"error\":\"Authentication required\"}");
            return;
        }

        String path = exchange.getRequestURI().getRawPath();
        String prefix = "/api/shards/";
        String encodedId = path.startsWith(prefix) ? path.substring(prefix.length()) : "";
        String nodeId;
        try { nodeId = URLDecoder.decode(encodedId.replace("+", "%2B"), StandardCharsets.UTF_8); }
        catch (IllegalArgumentException e) {
            send(exchange, 400, "application/json", "{\"error\":\"Invalid shard ID\"}");
            return;
        }

        try {
            if ("GET".equals(exchange.getRequestMethod()) && nodeId.isEmpty()) {
                StringBuilder json = new StringBuilder("{\"shards\":[");
                List<String> ids = shardAdmin.nodeIds();
                for (int i = 0; i < ids.size(); i++) {
                    if (i > 0) json.append(',');
                    json.append('\"').append(escape(ids.get(i))).append('\"');
                }
                send(exchange, 200, "application/json", json.append("]}").toString());
            } else if ("POST".equals(exchange.getRequestMethod()) && !nodeId.isEmpty()) {
                shardAdmin.addNode(nodeId);
                send(exchange, 201, "application/json", "{\"status\":\"added\",\"shard\":\"" + escape(nodeId) + "\"}");
            } else if ("DELETE".equals(exchange.getRequestMethod()) && !nodeId.isEmpty()) {
                shardAdmin.removeNode(nodeId);
                send(exchange, 200, "application/json", "{\"status\":\"removed\",\"shard\":\"" + escape(nodeId) + "\"}");
            } else {
                send(exchange, 405, "application/json", "{\"error\":\"Unsupported shard operation\"}");
            }
        } catch (IllegalArgumentException e) {
            send(exchange, 400, "application/json", "{\"error\":\"" + escape(e.getMessage()) + "\"}");
        } catch (IllegalStateException e) {
            send(exchange, 409, "application/json", "{\"error\":\"" + escape(e.getMessage()) + "\"}");
        } catch (java.io.UncheckedIOException e) {
            send(exchange, 503, "application/json", errorJson(new IOException(e.getMessage(), e)));
        }
    }

    public int getPort() { return server.getAddress().getPort(); }

    private boolean authorized(HttpExchange exchange) throws IOException {
        if (clientToken == null) return true;
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        String expected = "Bearer " + clientToken;
        if (authorization != null && java.security.MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), authorization.getBytes(StandardCharsets.UTF_8))) return true;
        exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
        send(exchange, 401, "application/json", "{\"error\":\"Authentication required\"}");
        return false;
    }

    private String readValue(String key) throws IOException {
        if (raftGroup == null) return access.get(key);
        byte[] value = followerReads && !raftGroup.isLeader()
                ? raftGroup.getFollowerRead(key) : raftGroup.getWithReadIndex(key);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    private List<String> readKeys(String pattern) throws IOException {
        if (raftGroup == null) return access.keys(pattern);
        return followerReads && !raftGroup.isLeader()
                ? raftGroup.keysFollowerRead(pattern) : raftGroup.keysWithReadIndex(pattern);
    }

    private static String errorJson(IOException error) {
        return "{\"error\":\"" + escape(error.getMessage() == null ? "cluster operation failed" : error.getMessage()) + "\"}";
    }

    private void dashboard(HttpExchange exchange) throws IOException {
        if (!authorized(exchange)) return;
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "text/plain", "Method not allowed");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (!"/".equals(path) && !"/index.html".equals(path)) {
            send(exchange, 404, "text/plain", "Not found");
            return;
        }

        String html = """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <meta name="color-scheme" content="dark">
                  <title>DistributedKVStore · Console</title>
                  <style>
                    :root {
                      --bg-canvas: #0b0f17;
                      --surface-neu: #121824;
                      --surface-neu-raised: #161e2e;
                      --surface-neu-inset: #090d14;
                      --shadow-neu-dark: #05070c;
                      --shadow-neu-light: rgba(255, 255, 255, 0.035);
                      --border-subtle: rgba(255, 255, 255, 0.06);
                      --border-active: rgba(56, 189, 248, 0.4);
                      --text-primary: #f1f5f9;
                      --text-secondary: #94a3b8;
                      --text-muted: #64748b;
                      --accent-cyan: #38bdf8;
                      --accent-cyan-glow: rgba(56, 189, 248, 0.25);
                      --accent-emerald: #10b981;
                      --accent-emerald-glow: rgba(16, 185, 129, 0.25);
                      --accent-rose: #f43f5e;
                      --accent-rose-glow: rgba(244, 63, 94, 0.25);
                      --spring-easing: cubic-bezier(0.16, 1, 0.3, 1);
                      --spring-snappy: cubic-bezier(0.34, 1.56, 0.64, 1);
                      --radius-card: 18px;
                      --radius-inner: 12px;
                      --radius-btn: 10px;
                    }

                    *, *::before, *::after {
                      box-sizing: border-box;
                      margin: 0;
                      padding: 0;
                    }

                    body {
                      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Inter", sans-serif;
                      background-color: var(--bg-canvas);
                      background-image:
                        radial-gradient(circle at 10% 10%, rgba(56, 189, 248, 0.05) 0%, transparent 40%),
                        radial-gradient(circle at 90% 90%, rgba(99, 102, 241, 0.04) 0%, transparent 45%),
                        radial-gradient(circle at 50% 50%, #101622 0%, #0b0f17 100%);
                      background-attachment: fixed;
                      color: var(--text-primary);
                      min-height: 100vh;
                      padding: 32px 20px 60px;
                      line-height: 1.5;
                      -webkit-font-smoothing: antialiased;
                    }

                    .container {
                      max-width: 960px;
                      margin: 0 auto;
                      display: flex;
                      flex-direction: column;
                      gap: 24px;
                    }

                    /* Staggered entrance animations */
                    .anim-fade-in {
                      animation: fadeIn 0.6s var(--spring-easing) both;
                    }
                    .anim-stagger-1 { animation: slideUp 0.6s var(--spring-easing) 0.06s both; }
                    .anim-stagger-2 { animation: slideUp 0.6s var(--spring-easing) 0.14s both; }
                    .anim-stagger-3 { animation: slideUp 0.6s var(--spring-easing) 0.22s both; }
                    .anim-stagger-4 { animation: slideUp 0.6s var(--spring-easing) 0.30s both; }

                    @keyframes fadeIn {
                      from { opacity: 0; }
                      to { opacity: 1; }
                    }
                    @keyframes slideUp {
                      from { opacity: 0; transform: translateY(18px); }
                      to { opacity: 1; transform: translateY(0); }
                    }
                    @keyframes slideDown {
                      from { opacity: 0; transform: translateY(-16px); }
                      to { opacity: 1; transform: translateY(0); }
                    }
                    @keyframes pulseBadge {
                      0%, 100% { transform: scale(1); opacity: 1; }
                      50% { transform: scale(1.15); opacity: 0.7; }
                    }
                    @keyframes highlightFlash {
                      0% { box-shadow: 0 0 0 0 var(--accent-cyan); }
                      30% { box-shadow: 0 0 0 6px var(--accent-cyan-glow); }
                      100% { box-shadow: 0 0 0 0 transparent; }
                    }
                    @keyframes shimmer {
                      0% { background-position: -200% 0; }
                      100% { background-position: 200% 0; }
                    }

                    /* Header & Brand */
                    .header {
                      display: flex;
                      flex-wrap: wrap;
                      align-items: center;
                      justify-content: space-between;
                      gap: 16px;
                      padding: 10px 4px;
                      animation: slideDown 0.6s var(--spring-easing) both;
                    }
                    .brand {
                      display: flex;
                      align-items: center;
                      gap: 14px;
                    }
                    .brand-icon {
                      width: 44px;
                      height: 44px;
                      border-radius: 12px;
                      background: linear-gradient(135deg, #1e293b, #0f172a);
                      border: 1px solid var(--border-subtle);
                      box-shadow:
                        6px 6px 14px var(--shadow-neu-dark),
                        -4px -4px 10px var(--shadow-neu-light);
                      display: flex;
                      align-items: center;
                      justify-content: center;
                      color: var(--accent-cyan);
                      transition: transform 0.3s var(--spring-snappy), box-shadow 0.3s ease;
                    }
                    .brand:hover .brand-icon {
                      transform: rotate(6deg) scale(1.05);
                      box-shadow:
                        8px 8px 18px var(--shadow-neu-dark),
                        -5px -5px 12px var(--shadow-neu-light),
                        0 0 16px var(--accent-cyan-glow);
                    }
                    .brand-text h1 {
                      font-size: 1.45rem;
                      font-weight: 700;
                      letter-spacing: -0.02em;
                      color: #fff;
                      background: linear-gradient(135deg, #ffffff 40%, #94a3b8 100%);
                      -webkit-background-clip: text;
                      -webkit-text-fill-color: transparent;
                    }
                    .brand-text p {
                      font-size: 0.82rem;
                      color: var(--text-secondary);
                      letter-spacing: 0.01em;
                    }

                    /* Header status pill */
                    .status-pill {
                      display: inline-flex;
                      align-items: center;
                      gap: 9px;
                      padding: 8px 16px;
                      border-radius: 999px;
                      background: var(--surface-neu);
                      border: 1px solid var(--border-subtle);
                      box-shadow:
                        5px 5px 12px var(--shadow-neu-dark),
                        -3px -3px 8px var(--shadow-neu-light);
                      font-size: 0.82rem;
                      font-weight: 500;
                      color: var(--text-secondary);
                    }
                    .status-dot {
                      width: 8px;
                      height: 8px;
                      border-radius: 50%;
                      background: var(--accent-emerald);
                      box-shadow: 0 0 8px var(--accent-emerald);
                      animation: pulseBadge 2.2s infinite ease-in-out;
                    }

                    /* Neumorphic Cards */
                    .neu-card {
                      background: var(--surface-neu);
                      border-radius: var(--radius-card);
                      border: 1px solid var(--border-subtle);
                      box-shadow:
                        8px 8px 22px var(--shadow-neu-dark),
                        -6px -6px 18px var(--shadow-neu-light);
                      padding: 24px;
                      position: relative;
                      transition: transform 0.3s var(--spring-easing), box-shadow 0.3s ease, border-color 0.3s ease;
                    }
                    .neu-card:hover {
                      border-color: rgba(255, 255, 255, 0.08);
                    }

                    /* Stats Grid */
                    .stats-grid {
                      display: grid;
                      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                      gap: 16px;
                    }
                    .stat-card {
                      background: var(--surface-neu);
                      border-radius: 14px;
                      border: 1px solid var(--border-subtle);
                      box-shadow:
                        6px 6px 16px var(--shadow-neu-dark),
                        -4px -4px 12px var(--shadow-neu-light);
                      padding: 18px 20px;
                      display: flex;
                      flex-direction: column;
                      gap: 6px;
                      transition: transform 0.25s var(--spring-easing), box-shadow 0.25s ease;
                    }
                    .stat-card:hover {
                      transform: translateY(-2px);
                      box-shadow:
                        8px 8px 20px var(--shadow-neu-dark),
                        -5px -5px 14px var(--shadow-neu-light);
                    }
                    .stat-label {
                      font-size: 0.76rem;
                      font-weight: 600;
                      text-transform: uppercase;
                      letter-spacing: 0.06em;
                      color: var(--text-muted);
                    }
                    .stat-value {
                      font-size: 1.55rem;
                      font-weight: 700;
                      color: var(--text-primary);
                      font-variant-numeric: tabular-nums;
                    }
                    .stat-meta {
                      font-size: 0.76rem;
                      color: var(--accent-cyan);
                    }

                    /* Card Titles */
                    .card-header {
                      display: flex;
                      align-items: center;
                      justify-content: space-between;
                      margin-bottom: 20px;
                      gap: 12px;
                    }
                    .card-title {
                      display: flex;
                      align-items: center;
                      gap: 10px;
                      font-size: 1.05rem;
                      font-weight: 600;
                      color: #fff;
                    }
                    .card-title svg {
                      color: var(--accent-cyan);
                    }
                    .badge-count {
                      padding: 2px 10px;
                      border-radius: 999px;
                      font-size: 0.75rem;
                      font-weight: 600;
                      background: var(--surface-neu-inset);
                      border: 1px solid var(--border-subtle);
                      color: var(--accent-cyan);
                      box-shadow: inset 2px 2px 5px var(--shadow-neu-dark);
                    }

                    /* Inputs & Textareas: Inset Neumorphic */
                    .input-group {
                      display: flex;
                      flex-direction: column;
                      gap: 8px;
                      margin-bottom: 16px;
                    }
                    .input-label {
                      font-size: 0.8rem;
                      font-weight: 500;
                      color: var(--text-secondary);
                      display: flex;
                      justify-content: space-between;
                    }
                    .neu-input, .neu-textarea {
                      width: 100%;
                      background: var(--surface-neu-inset);
                      color: var(--text-primary);
                      border: 1px solid var(--border-subtle);
                      border-radius: var(--radius-inner);
                      padding: 12px 16px;
                      font-size: 0.92rem;
                      font-family: inherit;
                      box-shadow:
                        inset 4px 4px 10px var(--shadow-neu-dark),
                        inset -2px -2px 6px var(--shadow-neu-light);
                      transition: border-color 0.2s ease, box-shadow 0.2s ease;
                      outline: none;
                    }
                    .neu-textarea {
                      font-family: ui-monospace, "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
                      font-size: 0.88rem;
                      resize: vertical;
                      min-height: 90px;
                    }
                    .neu-input:focus, .neu-textarea:focus {
                      border-color: var(--accent-cyan);
                      box-shadow:
                        inset 4px 4px 10px var(--shadow-neu-dark),
                        inset -2px -2px 6px var(--shadow-neu-light),
                        0 0 0 3px var(--accent-cyan-glow);
                    }
                    .neu-input::placeholder, .neu-textarea::placeholder {
                      color: var(--text-muted);
                    }

                    /* Buttons: Convex Neumorphic */
                    .btn-row {
                      display: flex;
                      flex-wrap: wrap;
                      gap: 12px;
                      align-items: center;
                      margin-top: 8px;
                    }
                    .neu-btn {
                      display: inline-flex;
                      align-items: center;
                      justify-content: center;
                      gap: 8px;
                      padding: 10px 18px;
                      border-radius: var(--radius-btn);
                      font-size: 0.88rem;
                      font-weight: 600;
                      letter-spacing: 0.01em;
                      border: 1px solid var(--border-subtle);
                      background: linear-gradient(145deg, #18202e, #111722);
                      color: var(--text-primary);
                      cursor: pointer;
                      box-shadow:
                        5px 5px 12px var(--shadow-neu-dark),
                        -3px -3px 8px var(--shadow-neu-light);
                      transition:
                        transform 0.18s var(--spring-easing),
                        box-shadow 0.18s ease,
                        background 0.2s ease,
                        border-color 0.2s ease;
                      user-select: none;
                      outline: none;
                    }
                    .neu-btn:hover {
                      transform: translateY(-2px);
                      box-shadow:
                        7px 7px 16px var(--shadow-neu-dark),
                        -4px -4px 10px var(--shadow-neu-light);
                      color: #fff;
                    }
                    .neu-btn:active {
                      transform: translateY(1px) scale(0.98);
                      box-shadow:
                        inset 3px 3px 6px var(--shadow-neu-dark),
                        inset -2px -2px 5px var(--shadow-neu-light);
                    }
                    .neu-btn:focus-visible {
                      outline: 2px solid var(--accent-cyan);
                      outline-offset: 2px;
                    }

                    .neu-btn-primary {
                      background: linear-gradient(145deg, #0284c7, #0369a1);
                      border-color: rgba(56, 189, 248, 0.4);
                      color: #fff;
                      box-shadow:
                        5px 5px 14px var(--shadow-neu-dark),
                        -3px -3px 8px var(--shadow-neu-light),
                        0 2px 14px var(--accent-cyan-glow);
                    }
                    .neu-btn-primary:hover {
                      background: linear-gradient(145deg, #0ea5e9, #0284c7);
                      box-shadow:
                        7px 7px 18px var(--shadow-neu-dark),
                        -4px -4px 10px var(--shadow-neu-light),
                        0 4px 20px rgba(56, 189, 248, 0.4);
                    }

                    .neu-btn-danger {
                      background: linear-gradient(145deg, #be123c, #9f1239);
                      border-color: rgba(244, 63, 94, 0.4);
                      color: #fff;
                      box-shadow:
                        5px 5px 14px var(--shadow-neu-dark),
                        -3px -3px 8px var(--shadow-neu-light),
                        0 2px 12px var(--accent-rose-glow);
                    }
                    .neu-btn-danger:hover {
                      background: linear-gradient(145deg, #e11d48, #be123c);
                      box-shadow:
                        7px 7px 18px var(--shadow-neu-dark),
                        -4px -4px 10px var(--shadow-neu-light),
                        0 4px 18px rgba(244, 63, 94, 0.4);
                    }

                    .neu-btn-sm {
                      padding: 6px 12px;
                      font-size: 0.8rem;
                      border-radius: 8px;
                    }
                    .neu-btn-icon-only {
                      padding: 8px;
                      border-radius: 8px;
                    }

                    /* Search and filter bar */
                    .filter-bar {
                      display: flex;
                      gap: 12px;
                      margin-bottom: 18px;
                    }
                    .search-wrap {
                      flex: 1;
                      position: relative;
                    }
                    .search-icon {
                      position: absolute;
                      left: 14px;
                      top: 50%;
                      transform: translateY(-50%);
                      color: var(--text-muted);
                      pointer-events: none;
                    }
                    .search-input {
                      padding-left: 40px !important;
                    }

                    /* Keys List */
                    .keys-list {
                      list-style: none;
                      display: flex;
                      flex-direction: column;
                      gap: 10px;
                      min-height: 80px;
                    }
                    .key-item {
                      display: flex;
                      align-items: center;
                      justify-content: space-between;
                      padding: 12px 16px;
                      border-radius: var(--radius-inner);
                      background: var(--surface-neu-raised);
                      border: 1px solid var(--border-subtle);
                      box-shadow:
                        4px 4px 10px var(--shadow-neu-dark),
                        -2px -2px 6px var(--shadow-neu-light);
                      transition:
                        transform 0.22s var(--spring-easing),
                        box-shadow 0.22s ease,
                        border-color 0.2s ease,
                        opacity 0.25s ease;
                      gap: 12px;
                    }
                    .key-item:hover {
                      transform: translateX(4px);
                      border-color: rgba(56, 189, 248, 0.3);
                      box-shadow:
                        6px 6px 14px var(--shadow-neu-dark),
                        -3px -3px 8px var(--shadow-neu-light),
                        0 0 12px rgba(56, 189, 248, 0.08);
                    }
                    .key-item.updated-flash {
                      animation: highlightFlash 1.2s var(--spring-easing);
                    }
                    .key-info {
                      display: flex;
                      align-items: center;
                      gap: 12px;
                      overflow: hidden;
                      cursor: pointer;
                      flex: 1;
                    }
                    .key-tag {
                      font-family: ui-monospace, "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
                      font-size: 0.88rem;
                      font-weight: 600;
                      color: var(--accent-cyan);
                      overflow: hidden;
                      text-overflow: ellipsis;
                      white-space: nowrap;
                    }
                    .key-item:hover .key-tag {
                      text-decoration: underline;
                      text-underline-offset: 3px;
                    }
                    .key-actions {
                      display: flex;
                      align-items: center;
                      gap: 6px;
                      flex-shrink: 0;
                    }

                    /* Empty State */
                    .empty-state {
                      padding: 40px 20px;
                      text-align: center;
                      display: flex;
                      flex-direction: column;
                      align-items: center;
                      gap: 12px;
                      color: var(--text-muted);
                    }
                    .empty-state svg {
                      color: #334155;
                      width: 48px;
                      height: 48px;
                    }
                    .empty-state p {
                      font-size: 0.92rem;
                    }

                    /* Loading Skeleton */
                    .skeleton-container {
                      display: flex;
                      flex-direction: column;
                      gap: 10px;
                    }
                    .skeleton-item {
                      height: 52px;
                      border-radius: var(--radius-inner);
                      background: linear-gradient(90deg, #121824 0%, #1a2233 50%, #121824 100%);
                      background-size: 200% 100%;
                      animation: shimmer 1.6s infinite ease-in-out;
                      border: 1px solid var(--border-subtle);
                      box-shadow: inset 2px 2px 6px var(--shadow-neu-dark);
                    }

                    /* Toast Notification */
                    .toast-container {
                      position: fixed;
                      top: 24px;
                      right: 24px;
                      z-index: 1000;
                      display: flex;
                      flex-direction: column;
                      gap: 10px;
                      pointer-events: none;
                    }
                    .toast {
                      pointer-events: auto;
                      min-width: 280px;
                      max-width: 400px;
                      padding: 14px 18px;
                      border-radius: 12px;
                      background: #141c2a;
                      border: 1px solid var(--border-subtle);
                      box-shadow:
                        8px 8px 24px rgba(0, 0, 0, 0.75),
                        -4px -4px 12px var(--shadow-neu-light);
                      color: var(--text-primary);
                      font-size: 0.88rem;
                      display: flex;
                      align-items: center;
                      gap: 12px;
                      animation: toastIn 0.35s var(--spring-easing) both;
                      transition: opacity 0.25s ease, transform 0.25s ease;
                    }
                    .toast.toast-exit {
                      opacity: 0;
                      transform: translateX(30px) scale(0.95);
                    }
                    .toast-success {
                      border-left: 4px solid var(--accent-emerald);
                    }
                    .toast-error {
                      border-left: 4px solid var(--accent-rose);
                    }
                    .toast-info {
                      border-left: 4px solid var(--accent-cyan);
                    }
                    @keyframes toastIn {
                      from { opacity: 0; transform: translateX(40px) scale(0.92); }
                      to { opacity: 1; transform: translateX(0) scale(1); }
                    }

                    /* Fluid Native Modal Dialog */
                    dialog {
                      position: fixed;
                      inset: 0;
                      margin: auto;
                      width: 90%;
                      max-width: 460px;
                      border: none;
                      padding: 0;
                      background: transparent;
                      overflow: visible;
                    }
                    dialog[open] {
                      display: flex;
                      flex-direction: column;
                    }
                    dialog::backdrop {
                      background: rgba(5, 8, 14, 0.75);
                      backdrop-filter: blur(8px);
                      -webkit-backdrop-filter: blur(8px);
                      transition: opacity 0.3s ease, overlay 0.3s ease allow-discrete, display 0.3s ease allow-discrete;
                    }
                    .modal-card {
                      background: #151c2a;
                      border: 1px solid rgba(255, 255, 255, 0.08);
                      border-radius: var(--radius-card);
                      box-shadow:
                        12px 12px 32px rgba(0, 0, 0, 0.8),
                        -6px -6px 20px var(--shadow-neu-light);
                      padding: 24px;
                      display: flex;
                      flex-direction: column;
                      gap: 18px;
                      transform: scale(1);
                      opacity: 1;
                      transition: transform 0.3s var(--spring-easing), opacity 0.25s ease;
                    }
                    dialog:not([open]) .modal-card {
                      transform: scale(0.92);
                      opacity: 0;
                    }
                    @starting-style {
                      dialog[open]::backdrop {
                        opacity: 0;
                      }
                      dialog[open] .modal-card {
                        transform: scale(0.92);
                        opacity: 0;
                      }
                    }
                    .modal-header {
                      display: flex;
                      align-items: center;
                      gap: 12px;
                    }
                    .modal-icon-danger {
                      width: 40px;
                      height: 40px;
                      border-radius: 10px;
                      background: rgba(244, 63, 94, 0.12);
                      border: 1px solid rgba(244, 63, 94, 0.3);
                      display: flex;
                      align-items: center;
                      justify-content: center;
                      color: var(--accent-rose);
                      flex-shrink: 0;
                    }
                    .modal-header h3 {
                      font-size: 1.1rem;
                      font-weight: 600;
                      color: #fff;
                    }
                    .modal-body {
                      font-size: 0.9rem;
                      color: var(--text-secondary);
                      line-height: 1.5;
                    }
                    .modal-key-chip {
                      display: inline-block;
                      margin-top: 8px;
                      padding: 6px 12px;
                      border-radius: 8px;
                      background: var(--surface-neu-inset);
                      border: 1px solid var(--border-subtle);
                      font-family: ui-monospace, "SFMono-Regular", monospace;
                      color: var(--accent-cyan);
                      font-size: 0.85rem;
                      word-break: break-all;
                    }
                    .modal-actions {
                      display: flex;
                      justify-content: flex-end;
                      gap: 10px;
                      margin-top: 6px;
                    }

                    /* Rotating animation utility */
                    .spin {
                      animation: rotate 0.8s linear infinite;
                    }
                    @keyframes rotate {
                      to { transform: rotate(360deg); }
                    }

                    /* Accessibility: Prefers reduced motion */
                    @media (prefers-reduced-motion: reduce) {
                      *, *::before, *::after {
                        animation-duration: 0.01ms !important;
                        animation-iteration-count: 1 !important;
                        transition-duration: 0.01ms !important;
                        scroll-behavior: auto !important;
                      }
                      .key-item:hover, .neu-card:hover, .stat-card:hover, .neu-btn:hover {
                        transform: none !important;
                      }
                    }
                  </style>
                </head>
                <body>
                  <div class="toast-container" id="toastContainer" aria-live="polite"></div>

                  <div class="container">
                    <!-- Brand Header -->
                    <header class="header">
                      <div class="brand">
                        <div class="brand-icon" aria-hidden="true">
                          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                            <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
                          </svg>
                        </div>
                        <div class="brand-text">
                          <h1>DistributedKVStore</h1>
                          <p>Persistent LSM-Tree & Distributed Storage Engine</p>
                        </div>
                      </div>
                      <div class="status-pill" id="healthPill">
                        <span class="status-dot"></span>
                        <span id="healthText">Connecting...</span>
                      </div>
                    </header>

                    <!-- Stats Overview -->
                    <section class="stats-grid anim-stagger-1" aria-label="Engine Statistics">
                      <div class="stat-card">
                        <span class="stat-label">Stored Keys</span>
                        <span class="stat-value" id="statKeyCount">0</span>
                        <span class="stat-meta">Live in memory & WAL</span>
                      </div>
                      <div class="stat-card">
                        <span class="stat-label">Storage Engine</span>
                        <span class="stat-value" style="font-size:1.15rem;padding-top:4px">LSM + WAL</span>
                        <span class="stat-meta">Sorted Table & Sparse Index</span>
                      </div>
                      <div class="stat-card">
                        <span class="stat-label">API Endpoints</span>
                        <span class="stat-value" style="font-size:1.15rem;padding-top:4px">REST / JSON</span>
                        <span class="stat-meta">GET · PUT · DELETE</span>
                      </div>
                      <div class="stat-card">
                        <span class="stat-label">Cluster Status</span>
                        <span class="stat-value" id="statStatus" style="font-size:1.15rem;padding-top:4px;color:var(--accent-emerald)">Active</span>
                        <span class="stat-meta" id="statLatency">HTTP / 200 OK</span>
                      </div>
                    </section>

                    <!-- Key Value Editor -->
                    <section class="neu-card anim-stagger-2" id="editorCard">
                      <div class="card-header">
                        <div class="card-title">
                          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                          </svg>
                          <span>Store or Mutate Key</span>
                        </div>
                        <span class="badge-count" id="editorMode">Create / Update</span>
                      </div>

                      <div class="input-group">
                        <label class="input-label" for="key">Key Identifier</label>
                        <input id="key" class="neu-input" placeholder="e.g. user:1001, config:timeout" autocomplete="off" spellcheck="false">
                      </div>

                      <div class="input-group">
                        <div class="input-label">
                          <label for="value">Value Payload</label>
                          <span id="charCount" style="color:var(--text-muted);font-size:0.75rem">0 chars</span>
                        </div>
                        <textarea id="value" class="neu-textarea" rows="4" placeholder="Enter string, JSON object, or binary representation..." spellcheck="false"></textarea>
                      </div>

                      <div class="btn-row">
                        <button class="neu-btn neu-btn-primary" id="btnSave" onclick="save()">
                          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path>
                            <polyline points="17 21 17 13 7 13 7 21"></polyline>
                            <polyline points="7 3 7 8 15 8"></polyline>
                          </svg>
                          <span>Save Key</span>
                        </button>
                        <button class="neu-btn" onclick="formatJson()" title="Format JSON payload">
                          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <polyline points="16 18 22 12 16 6"></polyline>
                            <polyline points="8 6 2 12 8 18"></polyline>
                          </svg>
                          <span>Format JSON</span>
                        </button>
                        <button class="neu-btn" onclick="copyValue()" title="Copy value to clipboard">
                          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                          </svg>
                          <span>Copy Value</span>
                        </button>
                        <button class="neu-btn" onclick="clearForm()" title="Reset fields">
                          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <line x1="18" y1="6" x2="6" y2="18"></line>
                            <line x1="6" y1="6" x2="18" y2="18"></line>
                          </svg>
                          <span>Clear</span>
                        </button>
                      </div>
                    </section>

                    <!-- Stored Keys Directory -->
                    <section class="neu-card anim-stagger-3">
                      <div class="card-header">
                        <div class="card-title">
                          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <line x1="8" y1="6" x2="21" y2="6"></line>
                            <line x1="8" y1="12" x2="21" y2="12"></line>
                            <line x1="8" y1="18" x2="21" y2="18"></line>
                            <line x1="3" y1="6" x2="3.01" y2="6"></line>
                            <line x1="3" y1="12" x2="3.01" y2="12"></line>
                            <line x1="3" y1="18" x2="3.01" y2="18"></line>
                          </svg>
                          <span>Stored Keys Directory</span>
                        </div>
                        <div style="display:flex;align-items:center;gap:8px">
                          <span class="badge-count" id="filteredCount">0 keys</span>
                          <button class="neu-btn neu-btn-sm" id="btnRefresh" onclick="loadKeys()" aria-label="Refresh keys list" title="Refresh">
                            <svg id="refreshIcon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                              <path d="M23 4v6h-6"></path>
                              <path d="M1 20v-6h6"></path>
                              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
                            </svg>
                          </button>
                        </div>
                      </div>

                      <div class="filter-bar">
                        <div class="search-wrap">
                          <svg class="search-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <circle cx="11" cy="11" r="8"></circle>
                            <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                          </svg>
                          <input id="searchInput" class="neu-input search-input" placeholder="Filter keys by name..." oninput="filterKeys()" spellcheck="false">
                        </div>
                      </div>

                      <!-- Skeleton Loader -->
                      <div id="keysSkeleton" class="skeleton-container" style="display:none">
                        <div class="skeleton-item"></div>
                        <div class="skeleton-item"></div>
                        <div class="skeleton-item"></div>
                      </div>

                      <!-- Keys List -->
                      <ul id="keysList" class="keys-list" role="list"></ul>

                      <!-- Empty State -->
                      <div id="emptyState" class="empty-state" style="display:none">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                          <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
                          <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
                          <line x1="6" y1="6" x2="6.01" y2="6"></line>
                          <line x1="6" y1="18" x2="6.01" y2="18"></line>
                        </svg>
                        <p id="emptyMessage">No keys found in store.</p>
                      </div>
                    </section>
                  </div>

                  <!-- Fluid Delete Confirmation Modal Dialog -->
                  <dialog id="deleteModal" aria-labelledby="modalTitle" aria-describedby="modalDesc">
                    <div class="modal-card">
                      <div class="modal-header">
                        <div class="modal-icon-danger" aria-hidden="true">
                          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
                            <line x1="12" y1="9" x2="12" y2="13"></line>
                            <line x1="12" y1="17" x2="12.01" y2="17"></line>
                          </svg>
                        </div>
                        <div>
                          <h3 id="modalTitle">Delete Key Confirmation</h3>
                          <p style="font-size:0.8rem;color:var(--text-muted)">This action will append a tombstone and purge key.</p>
                        </div>
                      </div>
                      <div class="modal-body" id="modalDesc">
                        Are you sure you want to permanently delete this key from the DistributedKVStore?
                        <div><span class="modal-key-chip" id="deleteTargetKey"></span></div>
                      </div>
                      <div class="modal-actions">
                        <button class="neu-btn" type="button" onclick="closeDeleteModal()">Cancel</button>
                        <button class="neu-btn neu-btn-danger" type="button" id="btnConfirmDelete" onclick="executeDelete()">
                          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <polyline points="3 6 5 6 21 6"></polyline>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                          </svg>
                          <span>Confirm Delete</span>
                        </button>
                      </div>
                    </div>
                  </dialog>

                  <script>
                    let allKeys = [];
                    let keyToDelete = null;

                    const statusPill = document.getElementById('healthPill');
                    const healthText = document.getElementById('healthText');
                    const statKeyCount = document.getElementById('statKeyCount');
                    const keyInput = document.getElementById('key');
                    const valInput = document.getElementById('value');
                    const charCount = document.getElementById('charCount');
                    const keysList = document.getElementById('keysList');
                    const emptyState = document.getElementById('emptyState');
                    const emptyMessage = document.getElementById('emptyMessage');
                    const keysSkeleton = document.getElementById('keysSkeleton');
                    const filteredCount = document.getElementById('filteredCount');
                    const searchInput = document.getElementById('searchInput');
                    const refreshIcon = document.getElementById('refreshIcon');
                    const deleteModal = document.getElementById('deleteModal');
                    const deleteTargetKey = document.getElementById('deleteTargetKey');
                    const toastContainer = document.getElementById('toastContainer');

                    valInput.addEventListener('input', () => {
                      charCount.textContent = valInput.value.length + ' chars';
                    });

                    function showToast(message, type = 'info') {
                      const toast = document.createElement('div');
                      toast.className = 'toast toast-' + type;
                      const icon = type === 'success'
                        ? '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--accent-emerald)"><polyline points="20 6 9 17 4 12"></polyline></svg>'
                        : type === 'error'
                        ? '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--accent-rose)"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>'
                        : '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--accent-cyan)"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>';
                      toast.innerHTML = icon + '<span style="flex:1">' + escapeHtml(message) + '</span>';
                      toastContainer.appendChild(toast);

                      setTimeout(() => {
                        toast.classList.add('toast-exit');
                        setTimeout(() => toast.remove(), 250);
                      }, 3200);
                    }

                    function escapeHtml(str) {
                      const div = document.createElement('div');
                      div.textContent = str;
                      return div.innerHTML;
                    }

                    function copyToClipboard(text) {
                      if (navigator.clipboard && window.isSecureContext) {
                        return navigator.clipboard.writeText(text);
                      }
                      return new Promise((resolve, reject) => {
                        try {
                          const ta = document.createElement('textarea');
                          ta.value = text;
                          ta.style.position = 'fixed';
                          ta.style.left = '-999999px';
                          ta.style.top = '-999999px';
                          document.body.appendChild(ta);
                          ta.focus();
                          ta.select();
                          const ok = document.execCommand('copy');
                          ta.remove();
                          if (ok) resolve(); else reject(new Error('Copy failed'));
                        } catch (e) {
                          reject(e);
                        }
                      });
                    }

                    async function updateHealth() {
                      try {
                        const start = performance.now();
                        const r = await fetch('/api/health');
                        const data = await r.json();
                        const ms = Math.round(performance.now() - start);
                        if (r.ok && data.status === 'ok') {
                          healthText.textContent = 'Cluster Online (' + data.keys + ' keys)';
                          statKeyCount.textContent = data.keys;
                          document.getElementById('statLatency').textContent = ms + 'ms response';
                        }
                      } catch (e) {
                        healthText.textContent = 'Cluster Unreachable';
                        document.querySelector('.status-dot').style.background = 'var(--accent-rose)';
                        document.querySelector('.status-dot').style.boxShadow = '0 0 8px var(--accent-rose)';
                      }
                    }

                    async function loadKeys() {
                      refreshIcon.classList.add('spin');
                      keysSkeleton.style.display = 'flex';
                      keysList.style.display = 'none';
                      emptyState.style.display = 'none';

                      try {
                        const r = await fetch('/api/keys');
                        if (!r.ok) throw new Error(await r.text());
                        allKeys = await r.json();
                        statKeyCount.textContent = allKeys.length;
                        filterKeys();
                        await updateHealth();
                      } catch (e) {
                        showToast('Failed to load keys: ' + e.message, 'error');
                      } finally {
                        keysSkeleton.style.display = 'none';
                        keysList.style.display = 'flex';
                        refreshIcon.classList.remove('spin');
                      }
                    }

                    function filterKeys() {
                      const q = (searchInput.value || '').trim().toLowerCase();
                      const filtered = allKeys.filter(k => k.toLowerCase().includes(q));
                      filteredCount.textContent = filtered.length + ' / ' + allKeys.length + ' key(s)';

                      keysList.replaceChildren();

                      if (filtered.length === 0) {
                        emptyState.style.display = 'flex';
                        emptyMessage.textContent = q ? 'No keys matching "' + q + '"' : 'Store is currently empty.';
                        return;
                      }

                      emptyState.style.display = 'none';

                      filtered.forEach((k, index) => {
                        const li = document.createElement('li');
                        li.className = 'key-item';
                        li.id = 'key-row-' + encodeURIComponent(k);

                        const info = document.createElement('div');
                        info.className = 'key-info';
                        info.title = 'Click to inspect or edit ' + k;
                        info.onclick = () => readKey(k);

                        const tag = document.createElement('span');
                        tag.className = 'key-tag';
                        tag.textContent = k;

                        info.appendChild(tag);

                        const actions = document.createElement('div');
                        actions.className = 'key-actions';

                        // Inspect/Load button
                        const btnEdit = document.createElement('button');
                        btnEdit.className = 'neu-btn neu-btn-sm';
                        btnEdit.title = 'Inspect or Edit key';
                        btnEdit.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg> <span>Edit</span>';
                        btnEdit.onclick = (e) => { e.stopPropagation(); readKey(k); };

                        // Copy button
                        const btnCopy = document.createElement('button');
                        btnCopy.className = 'neu-btn neu-btn-sm neu-btn-icon-only';
                        btnCopy.title = 'Copy key name';
                        btnCopy.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
                        btnCopy.onclick = (e) => {
                          e.stopPropagation();
                          copyToClipboard(k).then(() => {
                            btnCopy.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--accent-emerald)"><polyline points="20 6 9 17 4 12"></polyline></svg>';
                            showToast('Key "' + k + '" copied to clipboard', 'info');
                            setTimeout(() => {
                              btnCopy.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
                            }, 1500);
                          }).catch((err) => {
                            showToast('Failed to copy: ' + err.message, 'error');
                          });
                        };

                        // Delete button
                        const btnDel = document.createElement('button');
                        btnDel.className = 'neu-btn neu-btn-sm neu-btn-danger';
                        btnDel.title = 'Delete key';
                        btnDel.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path></svg>';
                        btnDel.onclick = (e) => {
                          e.stopPropagation();
                          openDeleteModal(k);
                        };

                        actions.append(btnEdit, btnCopy, btnDel);
                        li.append(info, actions);
                        keysList.append(li);
                      });
                    }

                    async function save() {
                      const k = keyInput.value.trim();
                      const v = valInput.value;
                      if (!k) {
                        keyInput.focus();
                        showToast('Please specify a key identifier', 'error');
                        return;
                      }

                      const btnSave = document.getElementById('btnSave');
                      btnSave.disabled = true;
                      btnSave.style.opacity = '0.7';

                      try {
                        const r = await fetch('/api/keys/' + encodeURIComponent(k), {
                          method: 'PUT',
                          headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
                          body: v
                        });
                        const data = await r.text();
                        if (!r.ok) throw new Error(data);
                        showToast('Key "' + k + '" stored successfully', 'success');
                        await loadKeys();

                        // Flash highlight the updated/created row
                        const row = document.getElementById('key-row-' + encodeURIComponent(k));
                        if (row) {
                          row.classList.add('updated-flash');
                          setTimeout(() => row.classList.remove('updated-flash'), 1200);
                        }
                      } catch (e) {
                        showToast('Save failed: ' + e.message, 'error');
                      } finally {
                        btnSave.disabled = false;
                        btnSave.style.opacity = '1';
                      }
                    }

                    async function readKey(k) {
                      try {
                        const r = await fetch('/api/keys/' + encodeURIComponent(k));
                        const data = await r.json();
                        if (!r.ok) throw new Error(data.error || 'Key not found');

                        keyInput.value = data.key;
                        valInput.value = data.value;
                        charCount.textContent = data.value.length + ' chars';

                        // Scroll smoothly to editor
                        document.getElementById('editorCard').scrollIntoView({ behavior: 'smooth', block: 'start' });
                        keyInput.focus();

                        const editor = document.getElementById('editorCard');
                        editor.style.borderColor = 'var(--accent-cyan)';
                        setTimeout(() => editor.style.borderColor = '', 1000);

                        showToast('Loaded key: ' + k, 'info');
                      } catch (e) {
                        showToast('Failed to load key: ' + e.message, 'error');
                      }
                    }

                    function openDeleteModal(k) {
                      keyToDelete = k;
                      deleteTargetKey.textContent = k;
                      if (typeof deleteModal.showModal === 'function') {
                        deleteModal.showModal();
                      } else {
                        deleteModal.setAttribute('open', '');
                      }
                    }

                    function closeDeleteModal() {
                      keyToDelete = null;
                      if (typeof deleteModal.close === 'function') {
                        deleteModal.close();
                      } else {
                        deleteModal.removeAttribute('open');
                      }
                    }

                    async function executeDelete() {
                      if (!keyToDelete) return;
                      const k = keyToDelete;
                      const btn = document.getElementById('btnConfirmDelete');
                      btn.disabled = true;

                      try {
                        const r = await fetch('/api/keys/' + encodeURIComponent(k), { method: 'DELETE' });
                        const data = await r.json();
                        if (!r.ok) throw new Error(data.error || 'Delete failed');

                        closeDeleteModal();
                        showToast('Key "' + k + '" deleted', 'success');

                        if (keyInput.value === k) {
                          clearForm();
                        }

                        await loadKeys();
                      } catch (e) {
                        showToast('Delete error: ' + e.message, 'error');
                      } finally {
                        btn.disabled = false;
                      }
                    }

                    function clearForm() {
                      keyInput.value = '';
                      valInput.value = '';
                      charCount.textContent = '0 chars';
                      keyInput.focus();
                    }

                    function formatJson() {
                      try {
                        const raw = valInput.value.trim();
                        if (!raw) return;
                        const parsed = JSON.parse(raw);
                        valInput.value = JSON.stringify(parsed, null, 2);
                        charCount.textContent = valInput.value.length + ' chars';
                        showToast('JSON formatted', 'info');
                      } catch (e) {
                        showToast('Invalid JSON: ' + e.message, 'error');
                      }
                    }

                    function copyValue() {
                      const v = valInput.value;
                      if (!v) {
                        showToast('Value payload is empty', 'info');
                        return;
                      }
                      copyToClipboard(v).then(() => {
                        showToast('Value copied to clipboard (' + v.length + ' chars)', 'info');
                      }).catch((err) => {
                        showToast('Failed to copy value: ' + err.message, 'error');
                      });
                    }

                    // Dialog Backdrop and Escape Listener
                    deleteModal.addEventListener('click', (e) => {
                      if (e.target === deleteModal) {
                        closeDeleteModal();
                      }
                    });
                    deleteModal.addEventListener('cancel', () => {
                      closeDeleteModal();
                    });
                    document.addEventListener('keydown', (e) => {
                      if (e.key === 'Escape' && deleteModal.open) {
                        closeDeleteModal();
                      }
                    });

                    // Keyboard shortcuts: Enter in keyInput moves to value, Ctrl/Cmd+Enter saves
                    keyInput.addEventListener('keydown', (e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        valInput.focus();
                      }
                    });

                    valInput.addEventListener('keydown', (e) => {
                      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                        e.preventDefault();
                        save();
                      }
                    });

                    // Initial boot
                    loadKeys();
                    setInterval(updateHealth, 15000);
                  </script>
                </body>
                </html>
                """;
        send(exchange, 200, "text/html; charset=utf-8", html);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\b", "\\b")
                .replace("\f", "\\f");
    }

    private static void send(HttpExchange exchange, int status,
                             String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    @Override
    public void close() {
        try {
            server.stop(1);
        } finally {
            httpExecutor.shutdown();
            try {
                if (!httpExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
                    httpExecutor.shutdownNow();
                }
            } catch (InterruptedException interrupted) {
                httpExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
