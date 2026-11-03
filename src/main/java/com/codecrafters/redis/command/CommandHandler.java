
package com.codecrafters.redis.command;

import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.storage.KeyValueAccess;
import com.codecrafters.redis.storage.AsyncWriteBatcher;
import com.codecrafters.redis.raft.RaftGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Base64;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.concurrent.ExecutionException;

/**
 * Executes Redis-compatible commands for DistributedKVStore.
 */
public class CommandHandler {

    private static final String BINARY_VALUE_PREFIX = "\u0000KV-BINARY:";
    private static final String ESCAPED_VALUE_PREFIX = "\u0000KV-STRING:";
    private record CommandInfo(String name, int arity, List<String> flags, int firstKey, int lastKey, int keyStep,
                               List<String> aclCategories) { }
    private static final List<CommandInfo> COMMANDS = List.of(
            command("ping", -1, List.of("fast", "connection"), 0, 0, 0, "@fast", "@connection"),
            command("echo", 2, List.of("fast"), 0, 0, 0, "@fast"),
            command("get", 2, List.of("readonly", "fast"), 1, 1, 1, "@read", "@string"),
            command("set", -3, List.of("write", "denyoom"), 1, 1, 1, "@write", "@string"),
            command("put", 3, List.of("write", "denyoom"), 1, 1, 1, "@write", "@string"),
            command("del", -2, List.of("write"), 1, -1, 1, "@write", "@keyspace"),
            command("delete", -2, List.of("write"), 1, -1, 1, "@write", "@keyspace"),
            command("exists", -2, List.of("readonly", "fast"), 1, -1, 1, "@read", "@keyspace"),
            command("keys", 2, List.of("readonly", "sort_for_script"), 0, 0, 0, "@read", "@keyspace", "@dangerous"),
            command("command", -1, List.of("loading", "stale", "only", "no_async_loading", "allow_busy"), 0, 0, 0, "@slow", "@connection"),
            command("config", -2, List.of("admin", "noscript"), 0, 0, 0, "@slow", "@admin"),
            command("info", -1, List.of("loading", "stale", "fast"), 0, 0, 0, "@slow", "@dangerous"),
            command("auth", -2, List.of("no_auth", "loading", "stale", "fast", "connection"), 0, 0, 0, "@fast", "@connection"));

    private final RedisStore store;
    private final KeyValueAccess access;
    private final AsyncWriteBatcher writeBatcher;
    private final RaftGroup raftGroup;
    private final boolean followerReads;
    private final int tcpPort;
    private final IntSupplier connectedClients;
    private final long startedAtMillis = System.currentTimeMillis();
    private final AtomicLong commandsProcessed = new AtomicLong();

    public CommandHandler(RedisStore store) {
        this(store, null);
    }

    public CommandHandler(RedisStore store, AsyncWriteBatcher writeBatcher) {
        this(store, writeBatcher, null, false);
    }

    public CommandHandler(RedisStore store, AsyncWriteBatcher writeBatcher,
                          RaftGroup raftGroup, boolean followerReads) {
        this(store, store, writeBatcher, raftGroup, followerReads);
    }

    public CommandHandler(RedisStore store, KeyValueAccess access, AsyncWriteBatcher writeBatcher,
                          RaftGroup raftGroup, boolean followerReads) {
        this(store, access, writeBatcher, raftGroup, followerReads, 6379, () -> 0);
    }

    public CommandHandler(RedisStore store, KeyValueAccess access, AsyncWriteBatcher writeBatcher,
                          RaftGroup raftGroup, boolean followerReads, int tcpPort, IntSupplier connectedClients) {
        this.store = store;
        this.access = access;
        this.writeBatcher = writeBatcher;
        this.raftGroup = raftGroup;
        this.followerReads = followerReads;
        this.tcpPort = tcpPort;
        this.connectedClients = connectedClients;
    }

    public RespValue execute(Command command) {
        if (command == null || command.getName().isEmpty()) {
            return RespValue.error("ERR unknown command ''");
        }

        String name = command.getName().toUpperCase(Locale.ROOT);
        commandsProcessed.incrementAndGet();

        switch (name) {
            case "PING":
                return handlePing(command);

            case "ECHO":
                return handleEcho(command);

            case "SET":
                return handleSet(command);

            case "PUT":
                return handlePut(command);

            case "GET":
                return handleGet(command);

            case "DEL":
            case "DELETE":
                return handleDel(command);

            case "EXISTS":
                return handleExists(command);

            case "KEYS":
                return handleKeys(command);

            case "COMMAND":
                return handleCommand(command);

            case "INFO":
                return handleInfo(command);

            case "CONFIG":
                return handleConfig(command);

            default:
                return RespValue.error(
                    "ERR unknown command '" + name.toLowerCase(Locale.ROOT) + "'"
                );
        }
    }

    private RespValue handlePing(Command command) {
        if (command.getArgCount() == 0) {
            return RespValue.simpleString("PONG");
        }

        if (command.getArgCount() == 1) {
            return RespValue.bulkString(command.getArg(0));
        }

        return RespValue.error(
            "ERR wrong number of arguments for 'ping' command"
        );
    }

    private RespValue handleEcho(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'echo' command"
            );
        }

        return RespValue.bulkString(command.getArgBytes(0));
    }

    /**
     * PUT key value
     *
     * Uses the same WAL-backed store as SET.
     */
    private RespValue handlePut(Command command) {
        if (command.getArgCount() != 2) {
            return RespValue.error(
                "ERR wrong number of arguments for 'put' command"
            );
        }

        String key = command.getArg(0);
        String value = storeRedisBytes(command.getArgBytes(1));

        if (!write(key, value)) return RespValue.error("ERR write failed");

        return RespValue.simpleString("OK");
    }

    /**
     * SET key value [PX milliseconds | EX seconds]
     */
    private RespValue handleSet(Command command) {
        if (command.getArgCount() < 2) {
            return RespValue.error(
                "ERR wrong number of arguments for 'set' command"
            );
        }

        String key = command.getArg(0);
        String value = storeRedisBytes(command.getArgBytes(1));
        Long expiryMillis = null;

        for (int i = 2; i < command.getArgCount(); i++) {
            String option = command.getArg(i).toUpperCase(Locale.ROOT);

            if ("PX".equals(option)) {
                if (expiryMillis != null || i + 1 >= command.getArgCount()) {
                    return RespValue.error("ERR syntax error");
                }

                try {
                    long px = Long.parseLong(command.getArg(++i));

                    if (px <= 0) {
                        return RespValue.error(
                            "ERR invalid expire time in 'set' command"
                        );
                    }

                    expiryMillis = px;
                } catch (NumberFormatException e) {
                    return RespValue.error(
                        "ERR value is not an integer or out of range"
                    );
                }

            } else if ("EX".equals(option)) {
                if (expiryMillis != null || i + 1 >= command.getArgCount()) {
                    return RespValue.error("ERR syntax error");
                }

                try {
                    long seconds = Long.parseLong(command.getArg(++i));

                    if (seconds <= 0 || seconds > Long.MAX_VALUE / 1000L) {
                        return RespValue.error(
                            "ERR invalid expire time in 'set' command"
                        );
                    }

                    expiryMillis = seconds * 1000L;
                } catch (NumberFormatException e) {
                    return RespValue.error(
                        "ERR value is not an integer or out of range"
                    );
                }

            } else {
                return RespValue.error("ERR syntax error");
            }
        }

        if (expiryMillis == null) {
            if (!write(key, value)) return RespValue.error("ERR write failed");
        } else {
            try {
                if (raftGroup == null) access.set(key, value, expiryMillis);
                else raftGroup.setWithExpiry(key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8), expiryMillis);
            } catch (IOException e) {
                return RespValue.error("ERR replicated write failed: " + e.getMessage());
            } catch (RuntimeException e) {
                return RespValue.error("ERR write failed");
            }
        }

        return RespValue.simpleString("OK");
    }

    private boolean write(String key, String value) {
        if (raftGroup != null) {
            try { raftGroup.put(key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8)); return true; }
            catch (IOException | RuntimeException e) { return false; }
        }
        if (writeBatcher == null) {
            try { store.set(key, value); return true; }
            catch (RuntimeException e) { return false; }
        }
        try {
            writeBatcher.submit(key, value).get();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | RuntimeException e) {
            return false;
        }
    }

    private RespValue handleGet(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'get' command"
            );
        }

        String value;
        try { value = read(command.getArg(0)); }
        catch (IOException e) { return RespValue.error("ERR replicated read failed: " + e.getMessage()); }

        if (value == null) {
            return RespValue.nullBulkString();
        }

        return RespValue.bulkString(redisBytes(value));
    }

    /**
     * Supports DEL key [key ...] and DELETE key [key ...].
     */
    private RespValue handleDel(Command command) {
        if (command.getArgCount() < 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'del' command"
            );
        }

        String[] keys = command.getArgs().toArray(new String[0]);
        if (raftGroup == null) return RespValue.integer(access.delete(keys));
        int deleted;
        try { deleted = raftGroup.deleteBatch(keys); }
        catch (IOException e) { return RespValue.error("ERR replicated delete failed: " + e.getMessage()); }
        return RespValue.integer(deleted);
    }

    private RespValue handleExists(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'exists' command"
            );
        }

        try { return RespValue.integer(read(command.getArg(0)) != null ? 1 : 0); }
        catch (IOException e) { return RespValue.error("ERR replicated read failed: " + e.getMessage()); }
    }

    private RespValue handleKeys(Command command) {
        if (command.getArgCount() > 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'keys' command"
            );
        }

        String pattern = command.getArgCount() == 0
            ? "*"
            : command.getArg(0);

        List<RespValue> elements = new ArrayList<>();

        List<String> keys;
        try {
            if (raftGroup == null) keys = access.keys(pattern);
            else if (followerReads && !raftGroup.isLeader()) keys = raftGroup.keysFollowerRead(pattern);
            else keys = raftGroup.keysWithReadIndex(pattern);
        } catch (IOException e) { return RespValue.error("ERR replicated read failed: " + e.getMessage()); }
        for (String key : keys) {
            elements.add(RespValue.bulkString(key));
        }

        return RespValue.array(elements);
    }

    private String read(String key) throws IOException {
        if (raftGroup == null) return access.get(key);
        byte[] value = followerReads && !raftGroup.isLeader()
                ? raftGroup.getFollowerRead(key) : raftGroup.getWithReadIndex(key);
        return value == null ? null : new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private RespValue handleCommand(Command command) {
        if (command.getArgCount() == 0) return commandInfos(COMMANDS);
        String subcommand = command.getArg(0).toUpperCase(Locale.ROOT);
        if ("COUNT".equals(subcommand) && command.getArgCount() == 1) return RespValue.integer(COMMANDS.size());
        if ("LIST".equals(subcommand) && command.getArgCount() == 1)
            return RespValue.array(COMMANDS.stream().map(info -> RespValue.bulkString(info.name())).toList());
        if ("INFO".equals(subcommand) && command.getArgCount() >= 2) {
            List<RespValue> results = new ArrayList<>();
            for (int i = 1; i < command.getArgCount(); i++) {
                String requested = command.getArg(i).toLowerCase(Locale.ROOT);
                CommandInfo info = COMMANDS.stream().filter(candidate -> candidate.name().equals(requested)).findFirst().orElse(null);
                results.add(info == null ? RespValue.nullBulkString() : commandInfo(info));
            }
            return RespValue.array(results);
        }
        return RespValue.error("ERR unknown subcommand or wrong number of arguments for 'COMMAND'");
    }

    private RespValue handleInfo(Command command) {
        if (command.getArgCount() > 1) return RespValue.error("ERR wrong number of arguments for 'INFO' command");
        String requested = command.getArgCount() == 0 ? "all" : command.getArg(0).toLowerCase(Locale.ROOT);
        StringBuilder info = new StringBuilder();
        if ("all".equals(requested) || "default".equals(requested) || "server".equals(requested)) {
            info.append("# Server\r\nserver_name:DistributedKVStore\r\nserver_version:1.0.0\r\n")
                    .append("os:").append(System.getProperty("os.name", "unknown")).append('\r').append('\n')
                    .append("arch_bits:").append(System.getProperty("sun.arch.data.model", "unknown")).append("\r\n")
                    .append("process_id:").append(ProcessHandle.current().pid()).append("\r\n")
                    .append("tcp_port:").append(tcpPort).append("\r\n")
                    .append("uptime_in_seconds:").append(Math.max(0, (System.currentTimeMillis() - startedAtMillis) / 1000)).append("\r\n\r\n");
        }
        if ("all".equals(requested) || "default".equals(requested) || "clients".equals(requested))
            info.append("# Clients\r\nconnected_clients:").append(Math.max(0, connectedClients.getAsInt()))
                    .append("\r\n\r\n");
        if ("all".equals(requested) || "default".equals(requested) || "memory".equals(requested)) {
            Runtime runtime = Runtime.getRuntime();
            info.append("# Memory\r\nused_memory:").append(runtime.totalMemory() - runtime.freeMemory())
                    .append("\r\nused_memory_peak:").append(runtime.totalMemory())
                    .append("\r\nmaxmemory:0\r\n\r\n");
        }
        if ("all".equals(requested) || "default".equals(requested) || "stats".equals(requested))
            info.append("# Stats\r\ntotal_commands_processed:").append(commandsProcessed.get())
                    .append("\r\n\r\n");
        if ("all".equals(requested) || "default".equals(requested) || "replication".equals(requested))
            info.append("# Replication\r\nrole:").append(raftGroup == null || raftGroup.isLeader() ? "master" : "slave")
                    .append("\r\n\r\n");
        if ("all".equals(requested) || "default".equals(requested) || "keyspace".equals(requested)) {
            RedisStore.KeyspaceStats stats = store.keyspaceStats();
            if (stats.keys() > 0) info.append("# Keyspace\r\ndb0:keys=").append(stats.keys())
                    .append(",expires=").append(stats.expiringKeys())
                    .append(",avg_ttl=").append(stats.averageTtlMillis()).append("\r\n");
        }
        if (info.length() == 0 && !List.of("all", "default", "server", "clients", "memory", "stats", "replication", "keyspace").contains(requested))
            return RespValue.error("ERR invalid INFO section");
        return RespValue.bulkString(info.toString());
    }

    private RespValue handleConfig(Command command) {
        if (command.getArgCount() == 2 && "GET".equalsIgnoreCase(command.getArg(0))) {
            Map<String, String> values = new LinkedHashMap<>();
            values.put("port", Integer.toString(tcpPort));
            values.put("bind", System.getenv().getOrDefault("KV_BIND_HOST", "127.0.0.1"));
            values.put("databases", "1");
            values.put("timeout", System.getProperty("redis.client.readTimeoutMillis", "30000"));
            values.put("maxmemory", "0");
            values.put("maxmemory-policy", "noeviction");
            try { values.put("appendonly", store.storageUsage().walEnabled() ? "yes" : "no"); }
            catch (IOException e) { values.put("appendonly", "unknown"); }
            String pattern = command.getArg(1).toLowerCase(Locale.ROOT);
            List<RespValue> result = new ArrayList<>();
            values.forEach((key, value) -> {
                if (matchesGlob(pattern, key)) { result.add(RespValue.bulkString(key)); result.add(RespValue.bulkString(value)); }
            });
            return RespValue.array(result);
        }
        if (command.getArgCount() == 1 && "RESETSTAT".equalsIgnoreCase(command.getArg(0))) {
            commandsProcessed.set(0);
            return RespValue.simpleString("OK");
        }
        return RespValue.error("ERR unsupported CONFIG subcommand or invalid arguments");
    }

    private static CommandInfo command(String name, int arity, List<String> flags, int first, int last, int step, String... acl) {
        return new CommandInfo(name, arity, flags, first, last, step, List.of(acl));
    }

    private static RespValue commandInfos(List<CommandInfo> infos) {
        return RespValue.array(infos.stream().map(CommandHandler::commandInfo).toList());
    }

    private static RespValue commandInfo(CommandInfo info) {
        List<RespValue> fields = new ArrayList<>();
        fields.add(RespValue.bulkString(info.name())); fields.add(RespValue.integer(info.arity()));
        fields.add(RespValue.array(info.flags().stream().map(RespValue::bulkString).toList()));
        fields.add(RespValue.integer(info.firstKey())); fields.add(RespValue.integer(info.lastKey()));
        fields.add(RespValue.integer(info.keyStep()));
        fields.add(RespValue.array(info.aclCategories().stream().map(RespValue::bulkString).toList()));
        fields.add(RespValue.array(List.of())); fields.add(RespValue.array(List.of())); fields.add(RespValue.array(List.of()));
        return RespValue.array(fields);
    }

    private static boolean matchesGlob(String pattern, String value) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '*') regex.append(".*"); else if (c == '?') regex.append('.');
            else regex.append(java.util.regex.Pattern.quote(String.valueOf(c)));
        }
        return value.matches(regex.append('$').toString());
    }

    private static String storeRedisBytes(byte[] bytes) {
        try {
            String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            return decoded.startsWith(BINARY_VALUE_PREFIX) || decoded.startsWith(ESCAPED_VALUE_PREFIX)
                    ? ESCAPED_VALUE_PREFIX + decoded : decoded;
        } catch (CharacterCodingException malformedUtf8) {
            return BINARY_VALUE_PREFIX + Base64.getEncoder().encodeToString(bytes);
        }
    }

    private static byte[] redisBytes(String stored) {
        if (stored.startsWith(ESCAPED_VALUE_PREFIX))
            return stored.substring(ESCAPED_VALUE_PREFIX.length()).getBytes(StandardCharsets.UTF_8);
        if (stored.startsWith(BINARY_VALUE_PREFIX)) {
            try { return Base64.getDecoder().decode(stored.substring(BINARY_VALUE_PREFIX.length())); }
            catch (IllegalArgumentException malformedMarker) { /* Treat an ordinary string as UTF-8. */ }
        }
        return stored.getBytes(StandardCharsets.UTF_8);
    }
}
