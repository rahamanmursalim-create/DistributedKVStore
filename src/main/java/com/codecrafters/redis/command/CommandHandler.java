
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
import java.util.concurrent.ExecutionException;

/**
 * Executes Redis-compatible commands for DistributedKVStore.
 */
public class CommandHandler {

    private final RedisStore store;
    private final KeyValueAccess access;
    private final AsyncWriteBatcher writeBatcher;
    private final RaftGroup raftGroup;
    private final boolean followerReads;

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
        this.store = store;
        this.access = access;
        this.writeBatcher = writeBatcher;
        this.raftGroup = raftGroup;
        this.followerReads = followerReads;
    }

    public RespValue execute(Command command) {
        if (command == null || command.getName().isEmpty()) {
            return RespValue.error("ERR unknown command ''");
        }

        String name = command.getName().toUpperCase(Locale.ROOT);

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
                return handleCommand();

            case "INFO":
                return handleInfo();

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

        return RespValue.bulkString(command.getArg(0));
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
        String value = command.getArg(1);

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
        String value = command.getArg(1);
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

        return RespValue.bulkString(value);
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

    private RespValue handleCommand() {
        return RespValue.array(new ArrayList<>());
    }

    private RespValue handleInfo() {
        String info =
            "# Server\r\n" +
            "redis_version:7.0.0\r\n" +
            "role:master\r\n";

        return RespValue.bulkString(info);
    }

    private RespValue handleConfig(Command command) {
        if (command.getArgCount() >= 2
                && "GET".equalsIgnoreCase(command.getArg(0))) {

            String parameter = command.getArg(1);
            List<RespValue> result = new ArrayList<>();

            result.add(RespValue.bulkString(parameter));
            result.add(RespValue.bulkString(""));

            return RespValue.array(result);
        }

        return RespValue.array(new ArrayList<>());
    }
}
