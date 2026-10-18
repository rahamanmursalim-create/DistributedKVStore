package com.codecrafters.redis.command;

import com.codecrafters.redis.ReplicationMode;
import com.codecrafters.redis.distributed.ReplicationClient;
import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class CommandHandler implements AutoCloseable {

    private final StorageEngine store;
    private final ReplicationClient replicationClient;
    private final ReplicationMode replicationMode;

    private final BlockingQueue<ReplicationEvent> replicationQueue;
    private final Thread asyncWorkerThread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    /**
     * Standalone mode: no remote replication.
     */
    public CommandHandler(StorageEngine store) {
        this(store, null, ReplicationMode.SYNC);
    }

    /**
     * Replication-enabled mode: writes are sent to the configured replica synchronously.
     */
    public CommandHandler(
            StorageEngine store,
            ReplicationClient replicationClient) {
        this(store, replicationClient, ReplicationMode.SYNC);
    }

    /**
     * Configures command handling with specified replication mode (SYNC or ASYNC).
     */
    public CommandHandler(
            StorageEngine store,
            ReplicationClient replicationClient,
            ReplicationMode replicationMode) {

        if (store == null) {
            throw new IllegalArgumentException(
                    "Storage engine cannot be null");
        }

        this.store = store;
        this.replicationClient = replicationClient;
        this.replicationMode = replicationMode != null ? replicationMode : ReplicationMode.SYNC;

        if (this.replicationMode == ReplicationMode.ASYNC && this.replicationClient != null) {
            this.replicationQueue = new LinkedBlockingQueue<>();
            this.asyncWorkerThread = new Thread(this::runAsyncWorker, "replication-async-worker");
            this.asyncWorkerThread.setDaemon(true);
            this.asyncWorkerThread.start();
        } else {
            this.replicationQueue = null;
            this.asyncWorkerThread = null;
        }
    }

    public ReplicationMode getReplicationMode() {
        return replicationMode;
    }

    public int getPendingReplicationCount() {
        return pendingCount.get();
    }

    public int getReplicationLag() {
        return pendingCount.get();
    }

    public RespValue execute(Command command) {
        if (command == null || command.getName().isEmpty()) {
            return RespValue.error("ERR unknown command ''");
        }

        String name = command.getName().toUpperCase(Locale.ROOT);

        try {
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
                    return RespValue.array(new ArrayList<>());
                case "INFO":
                    return RespValue.bulkString(
                            "# Server\r\nredis_version:7.0.0\r\nrole:master\r\n");
                case "CONFIG":
                    return handleConfig(command);
                default:
                    return RespValue.error(
                            "ERR unknown command '" +
                            name.toLowerCase(Locale.ROOT) + "'");
            }
        } catch (IOException e) {
            return RespValue.error(
                    "ERR storage or replication operation failed");
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
                "ERR wrong number of arguments for 'ping' command");
    }

    private RespValue handleEcho(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'echo' command");
        }
        return RespValue.bulkString(command.getArg(0));
    }

    private RespValue handlePut(Command command) throws IOException {
        if (command.getArgCount() != 2) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'put' command");
        }

        String key = command.getArg(0);
        String value = command.getArg(1);

        store.put(key, value);
        replicatePut(key, value, -1L);

        return RespValue.simpleString("OK");
    }

    private RespValue handleSet(Command command) throws IOException {
        if (command.getArgCount() < 2) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'set' command");
        }

        String key = command.getArg(0);
        String value = command.getArg(1);
        Long durationMillis = null;

        for (int i = 2; i < command.getArgCount(); i++) {
            String option = command.getArg(i).toUpperCase(Locale.ROOT);

            if (!option.equals("EX") && !option.equals("PX")) {
                return RespValue.error("ERR syntax error");
            }

            if (durationMillis != null || i + 1 >= command.getArgCount()) {
                return RespValue.error("ERR syntax error");
            }

            try {
                long duration = Long.parseLong(command.getArg(++i));

                if (duration <= 0) {
                    return RespValue.error(
                            "ERR invalid expire time in 'set' command");
                }

                if (option.equals("EX")) {
                    duration = Math.multiplyExact(duration, 1000L);
                }

                durationMillis = duration;
            } catch (NumberFormatException | ArithmeticException e) {
                return RespValue.error(
                        "ERR value is not an integer or out of range");
            }
        }

        long expiryEpochMillis = -1L;

        if (durationMillis != null) {
            try {
                expiryEpochMillis = Math.addExact(
                        System.currentTimeMillis(), durationMillis);
            } catch (ArithmeticException e) {
                return RespValue.error(
                        "ERR value is not an integer or out of range");
            }
        }

        store.put(key, value, expiryEpochMillis);
        replicatePut(key, value, expiryEpochMillis);

        return RespValue.simpleString("OK");
    }

    private RespValue handleGet(Command command) throws IOException {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'get' command");
        }

        String value = store.get(command.getArg(0));
        return value == null
                ? RespValue.nullBulkString()
                : RespValue.bulkString(value);
    }

    private RespValue handleDel(Command command) throws IOException {
        if (command.getArgCount() < 1) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'del' command");
        }

        int deleted = 0;

        for (String key : command.getArgs()) {
            if (store.delete(key)) {
                deleted++;
            }

            replicateDelete(key);
        }

        return RespValue.integer(deleted);
    }

    private void replicatePut(
            String key,
            String value,
            long expiryEpochMillis) throws IOException {

        if (replicationClient == null) {
            return;
        }

        if (replicationMode == ReplicationMode.SYNC) {
            replicationClient.replicatePut(key, value, expiryEpochMillis);
        } else {
            pendingCount.incrementAndGet();
            replicationQueue.offer(new ReplicationEvent(false, key, value, expiryEpochMillis));
        }
    }

    private void replicateDelete(String key) throws IOException {
        if (replicationClient == null) {
            return;
        }

        if (replicationMode == ReplicationMode.SYNC) {
            replicationClient.replicateDelete(key);
        } else {
            pendingCount.incrementAndGet();
            replicationQueue.offer(new ReplicationEvent(true, key, null, -1L));
        }
    }

    private void runAsyncWorker() {
        while (running.get() || !replicationQueue.isEmpty()) {
            try {
                ReplicationEvent event = replicationQueue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    try {
                        if (event.isDelete) {
                            replicationClient.replicateDelete(event.key);
                        } else {
                            replicationClient.replicatePut(event.key, event.value, event.expiryEpochMillis);
                        }
                    } catch (IOException e) {
                        System.err.println("Async replication to replica failed: " + e.getMessage());
                    } finally {
                        pendingCount.decrementAndGet();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Waits until all pending replication operations have been dispatched and acknowledged,
     * or until timeout is exceeded.
     */
    public boolean flushReplicationQueue(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (pendingCount.get() > 0) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    @Override
    public void close() {
        running.set(false);
        if (asyncWorkerThread != null) {
            asyncWorkerThread.interrupt();
            try {
                asyncWorkerThread.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private RespValue handleExists(Command command) throws IOException {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'exists' command");
        }

        return RespValue.integer(
                store.get(command.getArg(0)) != null ? 1 : 0);
    }

    private RespValue handleKeys(Command command) throws IOException {
        if (command.getArgCount() > 1) {
            return RespValue.error(
                    "ERR wrong number of arguments for 'keys' command");
        }

        String glob = command.getArgCount() == 0
                ? "*" : command.getArg(0);

        Pattern pattern = globToRegex(glob);
        List<RespValue> result = new ArrayList<>();

        for (Map.Entry<String, String> entry : store.snapshot().entrySet()) {
            if (pattern.matcher(entry.getKey()).matches()) {
                result.add(RespValue.bulkString(entry.getKey()));
            }
        }

        return RespValue.array(result);
    }

    private static Pattern globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");

        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);

            if (c == '*') {
                regex.append(".*");
            } else if (c == '?') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }

        regex.append('$');
        return Pattern.compile(regex.toString());
    }

    private RespValue handleConfig(Command command) {
        if (command.getArgCount() >= 2
                && "GET".equalsIgnoreCase(command.getArg(0))) {
            List<RespValue> result = new ArrayList<>();
            result.add(RespValue.bulkString(command.getArg(1)));
            result.add(RespValue.bulkString(""));
            return RespValue.array(result);
        }

        return RespValue.array(new ArrayList<>());
    }

    private static final class ReplicationEvent {
        final boolean isDelete;
        final String key;
        final String value;
        final long expiryEpochMillis;

        ReplicationEvent(boolean isDelete, String key, String value, long expiryEpochMillis) {
            this.isDelete = isDelete;
            this.key = key;
            this.value = value;
            this.expiryEpochMillis = expiryEpochMillis;
        }
    }
}