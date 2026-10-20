package com.distributedkv.raft;

import com.distributedkv.common.util.SerializationUtil;
import com.distributedkv.storage.StorageEngine;
import com.distributedkv.storage.mvcc.MVCCStorage;
import com.distributedkv.storage.mvcc.Transaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Implementation of a key-value store state machine.
 *
 * This state machine implementation stores key-value pairs and supports
 * basic operations like put, get, and delete.
 */
public class KVStateMachine implements StateMachine {
    private static final Logger LOGGER = Logger.getLogger(KVStateMachine.class.getName());

    /** The storage engine backing this state machine */
    private final StorageEngine storageEngine;

    /** Lock to ensure thread safety */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Executor for async operations */
    private final ExecutorService executor;

    /** The index of the last applied log entry */
    private volatile long lastAppliedIndex = 0;

    /** The term of the last applied log entry */
    private volatile long lastAppliedTerm = 0;

    /** Flag indicating whether MVCC is enabled */
    private final boolean mvccEnabled;

    /** MVCC storage if enabled */
    private final MVCCStorage mvccStorage;

    /**
     * Enumeration of command types supported by this state machine.
     */
    public enum CommandType {
        PUT((byte) 0),
        GET((byte) 1),
        DELETE((byte) 2);

        private final byte value;

        CommandType(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static CommandType fromValue(byte value) {
            for (CommandType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown command type: " + value);
        }
    }

    /**
     * Creates a new KVStateMachine.
     *
     * @param storageEngine The storage engine to use
     */
    public KVStateMachine(StorageEngine storageEngine) {
        this(storageEngine, false, null);
    }

    /**
     * Creates a new KVStateMachine with MVCC support.
     *
     * @param storageEngine The storage engine to use
     * @param mvccEnabled Whether MVCC is enabled
     * @param mvccStorage The MVCC storage to use if enabled
     */
    public KVStateMachine(StorageEngine storageEngine, boolean mvccEnabled, MVCCStorage mvccStorage) {
        this.storageEngine = storageEngine;
        this.executor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "kv-state-machine-worker");
                    t.setDaemon(true);
                    return t;
                });
        this.mvccEnabled = mvccEnabled;
        this.mvccStorage = mvccStorage;
    }

    @Override
    public byte[] apply(LogEntry logEntry) {
        if (logEntry.getIndex() <= lastAppliedIndex) {
            LOGGER.warning("Ignoring already applied log entry: " + logEntry.getIndex());
            return null;
        }

        lock.writeLock().lock();
        try {
            byte[] result = applyInternal(logEntry);
            lastAppliedIndex = logEntry.getIndex();
            lastAppliedTerm = logEntry.getTerm();
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<byte[]> applyAsync(LogEntry logEntry) {
        return CompletableFuture.supplyAsync(() -> apply(logEntry), executor);
    }

    @Override
    public byte[] read(byte[] key) {
        lock.readLock().lock();
        try {
            if (mvccEnabled) {
                Transaction tx = mvccStorage.beginTransaction(false);
                try {
                    return tx.get(key);
                } finally {
                    tx.commit();
                }
            } else {
                return storageEngine.get(key);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Snapshot takeSnapshot() throws IOException {
        lock.readLock().lock();
        try {
            SnapshotMetadata metadata = new SnapshotMetadata(
                    lastAppliedIndex, lastAppliedTerm, System.currentTimeMillis());

            Map<byte[], byte[]> data;
            if (mvccEnabled) {
                Transaction tx = mvccStorage.beginTransaction(false);
                try {
                    data = tx.getAll();
                } finally {
                    tx.commit();
                }
            } else {
                data = storageEngine.getAll();
            }

            return new Snapshot(metadata, data);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) throws IOException {
        lock.writeLock().lock();
        try {
            SnapshotMetadata metadata = snapshot.getMetadata();

            // Only restore if the snapshot is newer than our current state
            if (metadata.getLastIncludedIndex() <= lastAppliedIndex) {
                LOGGER.warning("Ignoring older snapshot: " + metadata.getLastIncludedIndex() +
                             " <= " + lastAppliedIndex);
                return;
            }

            // Clear current state
            storageEngine.clear();

            // Apply snapshot data
            Map<byte[], byte[]> data = snapshot.getData();
            for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
                storageEngine.put(entry.getKey(), entry.getValue());
            }

            // Update state machine metadata
            lastAppliedIndex = metadata.getLastIncludedIndex();
            lastAppliedTerm = metadata.getLastIncludedTerm();

            LOGGER.info("Restored snapshot: " + metadata.getLastIncludedIndex());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    @Override
    public long getLastAppliedTerm() {
        return lastAppliedTerm;
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        storageEngine.close();
    }

    /**
     * Internal method to apply a command to the state machine.
     *
     * @param logEntry The log entry containing the command to apply
     * @return The result of applying the command
     */
    private byte[] applyInternal(LogEntry logEntry) {
        if (logEntry.getCommand() == null || logEntry.getCommand().length == 0) {
            // No-op entry
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(logEntry.getCommand());
        byte commandTypeByte = buffer.get();
        CommandType commandType = CommandType.fromValue(commandTypeByte);

        switch (commandType) {
            case PUT:
                return handlePut(buffer);
            case GET:
                return handleGet(buffer);
            case DELETE:
                return handleDelete(buffer);
            default:
                throw new IllegalArgumentException("Unknown command type: " + commandType);
        }
    }

    private byte[] handlePut(ByteBuffer buffer) {
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key);

        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);

        if (mvccEnabled) {
            Transaction tx = mvccStorage.beginTransaction(true);
            try {
                tx.put(key, value);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        } else {
            storageEngine.put(key, value);
        }

        return SerializationUtil.serialize(true);
    }

    private byte[] handleGet(ByteBuffer buffer) {
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key);

        byte[] value;
        if (mvccEnabled) {
            Transaction tx = mvccStorage.beginTransaction(false);
            try {
                value = tx.get(key);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        } else {
            value = storageEngine.get(key);
        }

        return value;
    }

    private byte[] handleDelete(ByteBuffer buffer) {
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key);

        boolean result;
        if (mvccEnabled) {
            Transaction tx = mvccStorage.beginTransaction(true);
            try {
                result = tx.delete(key);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        } else {
            result = storageEngine.delete(key);
        }

        return SerializationUtil.serialize(result);
    }

    /**
     * Creates a PUT command.
     *
     * @param key The key to put
     * @param value The value to put
     * @return The serialized command
     */
    public static byte[] createPutCommand(byte[] key, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(
                1 + // command type
                4 + key.length + // key length + key
                4 + value.length // value length + value
        );

        buffer.put(CommandType.PUT.getValue());
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);

        return buffer.array();
    }

    /**
     * Creates a GET command.
     *
     * @param key The key to get
     * @return The serialized command
     */
    public static byte[] createGetCommand(byte[] key) {
        ByteBuffer buffer = ByteBuffer.allocate(
                1 + // command type
                4 + key.length // key length + key
        );

        buffer.put(CommandType.GET.getValue());
        buffer.putInt(key.length);
        buffer.put(key);

        return buffer.array();
    }

    /**
     * Creates a DELETE command.
     *
     * @param key The key to delete
     * @return The serialized command
     */
    public static byte[] createDeleteCommand(byte[] key) {
        ByteBuffer buffer = ByteBuffer.allocate(
                1 + // command type
                4 + key.length // key length + key
        );

        buffer.put(CommandType.DELETE.getValue());
        buffer.putInt(key.length);
        buffer.put(key);

        return buffer.array();
    }
}