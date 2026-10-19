package com.distributedkv.storage;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.config.StorageConfig;
import com.distributedkv.raft.RaftStorage;
import com.distributedkv.storage.engines.btree.BPlusTreeEngine;
import com.distributedkv.storage.engines.hashtable.HashTableEngine;
import com.distributedkv.storage.engines.rocksdb.RocksDBEngine;
import com.distributedkv.storage.mvcc.MVCCStorage;
import com.distributedkv.storage.mvcc.TransactionManager;

/**
 * Manager for creating and managing storage engines.
 *
 * The StorageManager is responsible for creating, initializing, and managing
 * the lifecycle of storage engines and related components like MVCC storage
 * and RaftStorage instances.
 */
public class StorageManager {
    private static final Logger LOGGER = Logger.getLogger(StorageManager.class.getName());

    /** Configuration for storage */
    private final StorageConfig config;

    /** Map of storage engines by name */
    private final Map<String, StorageEngine> engines = new ConcurrentHashMap<>();

    /** Map of MVCC storage instances by engine name */
    private final Map<String, MVCCStorage> mvccStorages = new ConcurrentHashMap<>();

    /** Map of transaction managers by engine name */
    private final Map<String, TransactionManager> transactionManagers = new ConcurrentHashMap<>();

    /** Map of Raft storage instances by group ID */
    private final Map<String, RaftStorage> raftStorages = new ConcurrentHashMap<>();

    /**
     * Enum for supported storage engine types.
     */
    public enum EngineType {
        ROCKSDB,
        BTREE,
        HASHTABLE
    }

    /**
     * Creates a new storage manager.
     *
     * @param config The storage configuration
     */
    public StorageManager(StorageConfig config) {
        this.config = config;

        // Create data directory if it doesn't exist
        File dataDir = new File(config.getDataDir());
        if (!dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                LOGGER.warning("Failed to create data directory: " + dataDir.getAbsolutePath());
            }
        }
    }

    /**
     * Creates a storage engine with the specified type and name.
     *
     * @param type The engine type
     * @param name The engine name
     * @return The created storage engine
     * @throws IOException If an error occurs creating the engine
     */
    public StorageEngine createStorageEngine(EngineType type, String name) throws IOException {
        // Check if engine already exists
        StorageEngine existingEngine = engines.get(name);
        if (existingEngine != null) {
            return existingEngine;
        }

        // Create engine data directory
        String engineDataPath = config.getDataDir() + File.separator + name;
        File engineDataDir = new File(engineDataPath);
        if (!engineDataDir.exists()) {
            if (!engineDataDir.mkdirs()) {
                throw new IOException("Failed to create engine data directory: " + engineDataPath);
            }
        }

        // Create the engine
        StorageEngine engine;
        switch (type) {
            case ROCKSDB:
                engine = new RocksDBEngine(name, engineDataPath, config.getRocksDBOptions());
                break;
            case BTREE:
                engine = new BPlusTreeEngine(name, engineDataPath, config.getBTreeOptions());
                break;
            case HASHTABLE:
                engine = new HashTableEngine(name, engineDataPath);
                break;
            default:
                throw new IllegalArgumentException("Unsupported engine type: " + type);
        }

        // Initialize the engine
        try {
            engine.initialize();
        } catch (Exception e) {
            throw new IOException("Failed to initialize engine: " + name, e);
        }

        // Store the engine
        engines.put(name, engine);

        LOGGER.info("Created storage engine: " + name + " (" + type + ")");

        return engine;
    }

    /**
     * Creates MVCC storage for a storage engine.
     *
     * @param engine The storage engine
     * @return The MVCC storage
     */
    public MVCCStorage createMVCCStorage(StorageEngine engine) {
        // Check if MVCC storage already exists
        MVCCStorage existingMVCC = mvccStorages.get(engine.getName());
        if (existingMVCC != null) {
            return existingMVCC;
        }

        // Create transaction manager
        TransactionManager txManager = new TransactionManager();
        transactionManagers.put(engine.getName(), txManager);

        // Create MVCC storage
        MVCCStorage mvccStorage = new MVCCStorage(engine, txManager, config.getMvccOptions());
        mvccStorages.put(engine.getName(), mvccStorage);

        LOGGER.info("Created MVCC storage for engine: " + engine.getName());

        return mvccStorage;
    }

    /**
     * Creates Raft storage for a Raft group.
     *
     * @param groupId The Raft group ID
     * @return The Raft storage
     * @throws IOException If an error occurs creating the storage
     */
    public RaftStorage createRaftStorage(String groupId) throws IOException {
        // Check if Raft storage already exists
        RaftStorage existingStorage = raftStorages.get(groupId);
        if (existingStorage != null) {
            return existingStorage;
        }

        // Create Raft data directory
        String raftDataPath = config.getDataDir() + File.separator + "raft" +
                File.separator + groupId;
        File raftDataDir = new File(raftDataPath);
        if (!raftDataDir.exists()) {
            if (!raftDataDir.mkdirs()) {
                throw new IOException("Failed to create Raft data directory: " + raftDataPath);
            }
        }

        // TODO: Implement concrete RaftStorage
        RaftStorage raftStorage = null; // new FileRaftStorage(groupId, raftDataPath);

        // Store the Raft storage
        raftStorages.put(groupId, raftStorage);

        LOGGER.info("Created Raft storage for group: " + groupId);

        return raftStorage;
    }

    /**
     * Gets a storage engine by name.
     *
     * @param name The engine name
     * @return The storage engine, or null if not found
     */
    public StorageEngine getStorageEngine(String name) {
        return engines.get(name);
    }

    /**
     * Gets MVCC storage for a storage engine.
     *
     * @param engineName The engine name
     * @return The MVCC storage, or null if not found
     */
    public MVCCStorage getMVCCStorage(String engineName) {
        return mvccStorages.get(engineName);
    }

    /**
     * Gets a transaction manager by engine name.
     *
     * @param engineName The engine name
     * @return The transaction manager, or null if not found
     */
    public TransactionManager getTransactionManager(String engineName) {
        return transactionManagers.get(engineName);
    }

    /**
     * Gets Raft storage for a Raft group.
     *
     * @param groupId The Raft group ID
     * @return The Raft storage, or null if not found
     */
    public RaftStorage getRaftStorage(String groupId) {
        return raftStorages.get(groupId);
    }

    /**
     * Gets all storage engines.
     *
     * @return Map of all storage engines
     */
    public Map<String, StorageEngine> getAllStorageEngines() {
        return new HashMap<>(engines);
    }

    /**
     * Gets all MVCC storage instances.
     *
     * @return Map of all MVCC storage instances
     */
    public Map<String, MVCCStorage> getAllMVCCStorages() {
        return new HashMap<>(mvccStorages);
    }

    /**
     * Gets all Raft storage instances.
     *
     * @return Map of all Raft storage instances
     */
    public Map<String, RaftStorage> getAllRaftStorages() {
        return new HashMap<>(raftStorages);
    }

    /**
     * Closes all storage engines and related resources.
     */
    public void close() {
        LOGGER.info("Closing storage manager");

        // Close storage engines
        for (StorageEngine engine : engines.values()) {
            try {
                engine.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to close storage engine: " + engine.getName(), e);
            }
        }

        // Clear maps
        engines.clear();
        mvccStorages.clear();
        transactionManagers.clear();
        raftStorages.clear();
    }
}