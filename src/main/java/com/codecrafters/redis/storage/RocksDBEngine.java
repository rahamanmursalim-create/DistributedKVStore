package com.distributedkv.storage.engines.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;

import com.distributedkv.storage.StorageEngine;

/**
 * RocksDB implementation of the StorageEngine interface.
 *
 * This implementation uses RocksDB as the underlying storage engine, providing
 * high performance and durability for the key-value store.
 */
public class RocksDBEngine implements StorageEngine {
    private static final Logger LOGGER = Logger.getLogger(RocksDBEngine.class.getName());

    /** The name of this storage engine */
    private final String name;

    /** The path where this storage engine persists its data */
    private final String dataPath;

    /** RocksDB options */
    private final Map<String, String> options;

    /** The RocksDB instance */
    private RocksDB db;

    /** Default column family handle */
    private ColumnFamilyHandle defaultColumnFamily;

    /** Write options for normal operations */
    private WriteOptions writeOptions;

    /** Write options for batch operations */
    private WriteOptions batchWriteOptions;

    /** Read options for normal operations */
    private ReadOptions readOptions;

    /** Flush options */
    private FlushOptions flushOptions;

    /** Flag indicating whether this engine is closed */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    static {
        // Load RocksDB native library
        RocksDB.loadLibrary();
    }

    /**
     * Creates a new RocksDB engine.
     *
     * @param name The engine name
     * @param dataPath The data path
     */
    public RocksDBEngine(String name, String dataPath) {
        this(name, dataPath, new HashMap<>());
    }

    /**
     * Creates a new RocksDB engine with the specified options.
     *
     * @param name The engine name
     * @param dataPath The data path
     * @param options The RocksDB options
     */
    public RocksDBEngine(String name, String dataPath, Map<String, String> options) {
        this.name = name;
        this.dataPath = dataPath;
        this.options = options;
    }

    @Override
    public void initialize() throws Exception {
        if (db != null) {
            return;
        }

        LOGGER.info("Initializing RocksDB engine: " + name + " at " + dataPath);

        try {
            // Create data directory if it doesn't exist
            Path dirPath = Paths.get(dataPath);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            // Create RocksDB options
            DBOptions dbOptions = createDBOptions();
            ColumnFamilyOptions cfOptions = createColumnFamilyOptions();

            // Create default column family descriptor
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));

            // Create column family handles
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            // Open database
            db = RocksDB.open(dbOptions, dataPath, columnFamilyDescriptors, columnFamilyHandles);

            // Store default column family handle
            defaultColumnFamily = columnFamilyHandles.get(0);

            // Create write options
            writeOptions = new WriteOptions()
                    .setSync(getBooleanOption("sync", false))
                    .setDisableWAL(getBooleanOption("disable_wal", false));

            // Create batch write options
            batchWriteOptions = new WriteOptions()
                    .setSync(getBooleanOption("batch_sync", true))
                    .setDisableWAL(getBooleanOption("batch_disable_wal", false));

            // Create read options
            readOptions = new ReadOptions()
                    .setVerifyChecksums(getBooleanOption("verify_checksums", true))
                    .setFillCache(getBooleanOption("fill_cache", true));

            // Create flush options
            flushOptions = new FlushOptions()
                    .setWaitForFlush(getBooleanOption("wait_for_flush", true));

            LOGGER.info("RocksDB engine initialized: " + name);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize RocksDB engine: " + name, e);
            close();
            throw e;
        }
    }

    /**
     * Creates RocksDB DB options based on the provided options map.
     *
     * @return The DB options
     */
    private DBOptions createDBOptions() {
        DBOptions dbOptions = new DBOptions();

        // Set basic options
        dbOptions.setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setInfoLogLevel(getInfoLogLevel())
                .setMaxOpenFiles(getIntOption("max_open_files", -1))
                .setMaxFileOpeningThreads(getIntOption("max_file_opening_threads", 16))
                .setMaxBackgroundJobs(getIntOption("max_background_jobs", 2))
                .setMaxBackgroundCompactions(getIntOption("max_background_compactions", 1))
                .setMaxBackgroundFlushes(getIntOption("max_background_flushes", 1))
                .setIncreaseParallelism(getIntOption("parallel_threads", Runtime.getRuntime().availableProcessors()));

        // Set advanced options
        if (getBooleanOption("paranoid_checks", true)) {
            dbOptions.setParanoidChecks(true);
        }

        if (getBooleanOption("enable_statistics", false)) {
            dbOptions.setStatistics();
        }

        dbOptions.setDelayedWriteRate(getLongOption("delayed_write_rate", 16 * 1024 * 1024));

        return dbOptions;
    }

    /**
     * Creates RocksDB column family options based on the provided options map.
     *
     * @return The column family options
     */
    private ColumnFamilyOptions createColumnFamilyOptions() {
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();

        // Set basic options
        cfOptions.setCompressionType(getCompressionType())
                .setCompactionStyle(getCompactionStyle())
                .setNumLevels(getIntOption("num_levels", 7))
                .setLevelCompactionDynamicLevelBytes(getBooleanOption("level_compaction_dynamic_level_bytes", false))
                .setMinWriteBufferNumberToMerge(getIntOption("min_write_buffer_number_to_merge", 1))
                .setMaxWriteBufferNumber(getIntOption("max_write_buffer_number", 2))
                .setWriteBufferSize(getLongOption("write_buffer_size", 64 * 1024 * 1024))
                .setTargetFileSizeBase(getLongOption("target_file_size_base", 64 * 1024 * 1024))
                .setMaxBytesForLevelBase(getLongOption("max_bytes_for_level_base", 256 * 1024 * 1024))
                .setMaxBytesForLevelMultiplier(getDoubleOption("max_bytes_for_level_multiplier", 10.0));

        // Create table options
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();

        // Set cache size
        long blockCacheSize = getLongOption("block_cache_size", 64 * 1024 * 1024);
        if (blockCacheSize > 0) {
            tableConfig.setBlockCache(org.rocksdb.Cache.createLRUCache(blockCacheSize));
        }

        // Set bloom filter
        int bitsPerKey = getIntOption("bloom_filter_bits_per_key", 10);
        if (bitsPerKey > 0) {
            tableConfig.setFilterPolicy(new BloomFilter(bitsPerKey, getBooleanOption("bloom_filter_whole_key", true)));
        }

        // Set block size
        tableConfig.setBlockSize(getLongOption("block_size", 4 * 1024));

        // Set other table options
        tableConfig.setCacheIndexAndFilterBlocks(getBooleanOption("cache_index_and_filter_blocks", true))
                .setPinL0FilterAndIndexBlocksInCache(getBooleanOption("pin_l0_filter_and_index_blocks_in_cache", true))
                .setCacheIndexAndFilterBlocksWithHighPriority(getBooleanOption("cache_index_and_filter_blocks_with_high_priority", true));

        // Set table format config
        cfOptions.setTableFormatConfig(tableConfig);

        return cfOptions;
    }

    /**
     * Gets the RocksDB info log level based on the provided options.
     *
     * @return The info log level
     */
    private org.rocksdb.InfoLogLevel getInfoLogLevel() {
        String level = options.getOrDefault("info_log_level", "info").toLowerCase();
        switch (level) {
            case "debug":
                return org.rocksdb.InfoLogLevel.DEBUG_LEVEL;
            case "info":
                return org.rocksdb.InfoLogLevel.INFO_LEVEL;
            case "warn":
            case "warning":
                return org.rocksdb.InfoLogLevel.WARN_LEVEL;
            case "error":
                return org.rocksdb.InfoLogLevel.ERROR_LEVEL;
            case "fatal":
                return org.rocksdb.InfoLogLevel.FATAL_LEVEL;
            case "header":
                return org.rocksdb.InfoLogLevel.HEADER_LEVEL;
            default:
                return org.rocksdb.InfoLogLevel.INFO_LEVEL;
        }
    }

    /**
     * Gets the RocksDB compression type based on the provided options.
     *
     * @return The compression type
     */
    private CompressionType getCompressionType() {
        String compression = options.getOrDefault("compression", "snappy").toLowerCase();
        switch (compression) {
            case "none":
                return CompressionType.NO_COMPRESSION;
            case "snappy":
                return CompressionType.SNAPPY_COMPRESSION;
            case "zlib":
                return CompressionType.ZLIB_COMPRESSION;
            case "bzip2":
                return CompressionType.BZip2_COMPRESSION;
            case "lz4":
                return CompressionType.LZ4_COMPRESSION;
            case "lz4hc":
                return CompressionType.LZ4HC_COMPRESSION;
            case "xpress":
                return CompressionType.XPRESS_COMPRESSION;
            case "zstd":
                return CompressionType.ZSTD_COMPRESSION;
            default:
                return CompressionType.SNAPPY_COMPRESSION;
        }
    }

    /**
     * Gets the RocksDB compaction style based on the provided options.
     *
     * @return The compaction style
     */
    private CompactionStyle getCompactionStyle() {
        String style = options.getOrDefault("compaction_style", "level").toLowerCase();
        switch (style) {
            case "level":
                return CompactionStyle.LEVEL;
            case "universal":
                return CompactionStyle.UNIVERSAL;
            case "fifo":
                return CompactionStyle.FIFO;
            case "none":
                return CompactionStyle.NONE;
            default:
                return CompactionStyle.LEVEL;
        }
    }

    /**
     * Gets a boolean option from the options map.
     *
     * @param key The option key
     * @param defaultValue The default value
     * @return The option value
     */
    private boolean getBooleanOption(String key, boolean defaultValue) {
        String value = options.get(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    /**
     * Gets an integer option from the options map.
     *
     * @param key The option key
     * @param defaultValue The default value
     * @return The option value
     */
    private int getIntOption(String key, int defaultValue) {
        String value = options.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Gets a long option from the options map.
     *
     * @param key The option key
     * @param defaultValue The default value
     * @return The option value
     */
    private long getLongOption(String key, long defaultValue) {
        String value = options.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Gets a double option from the options map.
     *
     * @param key The option key
     * @param defaultValue The default value
     * @return The option value
     */
    private double getDoubleOption(String key, double defaultValue) {
        String value = options.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public byte[] get(byte[] key) {
        checkState();

        try {
            return db.get(defaultColumnFamily, readOptions, key);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to get key", e);
            return null;
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        checkState();

        try {
            db.put(defaultColumnFamily, writeOptions, key, value);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to put key", e);
            throw new RuntimeException("Failed to put key", e);
        }
    }

    @Override
    public void putAll(Map<byte[], byte[]> entries) {
        checkState();

        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
                batch.put(defaultColumnFamily, entry.getKey(), entry.getValue());
            }
            db.write(batchWriteOptions, batch);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to put batch", e);
            throw new RuntimeException("Failed to put batch", e);
        }
    }

    @Override
    public boolean delete(byte[] key) {
        checkState();

        try {
            // Check if the key exists
            byte[] value = db.get(defaultColumnFamily, readOptions, key);
            if (value == null) {
                return false;
            }

            // Delete the key
            db.delete(defaultColumnFamily, writeOptions, key);
            return true;
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to delete key", e);
            return false;
        }
    }

    @Override
    public void deleteAll(List<byte[]> keys) {
        checkState();

        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            for (byte[] key : keys) {
                batch.delete(defaultColumnFamily, key);
            }
            db.write(batchWriteOptions, batch);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to delete batch", e);
            throw new RuntimeException("Failed to delete batch", e);
        }
    }

    @Override
    public boolean exists(byte[] key) {
        checkState();

        try {
            byte[] value = db.get(defaultColumnFamily, readOptions, key);
            return value != null;
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to check key existence", e);
            return false;
        }
    }

    @Override
    public Map<byte[], byte[]> getAll() {
        checkState();

        Map<byte[], byte[]> result = new HashMap<>();

        try (RocksIterator iter = db.newIterator(defaultColumnFamily, readOptions)) {
            iter.seekToFirst();
            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();
                result.put(key, value);
                iter.next();
            }
        }

        return result;
    }

    @Override
    public Iterator<byte[]> keyIterator() {
        checkState();

        final RocksIterator iter = db.newIterator(defaultColumnFamily, readOptions);
        iter.seekToFirst();

        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                boolean valid = iter.isValid();
                if (!valid) {
                    iter.close();
                }
                return valid;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                byte[] key = iter.key();
                iter.next();
                return key;
            }
        };
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> entryIterator() {
        checkState();

        final RocksIterator iter = db.newIterator(defaultColumnFamily, readOptions);
        iter.seekToFirst();

        return new Iterator<Map.Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                boolean valid = iter.isValid();
                if (!valid) {
                    iter.close();
                }
                return valid;
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                byte[] key = iter.key();
                byte[] value = iter.value();
                iter.next();
                return new AbstractMap.SimpleImmutableEntry<>(key, value);
            }
        };
    }

    /**
     * Compares two byte arrays lexicographically.
     *
     * @param key1 The first key
     * @param key2 The second key
     * @return A negative, zero, or positive integer as the first key is less than, equal to,
     *         or greater than the second key
     */
    private int compareKeys(byte[] key1, byte[] key2) {
        int len1 = key1.length;
        int len2 = key2.length;
        int lim = Math.min(len1, len2);

        for (int i = 0; i < lim; i++) {
            int a = key1[i] & 0xff;
            int b = key2[i] & 0xff;
            if (a != b) {
                return a - b;
            }
        }

        return len1 - len2;
    }

    @Override
    public void clear() {
        checkState();

        try {
            // Recreate the database to clear all data
            close();

            // Delete all files in the database directory
            File dataDir = new File(dataPath);
            if (dataDir.exists() && dataDir.isDirectory()) {
                for (File file : dataDir.listFiles()) {
                    if (!file.delete()) {
                        LOGGER.warning("Failed to delete file: " + file.getAbsolutePath());
                    }
                }
            }

            // Reinitialize the database
            initialize();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to clear database", e);
            throw new RuntimeException("Failed to clear database", e);
        }
    }

    @Override
    public long approximateSize() {
        checkState();

        try {
            // Use RocksDB's GetApproximateSizes method
            long[] sizes = db.getApproximateSizes(
                    defaultColumnFamily,
                    new Range[] { new Range(new byte[0], new byte[] { (byte) 0xFF }) },
                    org.rocksdb.SizeApproximationFlag.INCLUDE_FILES);

            return sizes[0];
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to get approximate size", e);

            // Fall back to counting entries
            long count = 0;
            try (RocksIterator iter = db.newIterator(defaultColumnFamily, readOptions)) {
                iter.seekToFirst();
                while (iter.isValid()) {
                    count++;
                    iter.next();
                }
            }

            return count;
        }
    }

    @Override
    public Object createSnapshot() {
        checkState();

        // Create a RocksDB snapshot
        Snapshot snapshot = db.getSnapshot();

        // Create a custom read options with the snapshot
        ReadOptions snapshotReadOptions = new ReadOptions()
                .setSnapshot(snapshot)
                .setVerifyChecksums(readOptions.verifyChecksums())
                .setFillCache(readOptions.fillCache());

        // Create a snapshot wrapper
        return new RocksDBSnapshot(snapshot, snapshotReadOptions);
    }

    @Override
    public void releaseSnapshot(Object snapshot) {
        checkState();

        if (!(snapshot instanceof RocksDBSnapshot)) {
            throw new IllegalArgumentException("Invalid snapshot type");
        }

        RocksDBSnapshot rocksSnapshot = (RocksDBSnapshot) snapshot;

        // Release the snapshot
        db.releaseSnapshot(rocksSnapshot.getSnapshot());

        // Close the read options
        rocksSnapshot.getReadOptions().close();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDataPath() {
        return dataPath;
    }

    @Override
    public void flush() throws Exception {
        checkState();

        try {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            LOGGER.log(Level.WARNING, "Failed to flush database", e);
            throw e;
        }
    }

    @Override
    public void close() {
        if (closed.getAndSet(true)) {
            return;
        }

        LOGGER.info("Closing RocksDB engine: " + name);

        // Close resources in reverse order of creation
        if (flushOptions != null) {
            flushOptions.close();
            flushOptions = null;
        }

        if (readOptions != null) {
            readOptions.close();
            readOptions = null;
        }

        if (batchWriteOptions != null) {
            batchWriteOptions.close();
            batchWriteOptions = null;
        }

        if (writeOptions != null) {
            writeOptions.close();
            writeOptions = null;
        }

        if (defaultColumnFamily != null) {
            defaultColumnFamily.close();
            defaultColumnFamily = null;
        }

        if (db != null) {
            db.close();
            db = null;
        }

        LOGGER.info("RocksDB engine closed: " + name);
    }

    /**
     * Checks if the database is open and ready for operations.
     *
     * @throws IllegalStateException If the database is closed
     */
    private void checkState() {
        if (closed.get() || db == null) {
            throw new IllegalStateException("Database is closed");
        }
    }

    /**
     * Wrapper for a RocksDB snapshot.
     */
    private static class RocksDBSnapshot {
        private final Snapshot snapshot;
        private final ReadOptions readOptions;

        /**
         * Creates a new RocksDB snapshot wrapper.
         *
         * @param snapshot The RocksDB snapshot
         * @param readOptions The read options with the snapshot
         */
        public RocksDBSnapshot(Snapshot snapshot, ReadOptions readOptions) {
            this.snapshot = snapshot;
            this.readOptions = readOptions;
        }

        /**
         * Gets the RocksDB snapshot.
         *
         * @return The snapshot
         */
        public Snapshot getSnapshot() {
            return snapshot;
        }

        /**
         * Gets the read options with the snapshot.
         *
         * @return The read options
         */
        public ReadOptions getReadOptions() {
            return readOptions;
        }
    }

    /**
     * Range wrapper for RocksDB's GetApproximateSizes method.
     */
    private static class Range extends org.rocksdb.Range {
        /**
         * Creates a new range.
         *
         * @param start The start key
         * @param end The end key
         */
        public Range(byte[] start, byte[] end) {
            super(start, end);
        }
    }
}

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> rangeIterator(byte[] startKey, byte[] endKey) {
        checkState();

        final RocksIterator iter = db.newIterator(defaultColumnFamily, readOptions);
        iter.seek(startKey);

        return new Iterator<Map.Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                boolean valid = iter.isValid() && (endKey == null || compareKeys(iter.key(), endKey) < 0);
                if (!valid) {
                    iter.close();
                }
                return valid;
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                byte[] key = iter.key();
                byte[] value = iter.value();
                iter.next();
                return new AbstractMap.SimpleImmutableEntry<>(key, value);
            }