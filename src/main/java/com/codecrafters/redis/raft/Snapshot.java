package com.distributedkv.raft;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.distributedkv.common.util.SerializationUtil;

/**
 * Represents a snapshot of the state machine at a specific point in time.
 *
 * Snapshots are used for log compaction in Raft, allowing nodes to truncate
 * their logs and replace them with a more compact representation of the state.
 */
public class Snapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The metadata for this snapshot */
    private final SnapshotMetadata metadata;

    /** The state machine data */
    private transient Map<byte[], byte[]> data;

    /** The file where this snapshot is stored (optional) */
    private transient File snapshotFile;

    /**
     * Creates a new snapshot.
     *
     * @param metadata The snapshot metadata
     * @param data The state machine data
     */
    public Snapshot(SnapshotMetadata metadata, Map<byte[], byte[]> data) {
        this.metadata = metadata;
        this.data = new HashMap<>();

        // Deep copy the data
        if (data != null) {
            for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
                byte[] keyCopy = entry.getKey() != null ?
                    entry.getKey().clone() : null;
                byte[] valueCopy = entry.getValue() != null ?
                    entry.getValue().clone() : null;
                this.data.put(keyCopy, valueCopy);
            }
        }
    }

    /**
     * Gets the metadata for this snapshot.
     *
     * @return The snapshot metadata
     */
    public SnapshotMetadata getMetadata() {
        return metadata;
    }

    /**
     * Gets the state machine data.
     *
     * @return The data
     */
    public Map<byte[], byte[]> getData() {
        Map<byte[], byte[]> copy = new HashMap<>();

        // Deep copy the data
        for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
            byte[] keyCopy = entry.getKey() != null ?
                entry.getKey().clone() : null;
            byte[] valueCopy = entry.getValue() != null ?
                entry.getValue().clone() : null;
            copy.put(keyCopy, valueCopy);
        }

        return copy;
    }

    /**
     * Gets the file where this snapshot is stored.
     *
     * @return The snapshot file, or null if not stored in a file
     */
    public File getSnapshotFile() {
        return snapshotFile;
    }

    /**
     * Sets the file where this snapshot is stored.
     *
     * @param snapshotFile The snapshot file
     */
    public void setSnapshotFile(File snapshotFile) {
        this.snapshotFile = snapshotFile;
    }

    /**
     * Gets the size of this snapshot in bytes (approximate).
     *
     * @return The size in bytes
     */
    public long getSizeInBytes() {
        if (snapshotFile != null && snapshotFile.exists()) {
            return snapshotFile.length();
        }

        // Estimate size based on in-memory data
        long size = metadata.getSizeInBytes();

        for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
            size += (entry.getKey() != null ? entry.getKey().length : 0);
            size += (entry.getValue() != null ? entry.getValue().length : 0);
        }

        return size;
    }

    /**
     * Saves this snapshot to a file.
     *
     * @param dir The directory where the snapshot should be saved
     * @return The path to the saved snapshot file
     * @throws IOException If an I/O error occurs
     */
    public Path save(String dir) throws IOException {
        Path dirPath = Paths.get(dir);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        String fileName = String.format("snapshot-%d-%d-%s.snap",
                metadata.getLastIncludedIndex(),
                metadata.getLastIncludedTerm(),
                UUID.randomUUID().toString());

        Path path = dirPath.resolve(fileName);

        // Serialize the snapshot data
        byte[] serializedMetadata = SerializationUtil.serialize(metadata);

        // Write the metadata and data to the file
        try {
            Files.write(path, serializedMetadata);

            // Set the snapshot file
            snapshotFile = path.toFile();

            return path;
        } catch (IOException e) {
            // Clean up if an error occurs
            Files.deleteIfExists(path);
            throw e;
        }
    }

    /**
     * Loads a snapshot from a file.
     *
     * @param path The path to the snapshot file
     * @return The loaded snapshot
     * @throws IOException If an I/O error occurs
     * @throws ClassNotFoundException If the class of a serialized object cannot be found
     */
    public static Snapshot load(Path path) throws IOException, ClassNotFoundException {
        byte[] bytes = Files.readAllBytes(path);

        // Deserialize the snapshot metadata
        SnapshotMetadata metadata = SerializationUtil.deserialize(bytes);

        // Create a new snapshot with empty data
        Snapshot snapshot = new Snapshot(metadata, new HashMap<>());

        // Set the snapshot file
        snapshot.setSnapshotFile(path.toFile());

        return snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Snapshot snapshot = (Snapshot) o;
        return Objects.equals(metadata, snapshot.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata);
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "metadata=" + metadata +
                ", dataSize=" + (data != null ? data.size() : 0) +
                ", snapshotFile=" + (snapshotFile != null ? snapshotFile.getPath() : "null") +
                '}';
    }
}

/**
 * Metadata for a snapshot.
 */
class SnapshotMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The last included log index in this snapshot */
    private final long lastIncludedIndex;

    /** The term of the last included entry */
    private final long lastIncludedTerm;

    /** Timestamp when this snapshot was created */
    private final long timestamp;

    /**
     * Creates new snapshot metadata.
     *
     * @param lastIncludedIndex The last included log index
     * @param lastIncludedTerm The term of the last included entry
     * @param timestamp The timestamp when this snapshot was created
     */
    public SnapshotMetadata(long lastIncludedIndex, long lastIncludedTerm, long timestamp) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.timestamp = timestamp;
    }

    /**
     * Gets the last included log index.
     *
     * @return The last included index
     */
    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    /**
     * Gets the term of the last included entry.
     *
     * @return The last included term
     */
    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    /**
     * Gets the timestamp when this snapshot was created.
     *
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets the size of this metadata in bytes (approximate).
     *
     * @return The size in bytes
     */
    public int getSizeInBytes() {
        return 8 + // lastIncludedIndex
               8 + // lastIncludedTerm
               8;  // timestamp
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotMetadata that = (SnapshotMetadata) o;
        return lastIncludedIndex == that.lastIncludedIndex &&
               lastIncludedTerm == that.lastIncludedTerm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastIncludedIndex, lastIncludedTerm);
    }

    @Override
    public String toString() {
        return "SnapshotMetadata{" +
                "lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", timestamp=" + timestamp +
                '}';
    }
}