package com.distributedkv.raft;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a single entry in the Raft log.
 *
 * LogEntry objects are the fundamental units of the Raft consensus protocol.
 * Each entry contains a command to be executed by the state machine, along with
 * metadata such as term and index.
 */
public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The term in which the entry was created */
    private final long term;

    /** The index of this entry in the log */
    private final long index;

    /** The command to be applied to the state machine */
    private final byte[] command;

    /** The type of this entry */
    private final EntryType type;

    /** A unique identifier for this entry */
    private final String id;

    /** Timestamp when this entry was created */
    private final long timestamp;

    /**
     * Enumeration of possible log entry types.
     */
    public enum EntryType {
        /** Normal entry containing a command to be applied to the state machine */
        NORMAL,

        /** Configuration change entry */
        CONFIGURATION,

        /** No-op entry typically created when a leader is elected */
        NO_OP,

        /** Entry indicating a snapshot */
        SNAPSHOT_MARKER
    }

    /**
     * Creates a new log entry.
     *
     * @param term The term in which the entry was created
     * @param index The index of this entry in the log
     * @param command The command to be applied to the state machine
     * @param type The type of this entry
     */
    public LogEntry(long term, long index, byte[] command, EntryType type) {
        this.term = term;
        this.index = index;
        this.command = command != null ? Arrays.copyOf(command, command.length) : null;
        this.type = type;
        this.id = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Gets the term in which this entry was created.
     *
     * @return The term
     */
    public long getTerm() {
        return term;
    }

    /**
     * Gets the index of this entry in the log.
     *
     * @return The index
     */
    public long getIndex() {
        return index;
    }

    /**
     * Gets the command to be applied to the state machine.
     *
     * @return The command
     */
    public byte[] getCommand() {
        return command != null ? Arrays.copyOf(command, command.length) : null;
    }

    /**
     * Gets the type of this entry.
     *
     * @return The entry type
     */
    public EntryType getType() {
        return type;
    }

    /**
     * Gets the unique identifier for this entry.
     *
     * @return The ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the timestamp when this entry was created.
     *
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Checks if this entry is a normal command entry.
     *
     * @return True if this is a normal entry, false otherwise
     */
    public boolean isNormalEntry() {
        return type == EntryType.NORMAL;
    }

    /**
     * Checks if this entry is a configuration change entry.
     *
     * @return True if this is a configuration change entry, false otherwise
     */
    public boolean isConfigurationEntry() {
        return type == EntryType.CONFIGURATION;
    }

    /**
     * Checks if this entry is a no-op entry.
     *
     * @return True if this is a no-op entry, false otherwise
     */
    public boolean isNoOpEntry() {
        return type == EntryType.NO_OP;
    }

    /**
     * Checks if this entry is a snapshot marker entry.
     *
     * @return True if this is a snapshot marker entry, false otherwise
     */
    public boolean isSnapshotMarkerEntry() {
        return type == EntryType.SNAPSHOT_MARKER;
    }

    /**
     * Returns the size of this entry in bytes (approximate).
     *
     * @return The size in bytes
     */
    public int getSizeInBytes() {
        return 8 + // term
               8 + // index
               (command != null ? command.length : 0) +
               4 + // type
               36 + // id (UUID string)
               8;  // timestamp
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
               index == logEntry.index &&
               Objects.equals(id, logEntry.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index, id);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", index=" + index +
                ", commandSize=" + (command != null ? command.length : 0) +
                ", type=" + type +
                ", id='" + id + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}