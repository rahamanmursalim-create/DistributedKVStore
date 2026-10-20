package com.distributedkv.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Implementation of the Raft log.
 *
 * The log is a central data structure in the Raft consensus algorithm. It stores
 * a sequence of log entries that will be applied to the state machine once committed.
 */
public class Log {
    private static final Logger LOGGER = Logger.getLogger(Log.class.getName());

    /** The storage for this log */
    private final RaftStorage storage;

    /** Lock to ensure thread safety */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** In-memory cache of log entries */
    private final List<LogEntry> entries = new ArrayList<>();

    /** The commit index (highest log entry known to be committed) */
    private volatile long commitIndex = 0;

    /** The last applied index (highest log entry applied to state machine) */
    private volatile long lastApplied = 0;

    /** The first index in the log */
    private long firstIndex = 1;

    /** The last index in the log */
    private long lastIndex = 0;

    /** The term of the last snapshot */
    private long lastSnapshotTerm = 0;

    /** The index of the last snapshot */
    private long lastSnapshotIndex = 0;

    /**
     * Creates a new log.
     *
     * @param storage The storage for this log
     */
    public Log(RaftStorage storage) {
        this.storage = storage;
    }

    /**
     * Initializes the log from persistent storage.
     *
     * @throws IOException If an I/O error occurs
     */
    public void initialize() throws IOException {
        lock.writeLock().lock();
        try {
            // Load snapshot info if available
            Snapshot snapshot = storage.readLatestSnapshot();
            if (snapshot != null) {
                SnapshotMetadata metadata = snapshot.getMetadata();
                lastSnapshotIndex = metadata.getLastIncludedIndex();
                lastSnapshotTerm = metadata.getLastIncludedTerm();
                firstIndex = lastSnapshotIndex + 1;
            }

            // Load persisted log entries
            List<LogEntry> persistedEntries = storage.readLogEntries(firstIndex);
            entries.clear();
            entries.addAll(persistedEntries);

            // Update indices
            if (!entries.isEmpty()) {
                lastIndex = entries.get(entries.size() - 1).getIndex();
            } else {
                lastIndex = firstIndex - 1;
            }

            LOGGER.info(String.format("Log initialized: firstIndex=%d, lastIndex=%d, " +
                    "lastSnapshotIndex=%d, lastSnapshotTerm=%d",
                    firstIndex, lastIndex, lastSnapshotIndex, lastSnapshotTerm));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends a new entry to the log.
     *
     * @param term The term of the new entry
     * @param command The command for the new entry
     * @param type The type of the new entry
     * @return The new log entry
     * @throws IOException If an I/O error occurs
     */
    public LogEntry append(long term, byte[] command, LogEntry.EntryType type) throws IOException {
        lock.writeLock().lock();
        try {
            long index = lastIndex + 1;
            LogEntry entry = new LogEntry(term, index, command, type);

            // Persist the entry
            storage.writeLogEntry(entry);

            // Update in-memory state
            entries.add(entry);
            lastIndex = index;

            return entry;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends multiple entries to the log.
     *
     * @param newEntries The entries to append
     * @throws IOException If an I/O error occurs
     */
    public void appendEntries(List<LogEntry> newEntries) throws IOException {
        if (newEntries == null || newEntries.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        try {
            // Persist the entries
            storage.writeLogEntries(newEntries);

            // Update in-memory state
            entries.addAll(newEntries);
            lastIndex = newEntries.get(newEntries.size() - 1).getIndex();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets a log entry by index.
     *
     * @param index The index of the entry to get
     * @return The log entry, or null if not found
     */
    public LogEntry getEntry(long index) {
        lock.readLock().lock();
        try {
            if (index < firstIndex || index > lastIndex) {
                return null;
            }

            int pos = (int) (index - firstIndex);
            if (pos < 0 || pos >= entries.size()) {
                return null;
            }

            return entries.get(pos);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets a range of log entries.
     *
     * @param fromIndex The starting index (inclusive)
     * @param toIndex The ending index (inclusive)
     * @return The log entries in the specified range
     */
    public List<LogEntry> getEntries(long fromIndex, long toIndex) {
        lock.readLock().lock();
        try {
            if (fromIndex < firstIndex) {
                fromIndex = firstIndex;
            }

            if (toIndex > lastIndex) {
                toIndex = lastIndex;
            }

            if (fromIndex > toIndex) {
                return new ArrayList<>();
            }

            int fromPos = (int) (fromIndex - firstIndex);
            int toPos = (int) (toIndex - firstIndex);

            List<LogEntry> result = new ArrayList<>(toPos - fromPos + 1);
            for (int i = fromPos; i <= toPos; i++) {
                if (i >= 0 && i < entries.size()) {
                    result.add(entries.get(i));
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets all entries from the specified index to the end of the log.
     *
     * @param fromIndex The starting index (inclusive)
     * @return The log entries from the specified index
     */
    public List<LogEntry> getEntriesFrom(long fromIndex) {
        return getEntries(fromIndex, lastIndex);
    }

    /**
     * Deletes all entries after the specified index.
     *
     * @param index The index after which entries should be deleted
     * @throws IOException If an I/O error occurs
     */
    public void deleteEntriesAfter(long index) throws IOException {
        lock.writeLock().lock();
        try {
            if (index < firstIndex - 1) {
                index = firstIndex - 1;
            }

            if (index >= lastIndex) {
                return;
            }

            // Remove entries from storage
            storage.truncateLog(index);

            // Remove entries from memory
            int pos = (int) (index - firstIndex + 1);
            if (pos >= 0 && pos < entries.size()) {
                entries.subList(pos, entries.size()).clear();
            }

            // Update last index
            lastIndex = index;

            LOGGER.info("Deleted entries after index " + index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the term of the entry at the specified index.
     *
     * @param index The index
     * @return The term, or 0 if the entry is not found
     */
    public long getTerm(long index) {
        if (index == 0) {
            return 0;
        }

        if (index == lastSnapshotIndex) {
            return lastSnapshotTerm;
        }

        LogEntry entry = getEntry(index);
        return entry != null ? entry.getTerm() : 0;
    }

    /**
     * Sets the commit index.
     *
     * @param commitIndex The new commit index
     */
    public void setCommitIndex(long commitIndex) {
        lock.writeLock().lock();
        try {
            if (commitIndex > this.commitIndex) {
                this.commitIndex = commitIndex;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the commit index.
     *
     * @return The commit index
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Sets the last applied index.
     *
     * @param lastApplied The new last applied index
     */
    public void setLastApplied(long lastApplied) {
        lock.writeLock().lock();
        try {
            if (lastApplied > this.lastApplied) {
                this.lastApplied = lastApplied;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the last applied index.
     *
     * @return The last applied index
     */
    public long getLastApplied() {
        return lastApplied;
    }

    /**
     * Gets the first index in the log.
     *
     * @return The first index
     */
    public long getFirstIndex() {
        return firstIndex;
    }

    /**
     * Gets the last index in the log.
     *
     * @return The last index
     */
    public long getLastIndex() {
        return lastIndex;
    }

    /**
     * Gets the term of the last entry in the log.
     *
     * @return The term of the last entry
     */
    public long getLastTerm() {
        return getTerm(lastIndex);
    }

    /**
     * Checks if the log contains an entry at the specified index with the specified term.
     *
     * @param index The index to check
     * @param term The term to check
     * @return True if the log contains an entry at the specified index with the specified term
     */
    public boolean containsEntry(long index, long term) {
        return getTerm(index) == term;
    }

    /**
     * Updates the log after installing a snapshot.
     *
     * @param snapshotIndex The last index in the snapshot
     * @param snapshotTerm The term of the last entry in the snapshot
     * @throws IOException If an I/O error occurs
     */
    public void installSnapshot(long snapshotIndex, long snapshotTerm) throws IOException {
        lock.writeLock().lock();
        try {
            // Update snapshot info
            lastSnapshotIndex = snapshotIndex;
            lastSnapshotTerm = snapshotTerm;

            // Update indices
            if (firstIndex > snapshotIndex) {
                firstIndex = snapshotIndex + 1;
            }

            // Remove obsolete entries
            List<LogEntry> newEntries = getEntriesFrom(snapshotIndex + 1);

            // Clear and rebuild the log
            storage.clear();
            entries.clear();

            if (!newEntries.isEmpty()) {
                appendEntries(newEntries);
            } else {
                lastIndex = snapshotIndex;
            }

            LOGGER.info(String.format("Installed snapshot: index=%d, term=%d",
                    snapshotIndex, snapshotTerm));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the number of entries in the log.
     *
     * @return The number of entries
     */
    public int size() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a string representation of this log.
     *
     * @return A string representation of this log
     */
    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "Log{" +
                    "firstIndex=" + firstIndex +
                    ", lastIndex=" + lastIndex +
                    ", commitIndex=" + commitIndex +
                    ", lastApplied=" + lastApplied +
                    ", entries.size=" + entries.size() +
                    ", lastSnapshotIndex=" + lastSnapshotIndex +
                    ", lastSnapshotTerm=" + lastSnapshotTerm +
                    '}';
        } finally {
            lock.readLock().unlock();
        }
    }
}