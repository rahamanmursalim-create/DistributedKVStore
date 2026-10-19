package com.distributedkv.raft;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for persistent storage of Raft state.
 *
 * RaftStorage is responsible for durably storing the Raft log, metadata,
 * and snapshots. This is critical for ensuring the safety properties of
 * the Raft consensus algorithm in the presence of node failures.
 */
public interface RaftStorage {

    /**
     * Initializes the storage.
     *
     * @throws IOException If an I/O error occurs
     */
    void initialize() throws IOException;

    /**
     * Reads the current term from storage.
     *
     * @return The current term
     * @throws IOException If an I/O error occurs
     */
    long readCurrentTerm() throws IOException;

    /**
     * Writes the current term to storage.
     *
     * @param term The current term
     * @throws IOException If an I/O error occurs
     */
    void writeCurrentTerm(long term) throws IOException;

    /**
     * Reads the voted-for candidate from storage.
     *
     * @return The ID of the voted-for candidate, or null if none
     * @throws IOException If an I/O error occurs
     */
    String readVotedFor() throws IOException;

    /**
     * Writes the voted-for candidate to storage.
     *
     * @param votedFor The ID of the voted-for candidate
     * @throws IOException If an I/O error occurs
     */
    void writeVotedFor(String votedFor) throws IOException;

    /**
     * Reads log entries starting from the specified index.
     *
     * @param fromIndex The starting index (inclusive)
     * @return The log entries
     * @throws IOException If an I/O error occurs
     */
    List<LogEntry> readLogEntries(long fromIndex) throws IOException;

    /**
     * Writes a log entry to storage.
     *
     * @param entry The log entry to write
     * @throws IOException If an I/O error occurs
     */
    void writeLogEntry(LogEntry entry) throws IOException;

    /**
     * Writes multiple log entries to storage.
     *
     * @param entries The log entries to write
     * @throws IOException If an I/O error occurs
     */
    void writeLogEntries(List<LogEntry> entries) throws IOException;

    /**
     * Truncates the log at the specified index.
     *
     * @param index The index after which entries should be removed
     * @throws IOException If an I/O error occurs
     */
    void truncateLog(long index) throws IOException;

    /**
     * Writes a snapshot to storage.
     *
     * @param snapshot The snapshot to write
     * @return The path to the saved snapshot file
     * @throws IOException If an I/O error occurs
     */
    Path writeSnapshot(Snapshot snapshot) throws IOException;

    /**
     * Reads the latest snapshot from storage.
     *
     * @return The latest snapshot, or null if none exists
     * @throws IOException If an I/O error occurs
     */
    Snapshot readLatestSnapshot() throws IOException;

    /**
     * Lists all available snapshots.
     *
     * @return A list of available snapshots
     * @throws IOException If an I/O error occurs
     */
    List<Snapshot> listSnapshots() throws IOException;

    /**
     * Deletes the specified snapshot.
     *
     * @param snapshot The snapshot to delete
     * @throws IOException If an I/O error occurs
     */
    void deleteSnapshot(Snapshot snapshot) throws IOException;

    /**
     * Reads the snapshot with the specified index.
     *
     * @param index The index of the snapshot
     * @return The snapshot, or null if not found
     * @throws IOException If an I/O error occurs
     */
    Snapshot readSnapshot(long index) throws IOException;

    /**
     * Clears all data from storage.
     *
     * @throws IOException If an I/O error occurs
     */
    void clear() throws IOException;

    /**
     * Closes the storage.
     *
     * @throws IOException If an I/O error occurs
     */
    void close() throws IOException;
}