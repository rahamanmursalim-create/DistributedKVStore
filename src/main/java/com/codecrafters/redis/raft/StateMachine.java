package com.distributedkv.raft;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for a state machine in the Raft consensus protocol.
 *
 * A state machine represents the application logic that is replicated across
 * all nodes in a Raft group. It receives commands from the Raft log and
 * applies them to maintain a consistent state.
 */
public interface StateMachine {

    /**
     * Applies a command to the state machine.
     *
     * @param logEntry The log entry containing the command to apply
     * @return The result of applying the command
     */
    byte[] apply(LogEntry logEntry);

    /**
     * Applies a command to the state machine asynchronously.
     *
     * @param logEntry The log entry containing the command to apply
     * @return A future that will complete with the result of applying the command
     */
    CompletableFuture<byte[]> applyAsync(LogEntry logEntry);

    /**
     * Reads the current state at the given key.
     *
     * @param key The key to read
     * @return The value at the given key, or null if not found
     */
    byte[] read(byte[] key);

    /**
     * Creates a snapshot of the current state.
     *
     * @return A snapshot of the current state
     * @throws IOException If an I/O error occurs
     */
    Snapshot takeSnapshot() throws IOException;

    /**
     * Restores the state from a snapshot.
     *
     * @param snapshot The snapshot to restore from
     * @throws IOException If an I/O error occurs
     */
    void restoreSnapshot(Snapshot snapshot) throws IOException;

    /**
     * Gets the last applied index.
     *
     * @return The index of the last log entry applied to this state machine
     */
    long getLastAppliedIndex();

    /**
     * Gets the last applied term.
     *
     * @return The term of the last log entry applied to this state machine
     */
    long getLastAppliedTerm();

    /**
     * Closes any resources used by this state machine.
     *
     * @throws Exception If an error occurs
     */
    void close() throws Exception;
}