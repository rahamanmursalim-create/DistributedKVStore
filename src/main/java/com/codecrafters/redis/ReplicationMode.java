package com.codecrafters.redis;

/**
 * Defines the replication synchronization mode between primary and replicas.
 */
public enum ReplicationMode {
    /**
     * Synchronous replication: the client write operation blocks until the
     * replica acknowledges receipt and persistence of the write frame.
     */
    SYNC,

    /**
     * Asynchronous replication: the write is applied to local storage and
     * immediately acknowledged to the client, while replication frames are queued
     * and delivered in the background by a dedicated replication worker.
     */
    ASYNC
}
