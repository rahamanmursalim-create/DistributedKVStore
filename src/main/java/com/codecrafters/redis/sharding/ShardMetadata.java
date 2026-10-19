
package com.codecrafters.redis.sharding;

import java.util.Objects;

public final class ShardMetadata {

    public enum State {
        ACTIVE,
        MIGRATING,
        UNAVAILABLE
    }

    private final String shardId;
    private volatile String ownerNodeId;
    private volatile State state;

    public ShardMetadata(String shardId, String ownerNodeId) {
        if (shardId == null || shardId.isBlank()) {
            throw new IllegalArgumentException("Shard ID is required");
        }
        if (ownerNodeId == null || ownerNodeId.isBlank()) {
            throw new IllegalArgumentException("Owner node ID is required");
        }

        this.shardId = shardId;
        this.ownerNodeId = ownerNodeId;
        this.state = State.ACTIVE;
    }

    public String getShardId() {
        return shardId;
    }

    public String getOwnerNodeId() {
        return ownerNodeId;
    }

    public State getState() {
        return state;
    }

    public synchronized void beginMigration() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException(
                    "Only an active shard can begin migration");
        }
        state = State.MIGRATING;
    }

    public synchronized void completeMigration(String newOwnerNodeId) {
        if (state != State.MIGRATING) {
            throw new IllegalStateException(
                    "Shard is not migrating");
        }
        if (newOwnerNodeId == null || newOwnerNodeId.isBlank()) {
            throw new IllegalArgumentException(
                    "New owner node ID is required");
        }

        ownerNodeId = newOwnerNodeId;
        state = State.ACTIVE;
    }

    public synchronized void markUnavailable() {
        state = State.UNAVAILABLE;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof ShardMetadata other)) return false;
        return shardId.equals(other.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId);
    }
}
