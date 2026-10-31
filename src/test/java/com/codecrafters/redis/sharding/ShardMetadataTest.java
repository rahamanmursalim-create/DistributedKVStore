package com.codecrafters.redis.sharding;

import org.junit.jupiter.api.Test;

public class ShardMetadataTest {

    public static void main(String[] args) {
        testShardMigrationFlow();
        testInvalidTransitions();
        System.out.println("ALL SHARD METADATA TESTS PASSED");
    }

    private static void testShardMigrationFlow() {
        ShardMetadata shard = new ShardMetadata("shard-1", "node-1");
        assert "shard-1".equals(shard.getShardId());
        assert "node-1".equals(shard.getOwnerNodeId());
        assert shard.getState() == ShardMetadata.State.ACTIVE;

        shard.beginMigration();
        assert shard.getState() == ShardMetadata.State.MIGRATING;

        shard.completeMigration("node-2");
        assert shard.getState() == ShardMetadata.State.ACTIVE;
        assert "node-2".equals(shard.getOwnerNodeId());

        shard.markUnavailable();
        assert shard.getState() == ShardMetadata.State.UNAVAILABLE;

        System.out.println("TEST 1 PASSED: Shard migration state transitions");
    }

    private static void testInvalidTransitions() {
        ShardMetadata shard = new ShardMetadata("shard-2", "node-1");

        try {
            shard.completeMigration("node-2");
            throw new AssertionError("completeMigration without beginMigration should fail");
        } catch (IllegalStateException expected) {
        }

        try {
            new ShardMetadata("", "node-1");
            throw new AssertionError("Blank shard ID should fail");
        } catch (IllegalArgumentException expected) {
        }

        System.out.println("TEST 2 PASSED: Shard invalid transition guards");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
