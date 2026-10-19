package com.codecrafters.redis.cluster;

import java.util.Collection;
import java.util.Optional;

public class ClusterMembershipTest {

    public static void main(String[] args) {
        testNodeLifecycle();
        testValidation();
        System.out.println("ALL CLUSTER MEMBERSHIP TESTS PASSED");
    }

    private static void testNodeLifecycle() {
        ClusterMembership membership = new ClusterMembership();
        assert membership.getNodeCount() == 0;

        membership.addNode("node-1");
        membership.addNode("node-2");
        assert membership.getNodeCount() == 2;
        assert membership.containsNode("node-1");

        Optional<NodeStatus> status1 = membership.getStatus("node-1");
        assert status1.isPresent() && status1.get() == NodeStatus.STARTING : "Initial status should be STARTING";

        boolean updated = membership.updateStatus("node-1", NodeStatus.ACTIVE);
        assert updated : "Status update should succeed";
        assert membership.getStatus("node-1").orElseThrow() == NodeStatus.ACTIVE;

        Collection<String> activeNodes = membership.getActiveNodeIds();
        assert activeNodes.size() == 1 && activeNodes.contains("node-1");

        boolean removed = membership.removeNode("node-2");
        assert removed : "Remove node-2 should succeed";
        assert !membership.containsNode("node-2");
        assert membership.getNodeCount() == 1;

        System.out.println("TEST 1 PASSED: Node lifecycle in cluster membership");
    }

    private static void testValidation() {
        ClusterMembership membership = new ClusterMembership();
        membership.addNode("node-A");

        try {
            membership.addNode("node-A");
            throw new AssertionError("Duplicate node addition should fail");
        } catch (IllegalArgumentException expected) {
        }

        try {
            membership.addNode("");
            throw new AssertionError("Blank node ID should fail");
        } catch (IllegalArgumentException expected) {
        }

        System.out.println("TEST 2 PASSED: Cluster membership validation");
    }
}
