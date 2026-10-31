package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.web.WebServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Verifies one authenticated coordinator can migrate data and live routers adopt its Raft manifest. */
public final class RaftRebalanceAdminTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-rebalance-admin");
        RedisStore base = new RedisStore(root.resolve("base.wal"), false);
        RedisStore storeA = new RedisStore(root.resolve("a.wal"), false);
        RedisStore storeB = new RedisStore(root.resolve("b.wal"), false);
        RaftGroup groupA = null, groupB = null;
        MultiRaftStore multi = null;
        try {
            groupA = new RaftGroup("a", "n1", new InetSocketAddress("127.0.0.1", 0),
                    Map.of(), root.resolve("group-a"), storeA);
            groupB = new RaftGroup("b", "n1", new InetSocketAddress("127.0.0.1", 0),
                    Map.of(), root.resolve("group-b"), storeB);
            groupA.start(); groupB.start();
            awaitLeaders(groupA, groupB);
            multi = new MultiRaftStore(Map.of("a", groupA, "b", groupB), 64, false);
            String moved = null;
            for (int i = 0; moved == null; i++) {
                String candidate = "admin-move-" + i;
                if ("b".equals(multi.groupFor(candidate))) moved = candidate;
            }
            multi.set(moved, "preserved");
            MultiRaftStore liveRouter = new MultiRaftStore(Map.of("a", groupA, "b", groupB), 64, false);
            try (WebServer web = new WebServer(base, multi, 0, null, false, null, null, null, "admin-secret")) {
                web.start();
                HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
                URI endpoint = URI.create("http://127.0.0.1:" + web.getPort() + "/api/raft/rebalance");
                HttpResponse<String> denied = client.send(HttpRequest.newBuilder(endpoint)
                        .POST(HttpRequest.BodyPublishers.ofString("a")).build(), HttpResponse.BodyHandlers.ofString());
                if (denied.statusCode() != 401) throw new AssertionError("Unauthenticated rebalance request was not rejected");
                HttpResponse<String> accepted = client.send(HttpRequest.newBuilder(endpoint)
                        .header("X-Cluster-Admin-Token", "admin-secret")
                        .POST(HttpRequest.BodyPublishers.ofString("a")).build(), HttpResponse.BodyHandlers.ofString());
                if (accepted.statusCode() != 200) throw new AssertionError("Rebalance coordinator failed: " + accepted.statusCode() + " " + accepted.body());
            }
            if (!"a".equals(liveRouter.groupFor(moved)))
                throw new AssertionError("Live router did not adopt committed ownership after remote admin migration");
            if (!"preserved".equals(liveRouter.get(moved)))
                throw new AssertionError("RPC-driven admin migration lost the moved value");
            System.out.println("RaftRebalanceAdminTest passed: authenticated coordinator, quorum migration, live router adoption");
        } finally {
            if (multi != null) try { multi.close(); } catch (RuntimeException ignored) { }
            else {
                if (groupA != null) try { groupA.close(); } catch (Exception ignored) { }
                if (groupB != null) try { groupB.close(); } catch (Exception ignored) { }
            }
            base.close(); storeA.close(); storeB.close();
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    private static void awaitLeaders(RaftGroup... groups) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            boolean ready = true;
            for (RaftGroup group : groups) if (!group.isLeader()) ready = false;
            if (ready) return;
            Thread.sleep(20);
        }
        throw new AssertionError("Single-voter groups did not elect their local coordinator");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
