package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Child JVM used to prove Multi-Raft transaction state survives actual process termination. */
public final class MultiRaftTransactionRecoveryProcessNode {
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        int coordinatorPort = Integer.parseInt(args[1]);
        int participantPort = Integer.parseInt(args[2]);
        boolean recoverOnStartup = args.length > 3 && Boolean.parseBoolean(args[3]);
        RedisStore coordinatorStore = new RedisStore(root.resolve("coordinator/state.wal"), false);
        RedisStore participantStore = new RedisStore(root.resolve("participant/state.wal"), false);
        RaftGroup coordinator = open("coordinator", coordinatorPort, root.resolve("coordinator"), coordinatorStore);
        RaftGroup participant = open("participant", participantPort, root.resolve("participant"), participantStore);
        MultiRaftStore router = null;
        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            coordinator.start(); participant.start();
            awaitLeader(coordinator); awaitLeader(participant);
            if (recoverOnStartup) {
                router = new MultiRaftStore(Map.of("coordinator", coordinator, "participant", participant), 64, false);
                router.recoverPendingTransactions();
            }
            System.out.println("READY"); System.out.flush();
            String line;
            while ((line = input.readLine()) != null) {
                String result = null;
                try {
                    String[] fields = line.split(" ");
                    result = switch (fields[0]) {
                        case "PREPARE" -> {
                            String id = fields[1], keyA = fields[2], keyB = fields[3];
                            List<String> participants = List.of("coordinator", "participant");
                            String digest = "digest-" + id;
                            coordinator.beginTransaction(id, digest, participants);
                            coordinator.prepareTransaction(id, digest, participants, Map.of(keyA, "value-a-" + id));
                            participant.prepareTransaction(id, digest, participants, Map.of(keyB, "value-b-" + id));
                            yield "PREPARED";
                        }
                        case "COMMIT", "ABORT" -> {
                            String id = fields[1];
                            coordinator.decideTransaction(id, "digest-" + id,
                                    List.of("coordinator", "participant"), fields[0].equals("COMMIT"));
                            yield "DECIDED-" + fields[0];
                        }
                        case "APPLY_AGAIN" -> {
                            participant.applyCommittedTransaction(fields[1], "digest-" + fields[1]);
                            yield "APPLY-OK";
                        }
                        case "ABORT_AGAIN" -> {
                            participant.abortPreparedTransaction(fields[1], "digest-" + fields[1]);
                            yield "ABORT-OK";
                        }
                        case "SNAPSHOT" -> "SNAPSHOTS " + coordinator.createSnapshot() + " " + participant.createSnapshot();
                        case "SUFFIX" -> { coordinator.put("post-snapshot-suffix", "suffix-value".getBytes(StandardCharsets.UTF_8)); yield "SUFFIX-WRITTEN"; }
                        case "STATE" -> state(coordinator, participant, fields[1], fields[2], fields[3]);
                        case "LOCK" -> probeLock(participant, fields[1], fields[2], fields[3]);
                        case "CAPACITY" -> hasReservation(participantStore, fields[1]) ? "RESERVED" : "FREE";
                        case "STOP_PARTICIPANT" -> { participant.close(); yield "PARTICIPANT-STOPPED"; }
                        case "START_RECOVERY" -> {
                            if (router == null) router = new MultiRaftStore(Map.of(
                                    "coordinator", coordinator, "participant", participant), 64, false);
                            yield "RECOVERY-STARTED";
                        }
                        case "RECOVERY_STATUS" -> recoveryInProgress() ? "RECOVERING" : "NOT-IN-PROGRESS";
                        case "UNRELATED_GET" -> {
                            if (router == null) throw new IllegalStateException("Recovery router has not started");
                            String key = null;
                            for (int i = 0; i < 10_000; i++) {
                                String candidate = "unrelated-read-" + i;
                                if (router.groupFor(candidate).equals("coordinator")) { key = candidate; break; }
                            }
                            if (key == null) throw new IllegalStateException("Could not route test key to coordinator");
                            yield router.get(key) == null ? "UNRELATED-GET-OK" : "UNRELATED-GET-UNEXPECTED-VALUE";
                        }
                        case "RECOVER" -> {
                            if (router == null) router = new MultiRaftStore(Map.of(
                                    "coordinator", coordinator, "participant", participant), 64, false);
                            router.recoverPendingTransactions();
                            yield "RECOVERED";
                        }
                        case "PUT" -> {
                            RaftGroup target = fields[1].equals("A") ? coordinator : participant;
                            target.put(fields[2], fields[3].getBytes(StandardCharsets.UTF_8));
                            yield "PUT-OK";
                        }
                        case "STOP" -> "BYE";
                        default -> throw new IllegalArgumentException("Unknown command " + fields[0]);
                    };
                    System.out.println(result);
                } catch (Exception failure) {
                    String message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
                    System.out.println("ERROR " + java.util.Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
                }
                System.out.flush();
                if ("BYE".equals(result)) return;
            }
        } finally {
            if (router != null) router.close();
            else {
                try { coordinator.close(); } finally { participant.close(); }
            }
            coordinatorStore.close(); participantStore.close();
        }
    }

    private static boolean recoveryInProgress() {
        for (Map.Entry<Thread, StackTraceElement[]> thread : Thread.getAllStackTraces().entrySet()) {
            if (!thread.getKey().getName().equals("raft-migration-recovery")) continue;
            for (StackTraceElement frame : thread.getValue())
                if (frame.getClassName().equals(MultiRaftStore.class.getName())
                        && frame.getMethodName().equals("retryTransactionRead")
                        || frame.getClassName().equals(RaftGroup.class.getName())
                        && frame.getMethodName().equals("awaitLeaderEndpoint")) return true;
        }
        return false;
    }

    private static RaftGroup open(String id, int port, Path directory, RedisStore store) throws Exception {
        return new RaftGroup(id, "local", new InetSocketAddress("127.0.0.1", port),
                Map.of(), directory, store);
    }

    private static void awaitLeader(RaftGroup group) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (group.isLeader() && "local".equals(group.getLeaderId())) return;
            Thread.sleep(10);
        }
        throw new AssertionError("Single-voter group did not elect its leader: " + group.getGroupId());
    }

    private static String state(RaftGroup coordinator, RaftGroup participant,
                                String id, String keyA, String keyB) {
        byte[] a = coordinator.get(keyA), b = participant.get(keyB);
        return "STATE " + value(a) + " " + value(b) + " "
                + coordinator.hasUnresolvedLocalTransactions() + " "
                + participant.hasUnresolvedLocalTransactions() + " "
                + hasReservation(storeOf(coordinator), id) + " "
                + hasReservation(storeOf(participant), id) + " "
                + value(coordinator.get("post-snapshot-suffix"));
    }

    private static String value(byte[] value) {
        return value == null ? "_" : java.util.Base64.getEncoder().encodeToString(value);
    }

    private static String probeLock(RaftGroup participant, String key, String id, String original) throws Exception {
        try {
            participant.put(key, ("probe-" + id).getBytes(StandardCharsets.UTF_8));
            if (original.equals("_")) participant.delete(key);
            else participant.put(key, original.getBytes(StandardCharsets.UTF_8));
            return "UNLOCKED";
        } catch (java.io.IOException expected) {
            if (expected.getMessage() != null && expected.getMessage().contains("prepared transaction")) return "LOCKED";
            throw expected;
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean hasReservation(RedisStore store, String id) {
        try {
            var field = RedisStore.class.getDeclaredField("capacityReservations");
            field.setAccessible(true);
            return ((Map<String, ?>) field.get(store)).containsKey(id);
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not inspect prepared capacity reservation", failure);
        }
    }

    private static RedisStore storeOf(RaftGroup group) {
        try {
            var field = RaftGroup.class.getDeclaredField("store");
            field.setAccessible(true);
            return (RedisStore) field.get(group);
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not inspect Raft participant storage", failure);
        }
    }
}
