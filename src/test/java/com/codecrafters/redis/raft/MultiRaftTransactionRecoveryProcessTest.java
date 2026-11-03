package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actual child-JVM crash/restart coverage for committed Multi-Raft transactions and snapshots. */
final class MultiRaftTransactionRecoveryProcessTest {
    private static final class Child {
        final Path root;
        final int[] ports;
        Process process;
        BufferedReader output;
        BufferedWriter input;
        Child(Path root, int[] ports) { this.root = root; this.ports = ports; }
    }

    @Test
    void committedDecisionRecoversAfterActualCoordinatorProcessCrashAndLogReplay() throws Exception {
        runCrashScenario("commit-log", false, true);
    }

    @Test
    void abortedDecisionRecoversAfterActualProcessCrashAndReleasesReservation() throws Exception {
        runCrashScenario("abort-log", false, false);
    }

    @Test
    void snapshotRestoreRebuildsTransactionIndexLocksAndCapacityReservation() throws Exception {
        runCrashScenario("commit-snapshot", true, true);
    }

    @Test
    void processTerminationDuringBlockedRecoveryResumesFromDurableCheckpoint() throws Exception {
        String id = "worker-crash", keyA = "key-a-" + id, keyB = "key-b-" + id;
        Path root = Files.createTempDirectory("multiraft-recovery-worker-process-");
        Child child = new Child(root, reservePorts());
        try {
            start(child);
            assertEquals("PREPARED", command(child, "PREPARE " + id + " " + keyA + " " + keyB));
            assertEquals("DECIDED-COMMIT", command(child, "COMMIT " + id));
            assertEquals("PARTICIPANT-STOPPED", command(child, "STOP_PARTICIPANT"));
            assertEquals("RECOVERY-STARTED", command(child, "START_RECOVERY"));
            awaitRecoveryInProgress(child);
            String[] unavailableState = command(child, "STATE " + id + " " + keyA + " " + keyB).split(" ");
            assertEquals("true", unavailableState[3], "Unavailable participant cleared the coordinator checkpoint");
            assertEquals("true", unavailableState[4], "Unavailable participant lost its active transaction index");
            assertEquals("true", unavailableState[6], "Unavailable participant lost its reservation");
            long unrelatedReadStarted = System.nanoTime();
            assertEquals("UNRELATED-GET-OK", command(child, "UNRELATED_GET"),
                    "An unrelated request depended on the unavailable participant");
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - unrelatedReadStarted) < 2_000,
                    "An unrelated GET was blocked behind background recovery retries");

            // This is an OS process termination while the real background worker is blocked
            // forwarding transaction state to the unavailable participant group.
            assertTrue(stopAbruptly(child) != 0);
            start(child, true);
            String[] pending = command(child, "STATE " + id + " " + keyA + " " + keyB).split(" ");
            assertEquals(encoded("value-a-" + id), pending[1]);
            assertEquals(encoded("value-b-" + id), pending[2]);
            assertEquals("false", pending[3]);
            assertEquals("false", pending[4]);
            assertEquals("false", pending[6]);
            String[] resolved = command(child, "STATE " + id + " " + keyA + " " + keyB).split(" ");
            assertEquals(encoded("value-a-" + id), resolved[1]);
            assertEquals(encoded("value-b-" + id), resolved[2]);
            assertEquals("false", resolved[3]);
            assertEquals("false", resolved[4]);
            assertEquals("false", resolved[6]);
            assertEquals("UNLOCKED", command(child, "LOCK " + keyB + " " + id + " value-b-" + id));
            assertEquals("APPLY-OK", command(child, "APPLY_AGAIN " + id));
            String[] idempotent = command(child, "STATE " + id + " " + keyA + " " + keyB).split(" ");
            assertEquals(encoded("value-b-" + id), idempotent[2]);
            assertEquals("false", idempotent[3]);
            assertEquals("false", idempotent[4]);
        } finally {
            stopGracefully(child);
            deleteTree(root);
        }
    }

    private static void runCrashScenario(String id, boolean snapshot, boolean commit) throws Exception {
        Path root = Files.createTempDirectory("multiraft-transaction-process-");
        Child child = new Child(root, reservePorts());
        String keyA = "key-a-" + id, keyB = "key-b-" + id;
        try {
            start(child);
            assertEquals("PREPARED", command(child, "PREPARE " + id + " " + keyA + " " + keyB));
            assertEquals(commit ? "DECIDED-COMMIT" : "DECIDED-ABORT",
                    command(child, (commit ? "COMMIT " : "ABORT ") + id));
            if (snapshot) {
                assertTrue(command(child, "SNAPSHOT").startsWith("SNAPSHOTS "));
                assertEquals("SUFFIX-WRITTEN", command(child, "SUFFIX"));
            }

            // The child has durably committed the coordinator decision and has not applied
            // any participant entry. Terminate the JVM instead of throwing in its code.
            int crashCode = stopAbruptly(child);
            assertTrue(crashCode != 0, "The process was expected to be forcibly terminated");

            start(child);
            String pending = command(child, "STATE " + id + " " + keyA + " " + keyB);
            String[] pendingFields = pending.split(" ");
            assertEquals("STATE", pendingFields[0]);
            assertEquals("_", pendingFields[1]);
            assertEquals("_", pendingFields[2]);
            assertEquals("true", pendingFields[3], "Coordinator checkpoint missing after restart");
            assertEquals("true", pendingFields[4], "Participant active index missing after restart");
            assertEquals("true", pendingFields[5], "Coordinator capacity reservation missing after restart");
            assertEquals("true", pendingFields[6], "Participant capacity reservation missing after restart");
            if (snapshot) assertEquals(encoded("suffix-value"), pendingFields[7],
                    "Committed log suffix after the snapshot was not replayed");
            assertEquals("LOCKED", command(child, "LOCK " + keyB + " " + id + " _"),
                    "Prepared-key protection was lost on restart");
            assertEquals("RESERVED", command(child, "CAPACITY " + id),
                    "Participant capacity reservation was lost on restart");

            assertEquals("RECOVERED", command(child, "RECOVER"));
            String resolved = command(child, "STATE " + id + " " + keyA + " " + keyB);
            String[] resolvedFields = resolved.split(" ");
            assertEquals(commit ? encoded("value-a-" + id) : "_", resolvedFields[1]);
            assertEquals(commit ? encoded("value-b-" + id) : "_", resolvedFields[2]);
            assertEquals("false", resolvedFields[3], "Coordinator checkpoint was not cleaned");
            assertEquals("false", resolvedFields[4], "Participant active index was not cleaned");
            assertEquals("false", resolvedFields[5], "Coordinator capacity reservation leaked");
            assertEquals("false", resolvedFields[6], "Participant capacity reservation leaked");
            assertEquals("UNLOCKED", command(child, "LOCK " + keyB + " " + id + " "
                    + (commit ? "value-b-" + id : "_")), "Prepared-key lock remained after recovery");
            assertEquals("FREE", command(child, "CAPACITY " + id), "Capacity reservation remained after recovery");
            assertEquals(commit ? "APPLY-OK" : "ABORT-OK",
                    command(child, (commit ? "APPLY_AGAIN " : "ABORT_AGAIN ") + id));
            String[] idempotent = command(child, "STATE " + id + " " + keyA + " " + keyB).split(" ");
            assertEquals(resolvedFields[1], idempotent[1]);
            assertEquals(resolvedFields[2], idempotent[2]);
            assertEquals("false", idempotent[3]);
            assertEquals("false", idempotent[4]);
            assertEquals("false", idempotent[5]);
            assertEquals("false", idempotent[6]);
        } finally {
            stopGracefully(child);
            deleteTree(root);
        }
    }

    private static String encoded(String value) {
        return java.util.Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void start(Child child) throws Exception { start(child, false); }

    private static void start(Child child, boolean recoverOnStartup) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        List<String> command = List.of(java, "-ea", "-cp", System.getProperty("java.class.path"),
                MultiRaftTransactionRecoveryProcessNode.class.getName(), child.root.toString(),
                Integer.toString(child.ports[0]), Integer.toString(child.ports[1]), Boolean.toString(recoverOnStartup));
        child.process = new ProcessBuilder(command).redirectErrorStream(true).start();
        child.output = new BufferedReader(new InputStreamReader(child.process.getInputStream(), StandardCharsets.UTF_8));
        child.input = new BufferedWriter(new OutputStreamWriter(child.process.getOutputStream(), StandardCharsets.UTF_8));
        assertEquals("READY", readLine(child, 15));
    }

    private static String command(Child child, String value) throws Exception {
        child.input.write(value); child.input.newLine(); child.input.flush();
        String response = readLine(child, 15);
        assertFalse(response.startsWith("ERROR "), "Child command failed: " + response);
        return response;
    }

    private static void awaitRecoveryInProgress(Child child) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < deadline) {
            if (command(child, "RECOVERY_STATUS").equals("RECOVERING")) return;
            Thread.sleep(10);
        }
        throw new AssertionError("Background recovery did not reach the blocked participant lookup");
    }

    private static String readLine(Child child, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            if (child.output.ready()) {
                String line = child.output.readLine();
                if (line != null) return line;
            }
            if (!child.process.isAlive()) throw new AssertionError("Child exited with code " + child.process.exitValue());
            Thread.sleep(10);
        }
        throw new AssertionError("Timed out waiting for Multi-Raft child response");
    }

    private static int stopAbruptly(Child child) throws Exception {
        Process process = child.process;
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Could not terminate child process");
        int code = process.exitValue(); close(child); return code;
    }

    private static void stopGracefully(Child child) {
        try {
            if (child.process != null && child.process.isAlive()) {
                child.input.write("STOP"); child.input.newLine(); child.input.flush();
                child.process.waitFor(3, TimeUnit.SECONDS);
                if (child.process.isAlive()) child.process.destroyForcibly();
            }
        } catch (Exception ignored) {
            if (child.process != null) child.process.destroyForcibly();
        } finally { close(child); }
    }

    private static void close(Child child) {
        try { if (child.output != null) child.output.close(); } catch (Exception ignored) { }
        try { if (child.input != null) child.input.close(); } catch (Exception ignored) { }
        child.process = null; child.output = null; child.input = null;
    }

    private static int[] reservePorts() throws Exception {
        List<ServerSocket> sockets = new ArrayList<>();
        int[] ports = new int[2];
        try {
            for (int i = 0; i < ports.length; i++) { sockets.add(new ServerSocket(0)); ports[i] = sockets.get(i).getLocalPort(); }
        } finally { for (ServerSocket socket : sockets) socket.close(); }
        return ports;
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}
