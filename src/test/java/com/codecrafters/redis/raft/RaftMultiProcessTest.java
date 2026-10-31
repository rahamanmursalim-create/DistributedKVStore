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
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Exercises Raft election, failover, and restart across independent JVM processes. */
public final class RaftMultiProcessTest {
    private static final int NODE_COUNT = 3;

    private static final class Node {
        final int index;
        final Path data;
        Process process;
        BufferedReader output;
        BufferedWriter input;
        Node(int index, Path data) { this.index = index; this.data = data; }
    }

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("KV_TEST_HOST", "127.0.0.1");
        String[] configuredHosts = System.getenv().getOrDefault("KV_TEST_HOSTS", "").split(",");
        if (configuredHosts.length == NODE_COUNT && !configuredHosts[0].isBlank())
            host = configuredHosts[0].trim(); // Multi-host orchestration is external; see physical-cluster runbook.
        Path root = Files.createTempDirectory("raft-multiprocess-test");
        int[] ports = reservePorts();
        List<Node> nodes = new ArrayList<>();
        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        String classpath = System.getProperty("java.class.path");
        try {
            for (int i = 0; i < NODE_COUNT; i++) nodes.add(new Node(i, root.resolve("node-" + (i + 1))));
            for (Node node : nodes) start(node, java, classpath, ports, host);
            for (Node node : nodes) expect(node, "READY n" + (node.index + 1));

            Node leader = awaitLeader(nodes, 15);
            command(leader, "SET stable before-failure", "OK");
            if (Boolean.parseBoolean(System.getenv().getOrDefault("KV_TEST_SKIP_FAILURE", "false"))) {
                for (Node node : nodes)
                    commandEventually(node, "GET stable", "VALUE YmVmb3JlLWZhaWx1cmU=", 10);
                System.out.println("RaftMultiProcessTest passed: three JVMs replicated over " + host + " interface");
                return;
            }
            int failedLeader = leader.index;
            stopAbruptly(leader);

            List<Node> survivors = nodes.stream().filter(node -> node.process != null).toList();
            Node replacement = awaitLeader(survivors, 15);
            command(replacement, "SET recovered after-failure", "OK");
            for (Node survivor : survivors)
                commandEventually(survivor, "GET recovered", "VALUE YWZ0ZXItZmFpbHVyZQ==", 10);

            Node restarted = nodes.get(failedLeader);
            start(restarted, java, classpath, ports, host);
            expect(restarted, "READY n" + (failedLeader + 1));
            awaitLeader(nodes, 15);
            commandEventually(restarted, "LOCAL recovered", "VALUE YWZ0ZXItZmFpbHVyZQ==", 30, nodes);
            commandEventually(restarted, "LOCAL stable", "VALUE YmVmb3JlLWZhaWx1cmU=", 30, nodes);
            System.out.println("RaftMultiProcessTest passed: separate JVMs, leader failure, quorum failover, restart catch-up");
        } finally {
            for (Node node : nodes) stopGracefully(node);
            deleteTree(root);
        }
    }

    private static void start(Node node, String java, String classpath, int[] ports, String host) throws Exception {
        List<String> command = new ArrayList<>(List.of(java, "-ea", "-cp", classpath,
                RaftProcessNode.class.getName(), "n" + (node.index + 1), Integer.toString(node.index),
                Integer.toString(ports[0]), Integer.toString(ports[1]), Integer.toString(ports[2]),
                node.data.toString(), host));
        String forceDelay = System.getProperty("kv.test.forceDelayMillis");
        if (forceDelay != null && !forceDelay.isBlank()) command.add(1, "-Dkv.test.forceDelayMillis=" + forceDelay);
        node.process = new ProcessBuilder(command).redirectErrorStream(true).start();
        node.output = new BufferedReader(new InputStreamReader(node.process.getInputStream(), StandardCharsets.UTF_8));
        node.input = new BufferedWriter(new OutputStreamWriter(node.process.getOutputStream(), StandardCharsets.UTF_8));
    }

    private static Node awaitLeader(List<Node> nodes, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            Node elected = null;
            for (Node node : nodes) {
                if (node.process == null || !node.process.isAlive()) continue;
                send(node, "STATUS");
                String response = readLine(node, deadline);
                String[] fields = response.split(" ");
                if (elected == null && fields.length >= 3 && fields[0].equals("STATUS") && fields[2].equals("true"))
                    elected = node;
            }
            if (elected != null) {
                Thread.sleep(100);
                send(elected, "STATUS");
                String[] confirmation = readLine(elected, deadline).split(" ");
                if (confirmation.length >= 3 && confirmation[0].equals("STATUS")
                        && confirmation[1].equals("n" + (elected.index + 1)) && confirmation[2].equals("true"))
                    return elected;
            }
            Thread.sleep(40);
        }
        throw new AssertionError("No process-local Raft leader elected within " + seconds + " seconds");
    }

    private static void command(Node node, String command, String expected) throws Exception {
        String response = commandResult(node, command);
        if (!expected.equals(response)) throw new AssertionError("Expected " + expected + " but got " + response);
    }

    private static String commandResult(Node node, String command) throws Exception {
        send(node, command);
        return readLine(node, System.nanoTime() + TimeUnit.SECONDS.toNanos(8));
    }

    private static void commandEventually(Node node, String command, String expected, int seconds) throws Exception {
        commandEventually(node, command, expected, seconds, List.of(node));
    }

    private static void commandEventually(Node node, String command, String expected, int seconds, List<Node> diagnostics)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        String last = "no response";
        while (System.nanoTime() < deadline) {
            send(node, command);
            last = readLine(node, deadline);
            if (expected.equals(last)) return;
            Thread.sleep(50);
        }
        List<String> status = new ArrayList<>();
        for (Node candidate : diagnostics) {
            if (candidate.process == null || !candidate.process.isAlive()) {
                status.add("n" + (candidate.index + 1) + "=stopped");
                continue;
            }
            try { status.add("n" + (candidate.index + 1) + "=" + commandResult(candidate, "STATUS")); }
            catch (Exception failure) { status.add("n" + (candidate.index + 1) + "=status-error:" + failure.getMessage()); }
        }
        throw new AssertionError("Expected " + expected + " after catch-up but got " + last + "; cluster=" + status);
    }

    private static void expect(Node node, String expected) throws Exception {
        String response = readLine(node, System.nanoTime() + TimeUnit.SECONDS.toNanos(8));
        if (!expected.equals(response)) throw new AssertionError("Expected " + expected + " but got " + response);
    }

    private static void send(Node node, String line) throws Exception {
        node.input.write(line); node.input.newLine(); node.input.flush();
    }

    private static String readLine(Node node, long deadline) throws Exception {
        while (System.nanoTime() < deadline) {
            if (node.output.ready()) {
                String line = node.output.readLine();
                if (line != null) return line;
            }
            if (!node.process.isAlive()) throw new AssertionError("Raft child exited: " + node.process.exitValue());
            Thread.sleep(10);
        }
        throw new AssertionError("Timed out waiting for child node " + (node.index + 1));
    }

    private static void stopAbruptly(Node node) throws Exception {
        node.process.destroyForcibly();
        if (!node.process.waitFor(5, TimeUnit.SECONDS)) throw new AssertionError("Could not stop Raft leader process");
        close(node);
    }

    private static void stopGracefully(Node node) {
        try {
            if (node.process != null && node.process.isAlive()) {
                send(node, "STOP");
                node.process.waitFor(3, TimeUnit.SECONDS);
                if (node.process.isAlive()) node.process.destroyForcibly();
            }
        } catch (Exception ignored) {
            if (node.process != null) node.process.destroyForcibly();
        } finally { close(node); }
    }

    private static void close(Node node) {
        try { if (node.output != null) node.output.close(); } catch (Exception ignored) { }
        try { if (node.input != null) node.input.close(); } catch (Exception ignored) { }
        node.process = null; node.output = null; node.input = null;
    }

    private static int[] reservePorts() throws Exception {
        int[] ports = new int[NODE_COUNT];
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            for (int i = 0; i < NODE_COUNT; i++) {
                ServerSocket socket = new ServerSocket(0);
                sockets.add(socket); ports[i] = socket.getLocalPort();
            }
        } finally { for (ServerSocket socket : sockets) socket.close(); }
        return ports;
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList())
                Files.deleteIfExists(path);
        }
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
