package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Small child-process endpoint used by {@link RaftMultiProcessTest}. */
public final class RaftProcessNode {
    public static void main(String[] args) throws Exception {
        String nodeId = args[0];
        int nodeIndex = Integer.parseInt(args[1]);
        int[] ports = {Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4])};
        Path directory = Path.of(args[5]);
        String host = args.length > 6 ? args[6] : "127.0.0.1";
        Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
        for (int i = 0; i < ports.length; i++) if (i != nodeIndex)
            peers.put("n" + (i + 1), new InetSocketAddress(host, ports[i]));
        RedisStore store = new RedisStore(directory.resolve("state.wal"), false);
        try (RaftGroup group = new RaftGroup("process-test", nodeId,
                new InetSocketAddress(host, ports[nodeIndex]), peers, directory, store);
             BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            group.start();
            System.out.println("READY " + nodeId);
            System.out.flush();
            String line;
            while ((line = input.readLine()) != null) {
                try {
                    String[] fields = line.split(" ", 3);
                    switch (fields[0]) {
                        case "STATUS" -> System.out.println("STATUS " + group.getLeaderId() + " " + group.isLeader()
                                + " " + group.getCommitIndex() + " " + group.getTerm() + " " + group.getLastLogIndex()
                                + " " + group.getLogBaseIndex());
                        case "SET" -> {
                            group.put(fields[1], fields[2].getBytes(StandardCharsets.UTF_8));
                            System.out.println("OK");
                        }
                        case "GET" -> {
                            byte[] value = group.getWithReadIndex(fields[1]);
                            System.out.println(value == null ? "NULL"
                                    : "VALUE " + java.util.Base64.getEncoder().encodeToString(value));
                        }
                        case "LOCAL" -> {
                            byte[] value = group.get(fields[1]);
                            System.out.println(value == null ? "NULL"
                                    : "VALUE " + java.util.Base64.getEncoder().encodeToString(value));
                        }
                        case "STOP" -> { System.out.println("BYE"); System.out.flush(); return; }
                        default -> throw new IllegalArgumentException("Unknown child command");
                    }
                } catch (Exception failure) {
                    String message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
                    System.out.println("ERROR " + java.util.Base64.getEncoder().encodeToString(
                            message.getBytes(StandardCharsets.UTF_8)));
                }
                System.out.flush();
            }
        } finally {
            store.close();
        }
    }
}
