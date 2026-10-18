package com.codecrafters.redis;

import com.codecrafters.redis.distributed.ReplicationServer;
import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.nio.file.Path;

public final class ReplicaMain {

    private static final int DEFAULT_REPLICA_PORT = 6380;

    private ReplicaMain() {
        // Utility class.
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0
                ? Integer.parseInt(args[0])
                : DEFAULT_REPLICA_PORT;

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535");
        }

        Path storageDirectory = Path.of(
                "data",
                "replica-" + port
        );

        StorageEngine store = new StorageEngine(storageDirectory);
        ReplicationServer replica;

        try {
            replica = new ReplicationServer(port, store);
        } catch (IOException | RuntimeException e) {
            try {
                store.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                replica.close();
            } catch (IOException e) {
                System.err.println(
                        "Could not close replica receiver: "
                                + e.getMessage());
            }

            try {
                store.close();
            } catch (IOException e) {
                System.err.println(
                        "Could not close replica storage: "
                                + e.getMessage());
            }
        }));

        System.out.println(
                "Replica receiver listening on port " + port);

        System.out.println(
                "Replica storage: "
                        + storageDirectory.toAbsolutePath());

        // This call accepts incoming replication requests.
        replica.start();
    }
}