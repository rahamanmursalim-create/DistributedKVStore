
package com.codecrafters.redis;

import com.codecrafters.redis.storage.StorageEngine;
import java.nio.file.Path;

public final class VerifyReplica {
    public static void main(String[] args) throws Exception {
        Path path = Path.of("data", "replica-6380");

        try (StorageEngine store = new StorageEngine(path)) {
            String value = store.get("replication");

            System.out.println("Replica value: " + value);

            if ("hello".equals(value)) {
                System.out.println("PASS: Replication verified!");
            } else {
                System.out.println("FAIL: Expected hello, but got " + value);
            }
        }
    }
}
