package com.codecrafters.redis.client;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.Server;
import com.codecrafters.redis.storage.RedisStore;
import java.net.ServerSocket;

/** Exercises the public client against the existing RESP server over real TCP. */
public final class KVClientIntegrationTest {
    public static void main(String[] args) throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) { port = reservation.getLocalPort(); }
        Server server = new Server(port, new RedisStore());
        Thread serverThread = new Thread(() -> {
            try { server.start(); }
            catch (Exception e) { if (!Thread.currentThread().isInterrupted()) throw new RuntimeException(e); }
        }, "kv-client-integration-server");
        serverThread.setDaemon(true);
        serverThread.start();
        try {
            long deadline = System.nanoTime() + 5_000_000_000L;
            try (KVClient client = new KVClientImpl("127.0.0.1", port, 200, 2_000)) {
                while (true) {
                    try { client.connect(); break; }
                    catch (java.io.IOException e) {
                        if (System.nanoTime() >= deadline) throw e;
                        Thread.sleep(20);
                    }
                }
                client.set("client:key", "value with spaces");
                assert "value with spaces".equals(client.get("client:key"));
                assert client.get("missing:key") == null;
                assert client.delete("client:key", "missing:key") == 1;
                assert client.get("client:key") == null;
            }
        } finally {
            server.stop();
            serverThread.join(2_000);
            server.close();
        }
        System.out.println("KVClientIntegrationTest passed");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
