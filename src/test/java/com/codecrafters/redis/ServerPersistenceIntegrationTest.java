package com.codecrafters.redis;

import com.codecrafters.redis.protocol.RespReader;
import com.codecrafters.redis.protocol.RespValue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * End-to-end integration tests verifying that Redis TCP clients writing to Server
 * persist data to disk, and that after server stop and restart, the data remains
 * readable over TCP.
 */
public final class ServerPersistenceIntegrationTest {

    private ServerPersistenceIntegrationTest() {
    }

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("server-persist-test-");

        try {
            testPersistenceAcrossServerRestart(root);
            testPipeliningAndSequentialRequests(root);

            System.out.println("\nALL SERVER PERSISTENCE INTEGRATION TESTS PASSED");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testPersistenceAcrossServerRestart(Path root) throws Exception {
        Path storeDir = root.resolve("store");
        int port = findFreePort();

        // Phase 1: Start server, write keys over TCP, then stop server.
        Server server1 = new Server(port, storeDir);
        Thread t1 = new Thread(() -> {
            try {
                server1.start();
            } catch (Exception ignored) {
            }
        });
        t1.setDaemon(true);
        t1.start();
        Thread.sleep(150);

        try {
            try (Socket socket = new Socket("127.0.0.1", port)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                // Write key1
                send(out, "*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$8\r\nMursalim\r\n");
                RespValue res1 = reader.read();
                check("OK".equals(res1.getStringValue()), "SET user returned OK");

                // Write key2 with flush to SSTable
                send(out, "*3\r\n$3\r\nSET\r\n$4\r\nlang\r\n$4\r\nJava\r\n");
                RespValue res2 = reader.read();
                check("OK".equals(res2.getStringValue()), "SET lang returned OK");
            }
        } finally {
            server1.stop();
            t1.join(2000);
        }

        // Phase 2: Start new server instance on same store directory and verify keys over TCP.
        Server server2 = new Server(port, storeDir);
        Thread t2 = new Thread(() -> {
            try {
                server2.start();
            } catch (Exception ignored) {
            }
        });
        t2.setDaemon(true);
        t2.start();
        Thread.sleep(150);

        try {
            try (Socket socket = new Socket("127.0.0.1", port)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                // GET user
                send(out, "*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n");
                RespValue res1 = reader.read();
                check("Mursalim".equals(res1.getStringValue()), "Recovered user should be Mursalim");

                // GET lang
                send(out, "*2\r\n$3\r\nGET\r\n$4\r\nlang\r\n");
                RespValue res2 = reader.read();
                check("Java".equals(res2.getStringValue()), "Recovered lang should be Java");

                // DEL user
                send(out, "*2\r\n$3\r\nDEL\r\n$4\r\nuser\r\n");
                RespValue res3 = reader.read();
                check(res3.getIntegerValue() != null && res3.getIntegerValue() == 1, "DEL user returned 1");

                // GET deleted user
                send(out, "*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n");
                RespValue res4 = reader.read();
                check(res4.isNull(), "GET deleted user should be null bulk string");
            }
        } finally {
            server2.stop();
            t2.join(2000);
        }

        System.out.println("PASS: Data persists and recovers across live TCP server restart");
    }

    private static void testPipeliningAndSequentialRequests(Path root) throws Exception {
        Path storeDir = root.resolve("pipeline-store");
        int port = findFreePort();

        Server server = new Server(port, storeDir);
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (Exception ignored) {
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(150);

        try {
            try (Socket socket = new Socket("127.0.0.1", port)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                // Send 3 requests in a single TCP write (pipelining)
                String batch = "*1\r\n$4\r\nPING\r\n"
                        + "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2\r\n42\r\n"
                        + "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n";
                send(out, batch);

                RespValue r1 = reader.read();
                check("PONG".equals(r1.getStringValue()), "Pipelined PING returned PONG");

                RespValue r2 = reader.read();
                check("OK".equals(r2.getStringValue()), "Pipelined SET returned OK");

                RespValue r3 = reader.read();
                check("42".equals(r3.getStringValue()), "Pipelined GET returned 42");
            }
        } finally {
            server.stop();
            t.join(2000);
        }

        System.out.println("PASS: Sequential requests and pipelining over persistent TCP connection");
    }

    private static void send(OutputStream out, String respCommand) throws Exception {
        out.write(respCommand.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception ignored) {
                            }
                        });
            }
        }
    }
}
