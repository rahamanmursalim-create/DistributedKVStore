package com.codecrafters.redis.client;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.Server;
import com.codecrafters.redis.storage.RedisStore;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.rpc.TlsContext;

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
                byte[] binary = new byte[] {0, (byte) 0xff, (byte) 0xfe, 13, 10, 42};
                client.setBytes("client:binary", binary);
                assert java.util.Arrays.equals(binary, client.getBytes("client:binary")) : "Binary bulk value did not round-trip";
                assert client.get("missing:key") == null;
                assert client.delete("client:key", "missing:key") == 1;
                assert client.get("client:key") == null;
                try (Session protocol = new Session("127.0.0.1", port, 200, 2_000)) {
                    RespValue count = protocol.execute("COMMAND", "COUNT");
                    assert count.getType() == RespType.INTEGER && count.getIntegerValue() >= 13;
                    RespValue config = protocol.execute("CONFIG", "GET", "port");
                    assert config.getArrayValue().size() == 2;
                    assert Integer.toString(port).equals(config.getArrayValue().get(1).getStringValue());
                    RespValue info = protocol.execute("INFO", "server");
                    assert info.getStringValue().contains("tcp_port:" + port);
                    assert !info.getStringValue().contains("redis_version:7.0.0");
                    protocol.execute("SET", "client:ttl", "short", "PX", "10000");
                    RespValue keyspace = protocol.execute("INFO", "keyspace");
                    assert keyspace.getStringValue().contains("expires=1") : keyspace.getStringValue();
                    RespValue unknownConfig = protocol.execute("CONFIG", "GET", "no-such-setting");
                    assert unknownConfig.getType() == RespType.ARRAY && unknownConfig.getArrayValue().isEmpty();
                }
            }
        } finally {
            server.stop();
            serverThread.join(2_000);
            server.close();
        }
        testAuthAndTlsClient();
        System.out.println("KVClientIntegrationTest passed");
    }

    private static void testAuthAndTlsClient() throws Exception {
        Path dir = Files.createTempDirectory("redis-client-tls");
        char[] password = "changeit-client".toCharArray();
        try {
            Path identity = dir.resolve("identity.p12");
            Process process = new ProcessBuilder("keytool", "-genkeypair", "-alias", "server", "-keyalg", "RSA",
                    "-storetype", "PKCS12", "-keystore", identity.toString(), "-storepass", new String(password),
                    "-keypass", new String(password), "-dname", "CN=127.0.0.1", "-ext", "SAN=ip:127.0.0.1",
                    "-validity", "2", "-noprompt").redirectErrorStream(true).start();
            if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0)
                throw new AssertionError("Could not create test TLS certificate");
            SSLContext serverTls = TlsContext.fromStores(identity, password, identity, password, "PKCS12", true);
            SSLContext trustedTls = TlsContext.fromStores(null, password, identity, password, "PKCS12", false);
            int port;
            try (ServerSocket reservation = new ServerSocket(0)) { port = reservation.getLocalPort(); }
            Server server = new Server(port, new RedisStore(), "integration-secret", serverTls);
            Thread thread = new Thread(() -> { try { server.start(); } catch (Exception e) { throw new RuntimeException(e); } });
            thread.setDaemon(true);
            thread.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            try (KVClient client = new KVClientImpl("127.0.0.1", port, true, trustedTls, "default", "integration-secret")) {
                while (true) {
                    try { client.connect(); break; }
                    catch (java.io.IOException e) {
                        if (System.nanoTime() >= deadline) throw e;
                        Thread.sleep(20);
                    }
                }
                client.set("tls:key", "authenticated");
                assert "authenticated".equals(client.get("tls:key"));
            } finally {
                server.stop(); thread.join(2_000); server.close();
            }
        } finally {
            try (var paths = Files.walk(dir)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> { try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); } });
            }
        }
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
