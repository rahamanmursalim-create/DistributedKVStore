package com.codecrafters.redis.rpc;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/** Certificate-backed tests for mutual TLS, untrusted/missing identity and plaintext rejection. */
public final class TlsTransportTest {
    private static final char[] PASSWORD = "changeit-test".toCharArray();

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("rpc-mtls-test");
        try {
            Path trusted = dir.resolve("trusted.p12"), untrusted = dir.resolve("untrusted.p12");
            keytool(trusted, "trusted"); keytool(untrusted, "untrusted");
            SSLContext server = TlsContext.fromStores(trusted, PASSWORD, trusted, PASSWORD, "PKCS12", true);
            SSLContext client = TlsContext.fromStores(trusted, PASSWORD, trusted, PASSWORD, "PKCS12", true);
            SSLContext noIdentity = TlsContext.fromStores(null, PASSWORD, trusted, PASSWORD, "PKCS12", false);
            SSLContext wrongTrust = TlsContext.fromStores(untrusted, PASSWORD, untrusted, PASSWORD, "PKCS12", true);
            try (RpcTransportServer rpc = new RpcTransportServer(new InetSocketAddress("127.0.0.1", 0), 2, 1500,
                    request -> new RpcMessage(RpcMessage.Type.RESPONSE, request.getRequestId(),
                            "server", request.getSourceNodeId(), request.getPayload()), server)) {
                rpc.start();
                InetSocketAddress address = new InetSocketAddress("127.0.0.1", rpc.getPort());
                RpcMessage request = new RpcMessage(RpcMessage.Type.REQUEST, UUID.randomUUID().toString(),
                        "client", "server", new byte[] {1, 2, 3});
                RpcMessage response = new RpcTransportClient(address, 1000, 1500, client).call(request);
                assert java.util.Arrays.equals(request.getPayload(), response.getPayload());
                assertFails(() -> new RpcTransportClient(address, 1000, 1500, noIdentity).call(request), "missing client certificate");
                assertFails(() -> new RpcTransportClient(address, 1000, 1500, wrongTrust).call(request), "untrusted server certificate");
                try (Socket plaintext = new Socket()) {
                    plaintext.connect(address, 1000); plaintext.setSoTimeout(1500);
                    boolean rejected = false;
                    try {
                        RpcCodec.write(new java.io.DataOutputStream(plaintext.getOutputStream()), request);
                        RpcCodec.read(new java.io.DataInputStream(plaintext.getInputStream()));
                    } catch (Exception expected) { rejected = true; }
                    assert rejected : "plaintext was accepted by TLS-only listener";
                }
            }
            System.out.println("TlsTransportTest passed");
        } finally {
            try (var paths = Files.walk(dir)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
                });
            }
        }
    }

    private static void keytool(Path file, String alias) throws Exception {
        String executable = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool").toString();
        Process process = new ProcessBuilder(executable, "-genkeypair", "-alias", alias,
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", file.toString(), "-storepass", new String(PASSWORD),
                "-keypass", new String(PASSWORD), "-noprompt").redirectErrorStream(true).start();
        byte[] output = process.getInputStream().readAllBytes();
        if (process.waitFor() != 0) throw new IllegalStateException("keytool failed: " + new String(output));
    }

    private static void assertFails(ThrowingAction action, String description) throws Exception {
        boolean failed = false;
        try { action.run(); } catch (Exception expected) { failed = true; }
        assert failed : description + " unexpectedly succeeded";
    }

    @FunctionalInterface private interface ThrowingAction { void run() throws Exception; }
}
