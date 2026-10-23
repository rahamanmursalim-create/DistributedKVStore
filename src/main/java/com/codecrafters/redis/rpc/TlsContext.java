package com.codecrafters.redis.rpc;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/** Shared optional mutual-TLS context for client, HTTP, and Raft transports. */
public final class TlsContext {
    private TlsContext() { }

    public static boolean configured() {
        String path = System.getenv("KV_TLS_KEYSTORE");
        return path != null && !path.isBlank();
    }

    public static SSLContext fromEnvironment() {
        String path = System.getenv("KV_TLS_KEYSTORE");
        if (path == null || path.isBlank()) return null;
        char[] password = System.getenv().getOrDefault("KV_TLS_PASSWORD", "").toCharArray();
        if (password.length == 0) throw new IllegalStateException("KV_TLS_PASSWORD is required with KV_TLS_KEYSTORE");
        String type = System.getenv().getOrDefault("KV_TLS_STORE_TYPE", "PKCS12");
        String trustPath = System.getenv().getOrDefault("KV_TLS_TRUSTSTORE", path);
        try {
            KeyStore trust = load(Path.of(trustPath), type, password);
            return create(Path.of(path), password, trust, type, true);
        } catch (Exception e) {
            throw new IllegalStateException("Could not initialize mutual TLS from configured key/trust stores", e);
        } finally { java.util.Arrays.fill(password, '\0'); }
    }

    /** Builds a TLS context for certificate integration tests and embedded callers. */
    public static SSLContext fromStores(Path identityStore, char[] identityPassword,
                                        Path trustStore, char[] trustPassword, String type,
                                        boolean withIdentity) throws Exception {
        KeyStore trust = load(trustStore, type, trustPassword);
        return create(withIdentity ? identityStore : null, identityPassword, trust, type, withIdentity);
    }

    private static SSLContext create(Path identityPath, char[] password, KeyStore trust, String type,
                                     boolean withIdentity) throws Exception {
            KeyManagerFactory keyManagers = null;
            if (withIdentity) {
                KeyStore keys = load(identityPath, type, password);
                keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagers.init(keys, password);
            }
            TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trust);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers == null ? null : keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
            return context;
    }

    private static KeyStore load(Path path, String type, char[] password) throws Exception {
        KeyStore store = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(path)) { store.load(in, password); }
        return store;
    }
}
