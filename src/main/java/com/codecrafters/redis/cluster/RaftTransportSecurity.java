package com.codecrafters.redis.cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;

/** Fail-closed transport policy for Raft listeners and peers. */
public final class RaftTransportSecurity {
    private RaftTransportSecurity() { }

    public static void validate(InetSocketAddress bindAddress,
                                Collection<InetSocketAddress> peerAddresses,
                                boolean tlsConfigured, String clusterToken) {
        if (bindAddress == null || bindAddress.isUnresolved())
            throw new IllegalArgumentException("Raft bind address must resolve");
        boolean exposed = requiresProtection(bindAddress.getAddress());
        if (peerAddresses != null) {
            for (InetSocketAddress peer : peerAddresses) {
                if (peer == null || peer.isUnresolved())
                    throw new IllegalArgumentException("Raft peer address must resolve");
                exposed |= !peer.getAddress().isLoopbackAddress();
            }
        }
        if (!exposed) return;
        if (!tlsConfigured)
            throw new IllegalArgumentException("Wildcard or non-loopback Raft transport requires mutual TLS (KV_TLS_KEYSTORE/KV_TLS_TRUSTSTORE)");
        if (clusterToken == null || clusterToken.isBlank())
            throw new IllegalArgumentException("Wildcard or non-loopback Raft transport requires KV_CLUSTER_TOKEN authentication");
    }

    public static boolean requiresProtection(InetAddress bindAddress) {
        return bindAddress.isAnyLocalAddress() || !bindAddress.isLoopbackAddress();
    }

    public static InetSocketAddress resolve(String host, int port) {
        try { return new InetSocketAddress(InetAddress.getByName(host), port); }
        catch (UnknownHostException e) { throw new IllegalArgumentException("Raft host does not resolve: " + host, e); }
    }
}
