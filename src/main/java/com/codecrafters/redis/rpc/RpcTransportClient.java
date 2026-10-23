package com.codecrafters.redis.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.util.Objects;

/** Synchronous one-request-per-connection RPC client. */
public final class RpcTransportClient {
    private final InetSocketAddress address;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final SSLContext configuredTls;

    public RpcTransportClient(InetSocketAddress address, int connectTimeoutMillis,
                              int readTimeoutMillis) {
        this(address, connectTimeoutMillis, readTimeoutMillis, null);
    }

    public RpcTransportClient(InetSocketAddress address, int connectTimeoutMillis,
                              int readTimeoutMillis, SSLContext tlsContext) {
        this.address = Objects.requireNonNull(address, "address");
        if (connectTimeoutMillis <= 0 || readTimeoutMillis <= 0)
            throw new IllegalArgumentException("RPC timeouts must be positive");
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.configuredTls = tlsContext;
    }

    public RpcMessage call(RpcMessage request) throws IOException {
        Objects.requireNonNull(request, "request");
        try (Socket socket = createSocket()) {
            socket.connect(address, connectTimeoutMillis);
            if (socket instanceof SSLSocket ssl) ssl.startHandshake();
            socket.setSoTimeout(readTimeoutMillis);
            try (DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                 DataInputStream input = new DataInputStream(socket.getInputStream())) {
                RpcCodec.write(output, request);
                RpcMessage response = RpcCodec.read(input);
                if (!request.getRequestId().equals(response.getRequestId()))
                    throw new IOException("RPC response request ID does not match");
                return response;
            }
        }
    }

    private Socket createSocket() throws IOException {
        SSLContext context = configuredTls == null ? TlsContext.fromEnvironment() : configuredTls;
        return context == null ? new Socket() : context.getSocketFactory().createSocket();
    }
}
