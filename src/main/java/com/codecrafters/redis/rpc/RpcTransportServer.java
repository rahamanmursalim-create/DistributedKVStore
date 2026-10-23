package com.codecrafters.redis.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;

/** Length-framed, request/reply RPC server built on the project's RpcCodec. */
public final class RpcTransportServer implements AutoCloseable {
    @FunctionalInterface
    public interface Handler {
        RpcMessage handle(RpcMessage request) throws Exception;
    }

    private final InetSocketAddress address;
    private final int socketTimeoutMillis;
    private final Handler handler;
    private final SSLContext configuredTls;
    private final ThreadPoolExecutor workers;
    private final AtomicBoolean running = new AtomicBoolean();
    private ServerSocket serverSocket;
    private Thread acceptThread;

    public RpcTransportServer(InetSocketAddress address, int workerCount,
                              int socketTimeoutMillis, Handler handler) {
        this(address, workerCount, socketTimeoutMillis, handler, null);
    }

    public RpcTransportServer(InetSocketAddress address, int workerCount,
                              int socketTimeoutMillis, Handler handler, SSLContext tlsContext) {
        this.address = Objects.requireNonNull(address, "address");
        if (workerCount <= 0 || socketTimeoutMillis <= 0)
            throw new IllegalArgumentException("worker count and timeout must be positive");
        this.socketTimeoutMillis = socketTimeoutMillis;
        this.handler = Objects.requireNonNull(handler, "handler");
        this.configuredTls = tlsContext;
        this.workers = new ThreadPoolExecutor(workerCount, workerCount, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(256), task -> {
                    Thread thread = new Thread(task, "rpc-transport-worker");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public synchronized void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;
        try {
            SSLContext tls = configuredTls == null ? TlsContext.fromEnvironment() : configuredTls;
            serverSocket = tls == null ? new ServerSocket() : tls.getServerSocketFactory().createServerSocket();
            if (serverSocket instanceof SSLServerSocket ssl) ssl.setNeedClientAuth(true);
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address);
            acceptThread = new Thread(this::acceptLoop, "rpc-transport-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        } catch (IOException e) {
            running.set(false);
            workers.shutdownNow();
            throw e;
        }
    }

    public int getPort() {
        if (serverSocket == null) throw new IllegalStateException("RPC server is not started");
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(socketTimeoutMillis);
                workers.execute(() -> handle(socket));
            } catch (SocketException e) {
                if (running.get()) close();
            } catch (IOException e) {
                if (running.get()) close();
            }
        }
    }

    private void handle(Socket socket) {
        try (socket;
             DataInputStream input = new DataInputStream(socket.getInputStream());
             DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
            RpcMessage request = RpcCodec.read(input);
            RpcMessage response = Objects.requireNonNull(handler.handle(request), "RPC handler response");
            if (!request.getRequestId().equals(response.getRequestId()))
                throw new IOException("RPC response request ID does not match");
            RpcCodec.write(output, response);
        } catch (Exception ignored) {
            // A malformed or failed request is isolated to this connection.
        }
    }

    @Override
    public synchronized void close() {
        if (!running.getAndSet(false)) return;
        try { if (serverSocket != null) serverSocket.close(); }
        catch (IOException ignored) { }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(2, TimeUnit.SECONDS)) workers.shutdownNow();
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
