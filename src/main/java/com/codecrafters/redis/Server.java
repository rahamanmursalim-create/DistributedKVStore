package com.codecrafters.redis;

import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.distributed.ReplicationClient;
import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server implements AutoCloseable {

    public static final int DEFAULT_PORT = 6379;

    private final int port;
    private final StorageEngine store;
    private final CommandHandler commandHandler;
    private final ExecutorService executor;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile ServerSocket serverSocket;

    /**
     * Starts configuration for a standalone server on the default port.
     */
    public Server() {
        this(DEFAULT_PORT, Path.of("data", "storage-engine"));
    }

    /**
     * Configures a standalone server on the specified port.
     */
    public Server(int port) {
        this(port, Path.of("data", "storage-engine"));
    }

    /**
     * Configures a standalone server with a custom storage directory.
     */
    public Server(int port, Path storageDirectory) {
        this(port, storageDirectory, null);
    }

    /**
     * Configures a server with an optional replication client.
     *
     * If replicationClient is null, the server runs in standalone mode.
     */
    private Server(
            int port,
            Path storageDirectory,
            ReplicationClient replicationClient) {
        this(port, storageDirectory, replicationClient, ReplicationMode.SYNC);
    }

    private Server(
            int port,
            Path storageDirectory,
            ReplicationClient replicationClient,
            ReplicationMode replicationMode) {

        validatePort(port);

        Objects.requireNonNull(
                storageDirectory,
                "Storage directory cannot be null");

        this.port = port;

        try {
            this.store = new StorageEngine(storageDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Could not open the storage engine", e);
        }

        this.commandHandler = replicationClient == null
                ? new CommandHandler(store)
                : new CommandHandler(store, replicationClient, replicationMode);

        this.executor = createExecutorService();
    }

    /**
     * Configures a primary server that sends writes to a replication receiver.
     *
     * The receiver must already be running at replicaHost:replicaPort.
     */
    public Server(
            int port,
            Path storageDirectory,
            String replicaHost,
            int replicaPort) {

        this(
                port,
                storageDirectory,
                replicaHost,
                replicaPort,
                ReplicationMode.SYNC);
    }

    /**
     * Configures a primary server with explicit replication mode (SYNC or ASYNC).
     */
    public Server(
            int port,
            Path storageDirectory,
            String replicaHost,
            int replicaPort,
            ReplicationMode replicationMode) {

        this(
                port,
                storageDirectory,
                createReplicationClient(replicaHost, replicaPort),
                replicationMode);
    }

    private static ReplicationClient createReplicationClient(
            String replicaHost,
            int replicaPort) {

        Objects.requireNonNull(
                replicaHost,
                "Replica host cannot be null");

        if (replicaHost.isBlank()) {
            throw new IllegalArgumentException(
                    "Replica host cannot be blank");
        }

        validatePort(replicaPort);

        return new ReplicationClient(replicaHost, replicaPort);
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535");
        }
    }

    private static ExecutorService createExecutorService() {
        try {
            return (ExecutorService) Executors.class
                    .getMethod("newVirtualThreadPerTaskExecutor")
                    .invoke(null);
        } catch (ReflectiveOperationException
                 | ClassCastException ignored) {
            return Executors.newCachedThreadPool();
        }
    }

    /**
     * Starts accepting client connections.
     */
    public void start() throws IOException {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Server has already been closed");
        }

        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "Server is already running");
        }

        ServerSocket socket = new ServerSocket();

        try {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(port));
            serverSocket = socket;

            System.out.println(
                    "Redis server listening on port " + port + "...");

            while (running.get() && !socket.isClosed()) {
                Socket clientSocket = socket.accept();

                try {
                    executor.submit(
                            new ClientHandler(
                                    clientSocket,
                                    commandHandler));
                } catch (RuntimeException e) {
                    try {
                        clientSocket.close();
                    } catch (IOException ignored) {
                        // Ignore client cleanup failures.
                    }
                    throw e;
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                throw e;
            }
        } finally {
            close();
        }
    }

    /**
     * Stops the server and releases its resources.
     */
    public void stop() {
        close();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        running.set(false);

        ServerSocket socket = serverSocket;

        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The server is already shutting down.
            }
        }

        executor.shutdown();

        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        commandHandler.close();

        try {
            store.close();
        } catch (IOException e) {
            System.err.println(
                    "Could not close storage engine: "
                            + e.getMessage());
        }
    }

    public StorageEngine getStorageEngine() {
        return store;
    }

    public CommandHandler getCommandHandler() {
        return commandHandler;
    }

    public int getPort() {
        return port;
    }
}