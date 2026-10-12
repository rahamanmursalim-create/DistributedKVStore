package com.codecrafters.redis;

import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.storage.RedisStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multi-threaded TCP Redis Server.
 */
public class Server implements AutoCloseable {
    public static final int DEFAULT_PORT = 6379;
    private final int port;
    private final RedisStore store;
    private final CommandHandler commandHandler;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerSocket serverSocket;

    public Server() {
        this(DEFAULT_PORT);
    }

    public Server(int port) {
        this.port = port;
        this.store = new RedisStore();
        this.commandHandler = new CommandHandler(store);
        this.executor = createExecutorService();
    }

    private static ExecutorService createExecutorService() {
        try {
            // Check if Java 21+ Virtual Threads are available
            return (ExecutorService) Executors.class
                    .getMethod("newVirtualThreadPerTaskExecutor")
                    .invoke(null);
        } catch (Throwable ignored) {
            // Fallback to standard cached thread pool for older JDKs
            return Executors.newCachedThreadPool();
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        // Mandatory requirement: setReuseAddress(true) before binding
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        running.set(true);

        System.out.println("Redis server listening on port " + port + "...");

        try {
            while (running.get() && !serverSocket.isClosed()) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket, commandHandler));
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("Server accept error: " + e.getMessage());
            }
        } finally {
            close();
        }
    }

    public void stop() {
        running.set(false);
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
        close();
    }

    @Override
    public void close() {
        running.set(false);
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
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
        store.close();
    }

    public RedisStore getStore() {
        return store;
    }

    public int getPort() {
        return port;
    }
}
