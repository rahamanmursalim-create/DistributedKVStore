
package com.codecrafters.redis;

import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.storage.AsyncWriteBatcher;
import com.codecrafters.redis.raft.RaftGroup;
import com.codecrafters.redis.storage.KeyValueAccess;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.ssl.SSLContext;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server implements AutoCloseable {
    public static final int DEFAULT_PORT = 6379;

    private final int port;
    private final RedisStore store;
    private final boolean ownsStore;
    private final RaftGroup raftGroup;
    private final CommandHandler commandHandler;
    private final AsyncWriteBatcher writeBatcher;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerSocket serverSocket;

    public Server() {
        this(DEFAULT_PORT);
    }

    public Server(int port) {
        this(port, new RedisStore(), null, true, null, false);
    }

    public Server(int port, RedisStore store) {
        this(port, store, store, false, null, false);
    }

    /** Creates a server whose SET/PUT/GET/DEL operations are backed by one Raft group. */
    public Server(int port, RedisStore store, RaftGroup raftGroup, boolean followerReads) {
        this(port, store, store, false, raftGroup, followerReads);
    }

    /** Creates a server using an alternate key router such as {@code ShardedStore}. */
    public Server(int port, RedisStore store, KeyValueAccess access) {
        this(port, store, access, false, null, false);
    }

    private Server(int port, RedisStore store, KeyValueAccess access, boolean ownsStore,
                   RaftGroup raftGroup, boolean followerReads) {
        this.port = port;
        this.store = store;
        this.ownsStore = ownsStore;
        this.raftGroup = raftGroup;
        this.writeBatcher = new AsyncWriteBatcher(access,
                readIntSetting("redis.write.queue.capacity", 4096, 1),
                readIntSetting("redis.write.batch.maxSize", 128, 1),
                readLongSetting("redis.write.batch.maxDelayMillis", 2, 0));
        this.commandHandler = new CommandHandler(store, access, writeBatcher, raftGroup, followerReads);
        this.executor = createExecutorService();
    }

    private static ExecutorService createExecutorService() {
        try {
            return (ExecutorService) Executors.class
                    .getMethod("newVirtualThreadPerTaskExecutor")
                    .invoke(null);
        } catch (Throwable ignored) {
            return Executors.newCachedThreadPool();
        }
    }

    private static int readIntSetting(String name, int fallback, int minimum) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) return fallback;
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < minimum) throw new IllegalArgumentException(name + " must be >= " + minimum);
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }

    private static long readLongSetting(String name, long fallback, long minimum) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) return fallback;
        try {
            long parsed = Long.parseLong(value);
            if (parsed < minimum) throw new IllegalArgumentException(name + " must be >= " + minimum);
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }

    public void start() throws IOException {
        if (raftGroup != null) raftGroup.start();
        try {
            SSLContext tls = com.codecrafters.redis.rpc.TlsContext.fromEnvironment();
            serverSocket = tls == null ? new ServerSocket() : tls.getServerSocketFactory().createServerSocket();
            serverSocket.setReuseAddress(true);
            String bindHost = System.getenv().getOrDefault("KV_BIND_HOST", "127.0.0.1");
            serverSocket.bind(new InetSocketAddress(bindHost, port));
        } catch (IOException e) {
            close();
            throw e;
        }
        running.set(true);

        System.out.println("Redis server listening on port " + port + "...");

        try {
            while (running.get() && !serverSocket.isClosed()) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket, commandHandler, System.getenv("KV_CLIENT_TOKEN")));
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

        writeBatcher.close();

        if (raftGroup != null) {
            try { raftGroup.close(); }
            catch (IOException e) { System.err.println("Raft group shutdown error: " + e.getMessage()); }
        }

        if (ownsStore) {
            store.close();
        }
    }

    public RedisStore getStore() {
        return store;
    }

    public int getPort() {
        return port;
    }
}
