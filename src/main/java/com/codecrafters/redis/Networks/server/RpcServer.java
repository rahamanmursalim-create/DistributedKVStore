package com.distributedkv.network.server;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.network.handler.ServerHandler;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * RpcServer provides a high-level RPC interface on top of the NettyServer.
 * It handles request routing, connection tracking, and metrics collection.
 */
public class RpcServer {
    private static final Logger LOGGER = Logger.getLogger(RpcServer.class.getName());

    // Server configuration
    private final String host;
    private final int port;
    private final int handlerThreads;
    private final int handlerQueueSize;

    // The underlying Netty server
    private NettyServer nettyServer;

    // Server handler
    private ServerHandler serverHandler;

    // Thread pool for request processing
    private ThreadPoolExecutor executor;

    // Connected clients
    private final ChannelGroup clients;

    // Request handlers
    private final Map<Request.Type, Function<Request, Response>> requestHandlers;

    // Metrics
    private final MetricsRegistry metricsRegistry;
    private final AtomicLong totalRequests;
    private final AtomicLong failedRequests;
    private final Map<Request.Type, AtomicLong> requestCountByType;

    // Server state
    private final AtomicBoolean isRunning;

    /**
     * Creates a new RpcServer with default parameters.
     *
     * @param host The server host
     * @param port The server port
     */
    public RpcServer(String host, int port) {
        this(host, port, Runtime.getRuntime().availableProcessors() * 2, 1000);
    }

    /**
     * Creates a new RpcServer with custom parameters.
     *
     * @param host The server host
     * @param port The server port
     * @param handlerThreads The number of handler threads
     * @param handlerQueueSize The handler queue size
     */
    public RpcServer(String host, int port, int handlerThreads, int handlerQueueSize) {
        this.host = host;
        this.port = port;
        this.handlerThreads = handlerThreads;
        this.handlerQueueSize = handlerQueueSize;

        this.clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.requestHandlers = new ConcurrentHashMap<>();
        this.metricsRegistry = new MetricsRegistry();
        this.totalRequests = new AtomicLong(0);
        this.failedRequests = new AtomicLong(0);
        this.requestCountByType = new ConcurrentHashMap<>();
        this.isRunning = new AtomicBoolean(false);
    }

    /**
     * Starts the RPC server.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (isRunning.getAndSet(true)) {
            return Result.success(null); // Already started
        }

        LOGGER.info("Starting RPC server on " + host + ":" + port);

        try {
            // Create the thread pool executor
            executor = new ThreadPoolExecutor(
                handlerThreads,
                handlerThreads,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(handlerQueueSize),
                r -> {
                    Thread thread = new Thread(r, "rpc-handler");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
            );

            // Create the server handler
            serverHandler = new ServerHandler(this, executor);

            // Register default handlers
            registerDefaultHandlers();

            // Create and start the Netty server
            nettyServer = new NettyServer(host, port, serverHandler);
            Result<Void> result = nettyServer.start();

            if (!result.isSuccess()) {
                isRunning.set(false);
                LOGGER.severe("Failed to start Netty server: " + result.getStatus() + " - " + result.getMessage());
                return result;
            }

            // Start metrics collection
            startMetricsCollection();

            LOGGER.info("RPC server started successfully");
            return Result.success(null);
        } catch (Exception e) {
            isRunning.set(false);
            LOGGER.severe("Failed to start RPC server: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to start RPC server: " + e.getMessage());
        }
    }

    /**
     * Stops the RPC server.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (!isRunning.getAndSet(false)) {
            return Result.success(null); // Already stopped
        }

        LOGGER.info("Stopping RPC server");

        try {
            // Close all client connections
            clients.close();

            // Stop the Netty server
            if (nettyServer != null) {
                Result<Void> result = nettyServer.stop();
                nettyServer = null;

                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to stop Netty server cleanly: " + result.getStatus() + " - " + result.getMessage());
                }
            }

            // Shutdown the executor
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                executor = null;
            }

            LOGGER.info("RPC server stopped successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to stop RPC server cleanly: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to stop RPC server: " + e.getMessage());
        }
    }

    /**
     * Registers a handler for a specific request type.
     *
     * @param type The request type
     * @param handler The handler function that processes requests of this type
     */
    public void registerHandler(Request.Type type, Function<Request, Response> handler) {
        requestHandlers.put(type, handler);
        serverHandler.registerHandler(type, handler);
        LOGGER.info("Registered handler for request type: " + type);
    }

    /**
     * Unregisters a handler for a specific request type.
     *
     * @param type The request type
     */
    public void unregisterHandler(Request.Type type) {
        requestHandlers.remove(type);
        serverHandler.unregisterHandler(type);
        LOGGER.info("Unregistered handler for request type: " + type);
    }

    /**
     * Broadcasts a message to all connected clients.
     *
     * @param message The message to broadcast
     */
    public void broadcast(Message message) {
        LOGGER.fine("Broadcasting message to " + clients.size() + " clients");

        for (Channel channel : clients) {
            channel.writeAndFlush(message);
        }
    }

    /**
     * Adds a client to the connected clients group.
     *
     * @param channel The client channel
     */
    public void addClient(Channel channel) {
        clients.add(channel);
        LOGGER.fine("Client connected: " + channel.remoteAddress() + " (total: " + clients.size() + ")");
    }

    /**
     * Removes a client from the connected clients group.
     *
     * @param channel The client channel
     */
    public void removeClient(Channel channel) {
        clients.remove(channel);
        LOGGER.fine("Client disconnected: " + channel.remoteAddress() + " (total: " + clients.size() + ")");
    }

    /**
     * Gets the number of connected clients.
     *
     * @return The number of connected clients
     */
    public int getClientCount() {
        return clients.size();
    }

    /**
     * Gets the metrics for this server.
     *
     * @return The metrics for this server
     */
    public Metrics getMetrics() {
        return metricsRegistry.getMetrics();
    }

    /**
     * Registers default handlers for common request types.
     */
    private void registerDefaultHandlers() {
        // Register a handler for PING requests
        registerHandler(Request.Type.PING, request -> {
            Response response = new Response();
            response.setRequestId(request.getRequestId());
            response.setStatus(Status.OK);
            return response;
        });

        // Register a handler for STATUS requests
        registerHandler(Request.Type.STATUS, request -> {
            Response response = new Response();
            response.setRequestId(request.getRequestId());
            response.setStatus(Status.OK);

            Metrics metrics = getMetrics();
            response.setMessage(metrics.toString());

            return response;
        });
    }

    /**
     * Starts collecting metrics for the server.
     */
    private void startMetricsCollection() {
        // Initialize metrics for all request types
        for (Request.Type type : Request.Type.values()) {
            requestCountByType.put(type, new AtomicLong(0));
        }

        // Schedule metrics collection
        java.util.Timer timer = new java.util.Timer(true);
        timer.scheduleAtFixedRate(new java.util.TimerTask() {
            @Override
            public void run() {
                collectMetrics();
            }
        }, 0, 5000);
    }

    /**
     * Collects metrics for the server.
     */
    private void collectMetrics() {
        Metrics metrics = new Metrics();

        // Server metrics
        metrics.put("server.host", host);
        metrics.put("server.port", port);
        metrics.put("server.running", isRunning.get());
        metrics.put("server.client_count", clients.size());

        // Thread pool metrics
        if (executor != null) {
            metrics.put("executor.active_count", executor.getActiveCount());
            metrics.put("executor.pool_size", executor.getPoolSize());
            metrics.put("executor.core_pool_size", executor.getCorePoolSize());
            metrics.put("executor.maximum_pool_size", executor.getMaximumPoolSize());
            metrics.put("executor.queue_size", executor.getQueue().size());
            metrics.put("executor.completed_task_count", executor.getCompletedTaskCount());
            metrics.put("executor.task_count", executor.getTaskCount());
        }

        // Request metrics
        metrics.put("requests.total", totalRequests.get());
        metrics.put("requests.failed", failedRequests.get());

        // Request count by type
        for (Map.Entry<Request.Type, AtomicLong> entry : requestCountByType.entrySet()) {
            metrics.put("requests.count." + entry.getKey().name().toLowerCase(), entry.getValue().get());
        }

        metricsRegistry.registerMetrics("server", metrics);
    }

    /**
     * Updates request metrics.
     *
     * @param request The request
     * @param response The response
     */
    public void updateRequestMetrics(Request request, Response response) {
        totalRequests.incrementAndGet();

        // Update request count by type
        AtomicLong count = requestCountByType.get(request.getType());
        if (count != null) {
            count.incrementAndGet();
        }

        // Update failed request count
        if (response.getStatus() != Status.OK) {
            failedRequests.incrementAndGet();
        }
    }

    /**
     * Gets the host that this server is bound to.
     *
     * @return The host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port that this server is bound to.
     *
     * @return The port
     */
    public int getPort() {
        return port;
    }

    /**
     * Checks if the server is running.
     *
     * @return true if the server is running, false otherwise
     */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Gets the server handler.
     *
     * @return The server handler
     */
    public ServerHandler getServerHandler() {
        return serverHandler;
    }

    /**
     * Gets the thread pool executor.
     *
     * @return The thread pool executor
     */
    public ThreadPoolExecutor getExecutor() {
        return executor;
    }
}