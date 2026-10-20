package com.distributedkv.network.client;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * RpcClient provides a high-level RPC interface on top of the NettyClient.
 * It handles retries, timeouts, and connection management.
 */
public class RpcClient {
    private static final Logger LOGGER = Logger.getLogger(RpcClient.class.getName());

    // Connection details
    private final String host;
    private final int port;

    // Timeout values
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int writeTimeoutMs;
    private final int rpcTimeoutMs;

    // Retry configuration
    private final int maxRetries;
    private final int retryDelayMs;

    // The underlying Netty client
    private NettyClient nettyClient;

    // Client state
    private final AtomicBoolean isRunning;
    private long lastHealthCheckTimeMs;

    /**
     * Creates a new RpcClient with default parameters.
     *
     * @param host The server host
     * @param port The server port
     */
    public RpcClient(String host, int port) {
        this(host, port, 5000, 30000, 30000, 60000, 3, 1000);
    }

    /**
     * Creates a new RpcClient with custom parameters.
     *
     * @param host The server host
     * @param port The server port
     * @param connectTimeoutMs The connection timeout in milliseconds
     * @param readTimeoutMs The read timeout in milliseconds
     * @param writeTimeoutMs The write timeout in milliseconds
     * @param rpcTimeoutMs The RPC timeout in milliseconds
     * @param maxRetries The maximum number of retries
     * @param retryDelayMs The delay between retries in milliseconds
     */
    public RpcClient(String host, int port, int connectTimeoutMs, int readTimeoutMs,
                   int writeTimeoutMs, int rpcTimeoutMs, int maxRetries, int retryDelayMs) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.writeTimeoutMs = writeTimeoutMs;
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;

        this.isRunning = new AtomicBoolean(false);
        this.lastHealthCheckTimeMs = 0;
    }

    /**
     * Starts the RPC client.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (isRunning.getAndSet(true)) {
            return Result.success(null); // Already started
        }

        LOGGER.info("Starting RPC client to " + host + ":" + port);

        try {
            nettyClient = new NettyClient(host, port, connectTimeoutMs, readTimeoutMs, writeTimeoutMs);
            Result<Void> result = nettyClient.start();

            if (!result.isSuccess()) {
                isRunning.set(false);
                LOGGER.severe("Failed to start Netty client: " + result.getStatus() + " - " + result.getMessage());
                return result;
            }

            LOGGER.info("RPC client started successfully");
            return Result.success(null);
        } catch (Exception e) {
            isRunning.set(false);
            LOGGER.severe("Failed to start RPC client: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to start RPC client: " + e.getMessage());
        }
    }

    /**
     * Stops the RPC client.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (!isRunning.getAndSet(false)) {
            return Result.success(null); // Already stopped
        }

        LOGGER.info("Stopping RPC client");

        try {
            if (nettyClient != null) {
                Result<Void> result = nettyClient.stop();
                nettyClient = null;

                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to stop Netty client cleanly: " + result.getStatus() + " - " + result.getMessage());
                    return result;
                }
            }

            LOGGER.info("RPC client stopped successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to stop RPC client cleanly: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to stop RPC client: " + e.getMessage());
        }
    }

    /**
     * Sends a request to the server and waits for a response.
     *
     * @param request The request to send
     * @return A result containing the response or an error
     */
    public Result<Response> sendRequest(Request request) {
        if (!isRunning.get() || nettyClient == null) {
            return Result.failure(Status.UNAVAILABLE, "Client not running");
        }

        // Retry logic
        int retries = 0;
        Exception lastException = null;
        Status lastStatus = null;

        while (retries <= maxRetries) {
            try {
                if (retries > 0) {
                    // Wait before retrying
                    Thread.sleep(retryDelayMs);

                    // Check if still running
                    if (!isRunning.get() || nettyClient == null) {
                        return Result.failure(Status.UNAVAILABLE, "Client stopped during retry");
                    }

                    // Check if connection is still alive
                    if (!nettyClient.isConnected()) {
                        reconnect();
                    }

                    LOGGER.fine("Retrying request (attempt " + (retries + 1) + "/" + (maxRetries + 1) + ")");
                }

                // Send the request
                CompletableFuture<Response> future = nettyClient.sendRequest(request);
                Response response = future.get(rpcTimeoutMs, TimeUnit.MILLISECONDS);

                // Check if we should retry based on the response status
                if (response.getStatus() == Status.UNAVAILABLE ||
                    response.getStatus() == Status.TIMEOUT) {
                    lastStatus = response.getStatus();
                    retries++;
                    continue;
                }

                return Result.success(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Result.failure(Status.CANCELLED, "Request interrupted");
            } catch (TimeoutException e) {
                lastException = e;
                lastStatus = Status.TIMEOUT;
                retries++;
            } catch (ExecutionException e) {
                lastException = e.getCause() != null ? (Exception) e.getCause() : e;
                lastStatus = Status.INTERNAL_ERROR;
                retries++;
            } catch (Exception e) {
                lastException = e;
                lastStatus = Status.INTERNAL_ERROR;
                retries++;
            }
        }

        // All retries failed
        String errorMessage = lastException != null ? lastException.getMessage() : "Unknown error";
        LOGGER.warning("Request failed after " + (maxRetries + 1) + " attempts: " + errorMessage);
        return Result.failure(lastStatus != null ? lastStatus : Status.UNKNOWN,
                            "Request failed after " + (maxRetries + 1) + " attempts: " + errorMessage);
    }

    /**
     * Sends a message to the server without expecting a response.
     *
     * @param message The message to send
     * @return A result indicating success or failure
     */
    public Result<Void> sendMessage(Message message) {
        if (!isRunning.get() || nettyClient == null) {
            return Result.failure(Status.UNAVAILABLE, "Client not running");
        }

        // Retry logic
        int retries = 0;
        Exception lastException = null;
        Status lastStatus = null;

        while (retries <= maxRetries) {
            try {
                if (retries > 0) {
                    // Wait before retrying
                    Thread.sleep(retryDelayMs);

                    // Check if still running
                    if (!isRunning.get() || nettyClient == null) {
                        return Result.failure(Status.UNAVAILABLE, "Client stopped during retry");
                    }

                    // Check if connection is still alive
                    if (!nettyClient.isConnected()) {
                        reconnect();
                    }

                    LOGGER.fine("Retrying message send (attempt " + (retries + 1) + "/" + (maxRetries + 1) + ")");
                }

                // Send the message
                CompletableFuture<Void> future = nettyClient.sendMessage(message);
                future.get(rpcTimeoutMs, TimeUnit.MILLISECONDS);

                return Result.success(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Result.failure(Status.CANCELLED, "Send interrupted");
            } catch (TimeoutException e) {
                lastException = e;
                lastStatus = Status.TIMEOUT;
                retries++;
            } catch (ExecutionException e) {
                lastException = e.getCause() != null ? (Exception) e.getCause() : e;
                lastStatus = Status.INTERNAL_ERROR;
                retries++;
            } catch (Exception e) {
                lastException = e;
                lastStatus = Status.INTERNAL_ERROR;
                retries++;
            }
        }

        // All retries failed
        String errorMessage = lastException != null ? lastException.getMessage() : "Unknown error";
        LOGGER.warning("Message send failed after " + (maxRetries + 1) + " attempts: " + errorMessage);
        return Result.failure(lastStatus != null ? lastStatus : Status.UNKNOWN,
                            "Message send failed after " + (maxRetries + 1) + " attempts: " + errorMessage);
    }

    /**
     * Checks the health of the connection.
     *
     * @return true if the connection is healthy, false otherwise
     */
    public boolean isHealthy() {
        if (!isRunning.get() || nettyClient == null) {
            return false;
        }

        // Rate limit health checks
        long now = System.currentTimeMillis();
        if (now - lastHealthCheckTimeMs < 1000) {
            return nettyClient.isConnected();
        }

        lastHealthCheckTimeMs = now;

        // Send a ping request
        Request pingRequest = new Request();
        pingRequest.setType(Request.Type.PING);

        Result<Response> result = sendRequest(pingRequest);

        return result.isSuccess() && result.getValue().getStatus() == Status.OK;
    }

    /**
     * Gets the number of pending requests.
     *
     * @return The number of pending requests, or -1 if the client is not running
     */
    public int getPendingRequestCount() {
        if (!isRunning.get() || nettyClient == null) {
            return -1;
        }

        return nettyClient.getPendingRequestCount();
    }

    /**
     * Reconnects to the server if the connection is lost.
     *
     * @return A result indicating success or failure
     */
    private Result<Void> reconnect() {
        LOGGER.info("Reconnecting to server " + host + ":" + port);

        try {
            // Stop the current Netty client
            if (nettyClient != null) {
                nettyClient.stop();
            }

            // Create and start a new Netty client
            nettyClient = new NettyClient(host, port, connectTimeoutMs, readTimeoutMs, writeTimeoutMs);
            Result<Void> result = nettyClient.start();

            if (!result.isSuccess()) {
                LOGGER.severe("Failed to reconnect to server: " + result.getStatus() + " - " + result.getMessage());
                return result;
            }

            LOGGER.info("Successfully reconnected to server");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to reconnect to server: " + e.getMessage());
            return Result.failure(Status.UNAVAILABLE, "Failed to reconnect to server: " + e.getMessage());
        }
    }

    /**
     * Gets the host that this client is connected to.
     *
     * @return The host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port that this client is connected to.
     *
     * @return The port
     */
    public int getPort() {
        return port;
    }

    /**
     * Checks if the client is running.
     *
     * @return true if the client is running, false otherwise
     */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Checks if the client is connected to the server.
     *
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return isRunning.get() && nettyClient != null && nettyClient.isConnected();
    }

    @Override
    public String toString() {
        return "RpcClient{" +
               "host='" + host + '\'' +
               ", port=" + port +
               ", running=" + isRunning.get() +
               ", connected=" + (nettyClient != null && nettyClient.isConnected()) +
               '}';
    }
}