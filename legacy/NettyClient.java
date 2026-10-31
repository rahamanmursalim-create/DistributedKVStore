package com.distributedkv.network.client;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.network.protocol.codec.MessageCodec;
import com.distributedkv.network.protocol.codec.ProtobufCodec;
import com.distributedkv.network.handler.ClientHandler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * NettyClient implements the low-level network communication using Netty framework.
 * It handles connection management, message encoding/decoding, and request-response tracking.
 */
public class NettyClient {
    private static final Logger LOGGER = Logger.getLogger(NettyClient.class.getName());

    // Connection details
    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int writeTimeoutMs;

    // Netty components
    private EventLoopGroup eventLoopGroup;
    private Channel channel;
    private ClientHandler clientHandler;

    // Message codec
    private final MessageCodec messageCodec;

    // Request tracking
    private final AtomicLong requestIdGenerator;
    private final ConcurrentHashMap<Long, CompletableFuture<Response>> pendingRequests;

    // Client state
    private final AtomicBoolean isRunning;

    /**
     * Creates a new NettyClient.
     *
     * @param host The server host
     * @param port The server port
     * @param connectTimeoutMs The connection timeout in milliseconds
     * @param readTimeoutMs The read timeout in milliseconds
     * @param writeTimeoutMs The write timeout in milliseconds
     */
    public NettyClient(String host, int port, int connectTimeoutMs, int readTimeoutMs, int writeTimeoutMs) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.writeTimeoutMs = writeTimeoutMs;

        this.messageCodec = new ProtobufCodec();
        this.requestIdGenerator = new AtomicLong(0);
        this.pendingRequests = new ConcurrentHashMap<>();
        this.isRunning = new AtomicBoolean(false);
    }

    /**
     * Creates a new NettyClient with default timeout values.
     *
     * @param host The server host
     * @param port The server port
     */
    public NettyClient(String host, int port) {
        this(host, port, 5000, 30000, 30000);
    }

    /**
     * Starts the Netty client and establishes a connection to the server.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (isRunning.getAndSet(true)) {
            return Result.success(null); // Already started
        }

        LOGGER.info("Starting Netty client to " + host + ":" + port);

        try {
            eventLoopGroup = new NioEventLoopGroup();
            clientHandler = new ClientHandler(this::handleResponse);

            Bootstrap bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // Add timeouts
                        pipeline.addLast("readTimeoutHandler",
                                new ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS));
                        pipeline.addLast("writeTimeoutHandler",
                                new WriteTimeoutHandler(writeTimeoutMs, TimeUnit.MILLISECONDS));

                        // Add message framing
                        pipeline.addLast("frameDecoder",
                                new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                        pipeline.addLast("frameEncoder",
                                new LengthFieldPrepender(4));

                        // Add message codec
                        pipeline.addLast("messageDecoder",
                                new MessageDecoder(messageCodec));
                        pipeline.addLast("messageEncoder",
                                new MessageEncoder(messageCodec));

                        // Add message handler
                        pipeline.addLast("clientHandler", clientHandler);
                    }
                });

            // Connect to the server
            ChannelFuture future = bootstrap.connect(host, port).sync();
            channel = future.channel();

            LOGGER.info("Netty client started successfully, connected to " + host + ":" + port);
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to start Netty client: " + e.getMessage());
            stop();
            return Result.failure(Status.UNAVAILABLE, "Failed to connect to server: " + e.getMessage());
        }
    }

    /**
     * Stops the Netty client and releases all resources.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (!isRunning.getAndSet(false)) {
            return Result.success(null); // Already stopped
        }

        LOGGER.info("Stopping Netty client");

        try {
            // Close the channel
            if (channel != null) {
                channel.close().sync();
                channel = null;
            }

            // Shutdown the event loop group
            if (eventLoopGroup != null) {
                eventLoopGroup.shutdownGracefully().sync();
                eventLoopGroup = null;
            }

            // Complete any pending requests with an error
            for (CompletableFuture<Response> future : pendingRequests.values()) {
                Response errorResponse = new Response();
                errorResponse.setStatus(Status.UNAVAILABLE);
                errorResponse.setMessage("Client closed");
                future.complete(errorResponse);
            }
            pendingRequests.clear();

            LOGGER.info("Netty client stopped successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to stop Netty client cleanly: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to stop client: " + e.getMessage());
        }
    }

    /**
     * Sends a request message to the server.
     *
     * @param request The request message
     * @return A future that will be completed when the response is received
     */
    public CompletableFuture<Response> sendRequest(Request request) {
        if (!isRunning.get() || channel == null || !channel.isActive()) {
            CompletableFuture<Response> future = new CompletableFuture<>();
            Response errorResponse = new Response();
            errorResponse.setStatus(Status.UNAVAILABLE);
            errorResponse.setMessage("Client not connected");
            future.complete(errorResponse);
            return future;
        }

        // Set the request ID
        long requestId = requestIdGenerator.incrementAndGet();
        request.setRequestId(requestId);

        // Create the future for the response
        CompletableFuture<Response> responseFuture = new CompletableFuture<>();
        pendingRequests.put(requestId, responseFuture);

        // Set a timeout for the request
        scheduleRequestTimeout(requestId, readTimeoutMs);

        // Send the request
        channel.writeAndFlush(request).addListener(future -> {
            if (!future.isSuccess()) {
                CompletableFuture<Response> pendingFuture = pendingRequests.remove(requestId);
                if (pendingFuture != null) {
                    Response errorResponse = new Response();
                    errorResponse.setStatus(Status.INTERNAL_ERROR);
                    errorResponse.setMessage("Failed to send request: " + future.cause().getMessage());
                    pendingFuture.complete(errorResponse);
                }
            }
        });

        return responseFuture;
    }

    /**
     * Sends a message to the server without expecting a response.
     *
     * @param message The message to send
     * @return A future that will be completed when the message is sent
     */
    public CompletableFuture<Void> sendMessage(Message message) {
        if (!isRunning.get() || channel == null || !channel.isActive()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Client not connected"));
            return future;
        }

        CompletableFuture<Void> sendFuture = new CompletableFuture<>();

        // Send the message
        channel.writeAndFlush(message).addListener(future -> {
            if (future.isSuccess()) {
                sendFuture.complete(null);
            } else {
                sendFuture.completeExceptionally(future.cause());
            }
        });

        return sendFuture;
    }

    /**
     * Checks if the client is connected to the server.
     *
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return isRunning.get() && channel != null && channel.isActive();
    }

    /**
     * Gets the number of pending requests.
     *
     * @return The number of pending requests
     */
    public int getPendingRequestCount() {
        return pendingRequests.size();
    }

    /**
     * Handles a response message from the server.
     *
     * @param response The response message
     */
    private void handleResponse(Response response) {
        long requestId = response.getRequestId();
        CompletableFuture<Response> future = pendingRequests.remove(requestId);

        if (future != null) {
            future.complete(response);
        } else {
            LOGGER.warning("Received response for unknown request ID: " + requestId);
        }
    }

    /**
     * Schedules a timeout for a request.
     *
     * @param requestId The request ID
     * @param timeoutMs The timeout in milliseconds
     */
    private void scheduleRequestTimeout(long requestId, long timeoutMs) {
        if (eventLoopGroup != null) {
            eventLoopGroup.schedule(() -> {
                CompletableFuture<Response> future = pendingRequests.remove(requestId);
                if (future != null) {
                    Response timeoutResponse = new Response();
                    timeoutResponse.setRequestId(requestId);
                    timeoutResponse.setStatus(Status.TIMEOUT);
                    timeoutResponse.setMessage("Request timed out after " + timeoutMs + " ms");
                    future.complete(timeoutResponse);
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Decoder for messages received from the server.
     */
    private static class MessageDecoder extends io.netty.handler.codec.ByteToMessageDecoder {
        private final MessageCodec messageCodec;

        public MessageDecoder(MessageCodec messageCodec) {
            this.messageCodec = messageCodec;
        }

        @Override
        protected void decode(io.netty.channel.ChannelHandlerContext ctx,
                             io.netty.buffer.ByteBuf in,
                             java.util.List<Object> out) throws Exception {
            byte[] bytes = new byte[in.readableBytes()];
            in.readBytes(bytes);

            Message message = messageCodec.decode(bytes);
            out.add(message);
        }
    }

    /**
     * Encoder for messages sent to the server.
     */
    private static class MessageEncoder extends io.netty.handler.codec.MessageToByteEncoder<Message> {
        private final MessageCodec messageCodec;

        public MessageEncoder(MessageCodec messageCodec) {
            this.messageCodec = messageCodec;
        }

        @Override
        protected void encode(io.netty.channel.ChannelHandlerContext ctx,
                             Message message,
                             io.netty.buffer.ByteBuf out) throws Exception {
            byte[] bytes = messageCodec.encode(message);
            out.writeBytes(bytes);
        }
    }
}