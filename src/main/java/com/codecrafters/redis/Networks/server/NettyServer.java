package com.distributedkv.network.server;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.codec.MessageCodec;
import com.distributedkv.network.protocol.codec.ProtobufCodec;
import com.distributedkv.network.handler.ServerHandler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * NettyServer implements the low-level network communication using Netty framework.
 * It handles connection management, message encoding/decoding, and request dispatching.
 */
public class NettyServer {
    private static final Logger LOGGER = Logger.getLogger(NettyServer.class.getName());

    // Server configuration
    private final String host;
    private final int port;
    private final int bossThreads;
    private final int workerThreads;
    private final int readTimeoutMs;
    private final int writeTimeoutMs;
    private final int connectionBacklog;

    // Netty components
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    // Message codec
    private final MessageCodec messageCodec;

    // Server handler
    private final ServerHandler serverHandler;

    // Server state
    private final AtomicBoolean isRunning;

    /**
     * Creates a new NettyServer with default parameters.
     *
     * @param host The server host
     * @param port The server port
     * @param serverHandler The server handler
     */
    public NettyServer(String host, int port, ServerHandler serverHandler) {
        this(host, port, 1, Runtime.getRuntime().availableProcessors() * 2,
             30000, 30000, 128, serverHandler);
    }

    /**
     * Creates a new NettyServer with custom parameters.
     *
     * @param host The server host
     * @param port The server port
     * @param bossThreads The number of boss threads
     * @param workerThreads The number of worker threads
     * @param readTimeoutMs The read timeout in milliseconds
     * @param writeTimeoutMs The write timeout in milliseconds
     * @param connectionBacklog The connection backlog
     * @param serverHandler The server handler
     */
    public NettyServer(String host, int port, int bossThreads, int workerThreads,
                      int readTimeoutMs, int writeTimeoutMs, int connectionBacklog,
                      ServerHandler serverHandler) {
        this.host = host;
        this.port = port;
        this.bossThreads = bossThreads;
        this.workerThreads = workerThreads;
        this.readTimeoutMs = readTimeoutMs;
        this.writeTimeoutMs = writeTimeoutMs;
        this.connectionBacklog = connectionBacklog;
        this.serverHandler = serverHandler;

        this.messageCodec = new ProtobufCodec();
        this.isRunning = new AtomicBoolean(false);
    }

    /**
     * Starts the Netty server and begins accepting connections.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (isRunning.getAndSet(true)) {
            return Result.success(null); // Already started
        }

        LOGGER.info("Starting Netty server on " + host + ":" + port);

        try {
            bossGroup = new NioEventLoopGroup(bossThreads);
            workerGroup = new NioEventLoopGroup(workerThreads);

            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(host, port))
                .option(ChannelOption.SO_BACKLOG, connectionBacklog)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
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
                        pipeline.addLast("serverHandler", serverHandler);
                    }
                });

            // Bind and start the server
            ChannelFuture future = bootstrap.bind().sync();
            serverChannel = future.channel();

            LOGGER.info("Netty server started successfully, listening on " + host + ":" + port);
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to start Netty server: " + e.getMessage());
            stop();
            return Result.failure(Status.INTERNAL_ERROR, "Failed to start server: " + e.getMessage());
        }
    }

    /**
     * Stops the Netty server and releases all resources.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (!isRunning.getAndSet(false)) {
            return Result.success(null); // Already stopped
        }

        LOGGER.info("Stopping Netty server");

        try {
            // Close the server channel
            if (serverChannel != null) {
                serverChannel.close().sync();
                serverChannel = null;
            }

            // Shutdown the event loop groups
            if (bossGroup != null) {
                bossGroup.shutdownGracefully().sync();
                bossGroup = null;
            }

            if (workerGroup != null) {
                workerGroup.shutdownGracefully().sync();
                workerGroup = null;
            }

            LOGGER.info("Netty server stopped successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to stop Netty server cleanly: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to stop server: " + e.getMessage());
        }
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
     * Checks if the server is running.
     *
     * @return true if the server is running, false otherwise
     */
    public boolean isRunning() {
        return isRunning.get();
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
     * Decoder for messages received from clients.
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
     * Encoder for messages sent to clients.
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