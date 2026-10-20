package com.distributedkv.network.handler;

import com.distributedkv.common.Status;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.network.server.RpcServer;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;

import java.net.SocketException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ServerHandler processes incoming messages from clients, dispatches requests to the appropriate
 * handlers, and sends responses back to clients. It also handles network-related events like
 * connection establishment, disconnection, and errors.
 */
@Sharable
public class ServerHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger LOGGER = Logger.getLogger(ServerHandler.class.getName());

    // Reference to the RPC server
    private final RpcServer rpcServer;

    // Thread pool for processing requests
    private final ThreadPoolExecutor executor;

    // Request handlers for different request types
    private final java.util.Map<Request.Type, Function<Request, Response>> requestHandlers;

    /**
     * Creates a new ServerHandler with the given RPC server and executor.
     *
     * @param rpcServer The RPC server
     * @param executor The thread pool for processing requests
     */
    public ServerHandler(RpcServer rpcServer, ThreadPoolExecutor executor) {
        this.rpcServer = rpcServer;
        this.executor = executor;
        this.requestHandlers = new java.util.concurrent.ConcurrentHashMap<>();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.fine("Client connected: " + ctx.channel().remoteAddress());
        rpcServer.addClient(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.fine("Client disconnected: " + ctx.channel().remoteAddress());
        rpcServer.removeClient(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg instanceof Request) {
            final Request request = (Request) msg;
            LOGGER.fine("Received request: " + request);

            // Process the request in a separate thread to avoid blocking the I/O thread
            try {
                executor.execute(() -> processRequest(ctx, request));
            } catch (RejectedExecutionException e) {
                // Handle rejection (e.g., when the executor is overloaded)
                Response response = new Response();
                response.setRequestId(request.getRequestId());
                response.setStatus(Status.RESOURCE_EXHAUSTED);
                response.setMessage("Server is too busy to process the request");
                ctx.writeAndFlush(response);
                LOGGER.warning("Request rejected due to server overload: " + request);
            }
        } else {
            LOGGER.warning("Received unexpected message type: " + msg.getClass().getName());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ReadTimeoutException) {
            LOGGER.warning("Read timeout from client: " + ctx.channel().remoteAddress());
        } else if (cause instanceof WriteTimeoutException) {
            LOGGER.warning("Write timeout to client: " + ctx.channel().remoteAddress());
        } else if (cause instanceof SocketException) {
            LOGGER.warning("Socket error: " + cause.getMessage());
        } else {
            LOGGER.log(Level.SEVERE, "Unexpected error", cause);
        }

        // Close the connection on errors
        if (ctx.channel().isActive()) {
            ctx.close();
        }
    }

    /**
     * Processes a request and sends the response back to the client.
     *
     * @param ctx The channel handler context
     * @param request The request to process
     */
    private void processRequest(ChannelHandlerContext ctx, Request request) {
        Response response = null;

        try {
            // Get the handler for this request type
            Function<Request, Response> handler = requestHandlers.get(request.getType());

            if (handler != null) {
                // Process the request using the handler
                response = handler.apply(request);
            } else {
                // No handler found for this request type
                response = new Response();
                response.setRequestId(request.getRequestId());
                response.setStatus(Status.UNIMPLEMENTED);
                response.setMessage("No handler for request type: " + request.getType());
            }
        } catch (Exception e) {
            // Handle any exceptions during request processing
            LOGGER.log(Level.SEVERE, "Error processing request", e);

            response = new Response();
            response.setRequestId(request.getRequestId());
            response.setStatus(Status.INTERNAL_ERROR);
            response.setMessage("Internal server error: " + e.getMessage());
        }

        // Make sure the response has the same request ID
        if (response != null) {
            response.setRequestId(request.getRequestId());

            // Send the response back to the client
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(response);
                LOGGER.fine("Sent response: " + response);
            } else {
                LOGGER.warning("Cannot send response as the channel is no longer active");
            }
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
        LOGGER.info("Registered handler for request type: " + type);
    }

    /**
     * Unregisters a handler for a specific request type.
     *
     * @param type The request type
     */
    public void unregisterHandler(Request.Type type) {
        requestHandlers.remove(type);
        LOGGER.info("Unregistered handler for request type: " + type);
    }

    /**
     * Gets the executor used for processing requests.
     *
     * @return The executor
     */
    public ThreadPoolExecutor getExecutor() {
        return executor;
    }
}