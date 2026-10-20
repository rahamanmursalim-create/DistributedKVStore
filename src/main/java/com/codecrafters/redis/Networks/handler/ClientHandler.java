package com.distributedkv.network.handler;

import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.Response;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.WriteTimeoutException;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ClientHandler processes incoming messages from the server and forwards them to the appropriate
 * callback handler. It also handles network-related events like connection establishment,
 * disconnection, and errors.
 */
public class ClientHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger LOGGER = Logger.getLogger(ClientHandler.class.getName());

    // Callback for handling responses
    private final Consumer<Response> responseHandler;

    // Channel context for this handler
    private ChannelHandlerContext ctx;

    /**
     * Creates a new ClientHandler with the given response handler.
     *
     * @param responseHandler The callback for handling responses
     */
    public ClientHandler(Consumer<Response> responseHandler) {
        this.responseHandler = responseHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        LOGGER.fine("Connected to server: " + ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.fine("Disconnected from server: " + ctx.channel().remoteAddress());
        this.ctx = null;
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg instanceof Response) {
            Response response = (Response) msg;
            LOGGER.fine("Received response: " + response);

            // Pass the response to the handler
            try {
                responseHandler.accept(response);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error in response handler", e);
            }
        } else {
            LOGGER.warning("Received unexpected message type: " + msg.getClass().getName());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ReadTimeoutException) {
            LOGGER.warning("Read timeout from server: " + ctx.channel().remoteAddress());
        } else if (cause instanceof WriteTimeoutException) {
            LOGGER.warning("Write timeout to server: " + ctx.channel().remoteAddress());
        } else if (cause instanceof ConnectException) {
            LOGGER.warning("Connection refused: " + cause.getMessage());
        } else if (cause instanceof SocketException) {
            LOGGER.warning("Socket error: " + cause.getMessage());
        } else if (cause instanceof TimeoutException) {
            LOGGER.warning("Operation timed out: " + cause.getMessage());
        } else {
            LOGGER.log(Level.SEVERE, "Unexpected error", cause);
        }

        // Close the connection on errors
        if (ctx.channel().isActive()) {
            ctx.close();
        }
    }

    /**
     * Checks if the handler is connected to a server.
     *
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return ctx != null && ctx.channel().isActive();
    }

    /**
     * Gets the channel context for this handler.
     *
     * @return The channel context, or null if not connected
     */
    public ChannelHandlerContext getContext() {
        return ctx;
    }
}