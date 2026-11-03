package com.codecrafters.redis;

import com.codecrafters.redis.command.Command;
import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.protocol.RespReader;
import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.protocol.RespWriter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * Handles communication with an individual connected Redis client.
 */
public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final CommandHandler commandHandler;
    private final String clientToken;

    public ClientHandler(Socket clientSocket, CommandHandler commandHandler) {
        this(clientSocket, commandHandler, null);
    }

    public ClientHandler(Socket clientSocket, CommandHandler commandHandler, String clientToken) {
        this.clientSocket = clientSocket;
        this.commandHandler = commandHandler;
        this.clientToken = clientToken == null || clientToken.isBlank() ? null : clientToken;
    }

    @Override
    public void run() {
        try (
            InputStream rawIn = clientSocket.getInputStream();
            BufferedInputStream in = new BufferedInputStream(rawIn);
            OutputStream rawOut = clientSocket.getOutputStream();
            BufferedOutputStream out = new BufferedOutputStream(rawOut)
        ) {
            RespReader reader = new RespReader(in);

            boolean authenticated = clientToken == null;
            while (!clientSocket.isClosed()) {
                RespValue value;
                try {
                    value = reader.read();
                } catch (EOFException | SocketException e) {
                    // Client disconnected
                    break;
                } catch (IOException e) {
                    if (clientSocket.isClosed()) {
                        break;
                    }
                    System.err.println("I/O error reading from client: " + e.getMessage());
                    break;
                }

                if (value == null) {
                    // Stream closed
                    break;
                }

                Command command = Command.fromRespValue(value);
                RespValue response;
                if ("AUTH".equalsIgnoreCase(command.getName())) {
                    boolean argumentShape = command.getArgCount() == 1 || command.getArgCount() == 2;
                    boolean validUser = command.getArgCount() == 1 ||
                            (command.getArgCount() == 2 && "default".equals(command.getArg(0)));
                    int passwordIndex = command.getArgCount() == 1 ? 0 : 1;
                    boolean accepted = argumentShape && validUser && clientToken != null
                            && java.security.MessageDigest.isEqual(clientToken.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                            command.getArgBytes(passwordIndex));
                    authenticated = accepted;
                    response = accepted ? RespValue.simpleString("OK") : RespValue.error(
                            argumentShape ? "WRONGPASS invalid username-password pair" :
                                    "ERR wrong number of arguments for 'auth' command");
                } else if (!authenticated) response = RespValue.error("NOAUTH Authentication required");
                else response = commandHandler.execute(command);

                RespWriter.write(out, response);
                out.flush();
            }
        } catch (IOException e) {
            // Socket or connection error
            if (!clientSocket.isClosed()) {
                System.err.println("Client handler exception: " + e.getMessage());
            }
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException ignored) {
            }
        }
    }
}
