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

    public ClientHandler(Socket clientSocket, CommandHandler commandHandler) {
        this.clientSocket = clientSocket;
        this.commandHandler = commandHandler;
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
                RespValue response = commandHandler.execute(command);

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
