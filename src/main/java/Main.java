import com.codecrafters.redis.Server;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        int port = Server.DEFAULT_PORT;

        // Parse optional arguments like --port <port>
        for (int i = 0; i < args.length; i++) {
            if ("--port".equalsIgnoreCase(args[i]) && i + 1 < args.length) {
                try {
                    port = Integer.parseInt(args[i + 1]);
                    i++;
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port number: " + args[i + 1]);
                }
            }
        }

        Server server = new Server(port);

        // Register shutdown hook for graceful exit
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));

        try {
            server.start();
        } catch (IOException e) {
            System.err.println("Failed to start Redis server on port " + port + ": " + e.getMessage());
            System.exit(1);
        }
    }
}
