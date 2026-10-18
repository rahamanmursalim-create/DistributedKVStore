import com.codecrafters.redis.Server;

import java.io.IOException;
import java.nio.file.Path;

public class Main {

    public static void main(String[] args) {
        int port = Server.DEFAULT_PORT;
        boolean replicationEnabled = false;
        String replicaHost = "127.0.0.1";
        int replicaPort = 6380;

        for (int i = 0; i < args.length; i++) {
            switch (args[i].toLowerCase()) {
                case "--port":
                    if (i + 1 < args.length) {
                        port = Integer.parseInt(args[++i]);
                    }
                    break;

                case "--replica-host":
                    if (i + 1 < args.length) {
                        replicaHost = args[++i];
                        replicationEnabled = true;
                    }
                    break;

                case "--replica-port":
                    if (i + 1 < args.length) {
                        replicaPort = Integer.parseInt(args[++i]);
                        replicationEnabled = true;
                    }
                    break;

                default:
                    System.err.println(
                            "Unknown argument: " + args[i]);
                    return;
            }
        }

        Server server;

        try {
            if (replicationEnabled) {
                server = new Server(
                        port,
                        Path.of("data", "primary"),
                        replicaHost,
                        replicaPort
                );
            } else {
                server = new Server(port);
            }
        } catch (RuntimeException e) {
            System.err.println(
                    "Failed to configure server: " + e.getMessage());
            return;
        }

        Runtime.getRuntime().addShutdownHook(
                new Thread(server::close));

        try {
            server.start();
        } catch (IOException e) {
            System.err.println(
                    "Failed to start Redis server on port "
                            + port + ": " + e.getMessage());
            System.exit(1);
        }
    }
}