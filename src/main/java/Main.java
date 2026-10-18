
import com.codecrafters.redis.Server;
import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.web.WebServer;

import java.io.IOException;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        int httpPort = readPort("PORT", 8080);
        int tcpPort = readPort("TCP_PORT", 6379);

        String dataDir = System.getenv("DATA_DIR");
        Path walPath = (dataDir != null && !dataDir.isBlank())
                ? Path.of(dataDir, "wal.log")
                : Path.of("data", "wal.log");

        RedisStore store = new RedisStore(walPath, true);
        Server tcpServer = new Server(tcpPort, store);
        WebServer webServer;

        try {
            webServer = new WebServer(store, httpPort);
        } catch (IOException e) {
            store.close();
            System.err.println("Could not create HTTP server: " + e.getMessage());
            System.exit(1);
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            webServer.close();
            tcpServer.close();
            store.close();
        }));

        Thread tcpThread = new Thread(() -> {
            try {
                tcpServer.start();
            } catch (IOException e) {
                System.err.println("TCP server error: " + e.getMessage());
            }
        }, "kv-tcp-server");

        tcpThread.setDaemon(true);
        tcpThread.start();

        webServer.start();
    }

    private static int readPort(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }

        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new NumberFormatException("Port out of range");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid port in environment variable " + name, e);
        }
    }
}
