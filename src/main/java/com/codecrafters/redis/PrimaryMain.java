package com.codecrafters.redis;

import java.io.IOException;
import java.nio.file.Path;

public final class PrimaryMain {

    private PrimaryMain() {
    }

    public static void main(String[] args) throws IOException {
        Server server = new Server(
                6379,
                Path.of("data", "primary"),
                "127.0.0.1",
                6380
        );

        server.start();
    }
}