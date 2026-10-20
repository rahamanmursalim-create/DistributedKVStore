package com.codecrafters.redis.sharding;

import com.codecrafters.redis.command.Command;
import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.storage.AsyncWriteBatcher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class ShardedCommandRoutingTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("sharded-command-test");
        RedisStore primary = new RedisStore(root.resolve("primary.wal"), false);
        ShardedStore router = new ShardedStore(32);
        AsyncWriteBatcher batcher = new AsyncWriteBatcher(router, 32, 8, 1);
        try {
            router.addNode("primary", primary);
            RedisStore secondary = new RedisStore(root.resolve("secondary.wal"), false);
            router.addNode("secondary", secondary);
            CommandHandler handler = new CommandHandler(primary, router, batcher, null, false);
            assert "OK".equals(handler.execute(new Command("SET", List.of("a", "one"))).getStringValue());
            assert "OK".equals(handler.execute(new Command("PUT", List.of("b", "two"))).getStringValue());
            assert "one".equals(handler.execute(new Command("GET", List.of("a"))).getStringValue());
            assert "two".equals(handler.execute(new Command("GET", List.of("b"))).getStringValue());
            RespValue keys = handler.execute(new Command("KEYS", List.of("*")));
            assert keys.getType() == RespType.ARRAY;
            assert keys.getArrayValue().size() == 2;
            assert handler.execute(new Command("DEL", List.of("a", "b"))).getIntegerValue() == 2;
            assert handler.execute(new Command("GET", List.of("a"))).getType() == RespType.NULL_BULK_STRING;
            System.out.println("ShardedCommandRoutingTest passed");
        } finally { batcher.close(); router.close(); }
    }
}
