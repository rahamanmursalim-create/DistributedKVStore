
package com.codecrafters.redis.command;

import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.storage.RedisStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Executes Redis-compatible commands for DistributedKVStore.
 */
public class CommandHandler {

    private final RedisStore store;

    public CommandHandler(RedisStore store) {
        this.store = store;
    }

    public RespValue execute(Command command) {
        if (command == null || command.getName().isEmpty()) {
            return RespValue.error("ERR unknown command ''");
        }

        String name = command.getName().toUpperCase(Locale.ROOT);

        switch (name) {
            case "PING":
                return handlePing(command);

            case "ECHO":
                return handleEcho(command);

            case "SET":
                return handleSet(command);

            case "PUT":
                return handlePut(command);

            case "GET":
                return handleGet(command);

            case "DEL":
            case "DELETE":
                return handleDel(command);

            case "EXISTS":
                return handleExists(command);

            case "KEYS":
                return handleKeys(command);

            case "COMMAND":
                return handleCommand();

            case "INFO":
                return handleInfo();

            case "CONFIG":
                return handleConfig(command);

            default:
                return RespValue.error(
                    "ERR unknown command '" + name.toLowerCase(Locale.ROOT) + "'"
                );
        }
    }

    private RespValue handlePing(Command command) {
        if (command.getArgCount() == 0) {
            return RespValue.simpleString("PONG");
        }

        if (command.getArgCount() == 1) {
            return RespValue.bulkString(command.getArg(0));
        }

        return RespValue.error(
            "ERR wrong number of arguments for 'ping' command"
        );
    }

    private RespValue handleEcho(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'echo' command"
            );
        }

        return RespValue.bulkString(command.getArg(0));
    }

    /**
     * PUT key value
     *
     * Uses the same WAL-backed store as SET.
     */
    private RespValue handlePut(Command command) {
        if (command.getArgCount() != 2) {
            return RespValue.error(
                "ERR wrong number of arguments for 'put' command"
            );
        }

        String key = command.getArg(0);
        String value = command.getArg(1);

        store.set(key, value);

        return RespValue.simpleString("OK");
    }

    /**
     * SET key value [PX milliseconds | EX seconds]
     */
    private RespValue handleSet(Command command) {
        if (command.getArgCount() < 2) {
            return RespValue.error(
                "ERR wrong number of arguments for 'set' command"
            );
        }

        String key = command.getArg(0);
        String value = command.getArg(1);
        Long expiryMillis = null;

        for (int i = 2; i < command.getArgCount(); i++) {
            String option = command.getArg(i).toUpperCase(Locale.ROOT);

            if ("PX".equals(option)) {
                if (expiryMillis != null || i + 1 >= command.getArgCount()) {
                    return RespValue.error("ERR syntax error");
                }

                try {
                    long px = Long.parseLong(command.getArg(++i));

                    if (px <= 0) {
                        return RespValue.error(
                            "ERR invalid expire time in 'set' command"
                        );
                    }

                    expiryMillis = px;
                } catch (NumberFormatException e) {
                    return RespValue.error(
                        "ERR value is not an integer or out of range"
                    );
                }

            } else if ("EX".equals(option)) {
                if (expiryMillis != null || i + 1 >= command.getArgCount()) {
                    return RespValue.error("ERR syntax error");
                }

                try {
                    long seconds = Long.parseLong(command.getArg(++i));

                    if (seconds <= 0 || seconds > Long.MAX_VALUE / 1000L) {
                        return RespValue.error(
                            "ERR invalid expire time in 'set' command"
                        );
                    }

                    expiryMillis = seconds * 1000L;
                } catch (NumberFormatException e) {
                    return RespValue.error(
                        "ERR value is not an integer or out of range"
                    );
                }

            } else {
                return RespValue.error("ERR syntax error");
            }
        }

        if (expiryMillis == null) {
            store.set(key, value);
        } else {
            store.set(key, value, expiryMillis);
        }

        return RespValue.simpleString("OK");
    }

    private RespValue handleGet(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'get' command"
            );
        }

        String value = store.get(command.getArg(0));

        if (value == null) {
            return RespValue.nullBulkString();
        }

        return RespValue.bulkString(value);
    }

    /**
     * Supports DEL key [key ...] and DELETE key [key ...].
     */
    private RespValue handleDel(Command command) {
        if (command.getArgCount() < 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'del' command"
            );
        }

        String[] keys = command.getArgs().toArray(new String[0]);

        return RespValue.integer(store.del(keys));
    }

    private RespValue handleExists(Command command) {
        if (command.getArgCount() != 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'exists' command"
            );
        }

        return RespValue.integer(
            store.exists(command.getArg(0)) ? 1 : 0
        );
    }

    private RespValue handleKeys(Command command) {
        if (command.getArgCount() > 1) {
            return RespValue.error(
                "ERR wrong number of arguments for 'keys' command"
            );
        }

        String pattern = command.getArgCount() == 0
            ? "*"
            : command.getArg(0);

        List<RespValue> elements = new ArrayList<>();

        for (String key : store.keys(pattern)) {
            elements.add(RespValue.bulkString(key));
        }

        return RespValue.array(elements);
    }

    private RespValue handleCommand() {
        return RespValue.array(new ArrayList<>());
    }

    private RespValue handleInfo() {
        String info =
            "# Server\r\n" +
            "redis_version:7.0.0\r\n" +
            "role:master\r\n";

        return RespValue.bulkString(info);
    }

    private RespValue handleConfig(Command command) {
        if (command.getArgCount() >= 2
                && "GET".equalsIgnoreCase(command.getArg(0))) {

            String parameter = command.getArg(1);
            List<RespValue> result = new ArrayList<>();

            result.add(RespValue.bulkString(parameter));
            result.add(RespValue.bulkString(""));

            return RespValue.array(result);
        }

        return RespValue.array(new ArrayList<>());
    }
}
