package com.codecrafters.redis.command;

import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Represents a parsed Redis command with its arguments.
 */
public final class Command {
    private final String name;
    private final List<RespValue> args;

    public Command(String name, List<String> args) {
        this.name = name != null ? name.toUpperCase(Locale.ROOT) : "";
        if (args == null) this.args = Collections.emptyList();
        else {
            List<RespValue> values = new ArrayList<>(args.size());
            for (String arg : args) values.add(RespValue.bulkString(arg));
            this.args = Collections.unmodifiableList(values);
        }
    }

    private Command(String name, List<RespValue> args, boolean preserveBytes) {
        this.name = name != null ? name.toUpperCase(Locale.ROOT) : "";
        this.args = args == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(args));
    }

    public static Command fromRespValue(RespValue value) {
        if (value == null) {
            return null;
        }

        if (value.getType() == RespType.ARRAY) {
            List<RespValue> elements = value.getArrayValue();
            if (elements == null || elements.isEmpty()) {
                return null;
            }
            String cmdName = elements.get(0).getStringValue();
            List<RespValue> cmdArgs = new ArrayList<>();
            for (int i = 1; i < elements.size(); i++) cmdArgs.add(elements.get(i));
            return new Command(cmdName, cmdArgs, true);
        }

        if (value.getType() == RespType.BULK_STRING || value.getType() == RespType.SIMPLE_STRING) {
            String raw = value.getStringValue();
            if (raw == null || raw.trim().isEmpty()) {
                return null;
            }
            String[] parts = raw.trim().split("\\s+");
            List<String> cmdArgs = new ArrayList<>();
            for (int i = 1; i < parts.length; i++) {
                cmdArgs.add(parts[i]);
            }
            return new Command(parts[0], cmdArgs);
        }

        return null;
    }

    public String getName() {
        return name;
    }

    public List<String> getArgs() {
        return args.stream().map(Command::stringValue).toList();
    }

    public int getArgCount() {
        return args.size();
    }

    public String getArg(int index) {
        if (index >= 0 && index < args.size()) {
            return stringValue(args.get(index));
        }
        return null;
    }

    public byte[] getArgBytes(int index) {
        if (index < 0 || index >= args.size()) return null;
        RespValue value = args.get(index);
        if (value.getBytesValue() != null) return value.getBytesValue();
        if (value.getIntegerValue() != null)
            return value.getIntegerValue().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new byte[0];
    }

    private static String stringValue(RespValue value) {
        if (value.getStringValue() != null) return value.getStringValue();
        return value.getIntegerValue() == null ? "" : value.getIntegerValue().toString();
    }

    @Override
    public String toString() {
        return "Command{" +
                "name='" + name + '\'' +
                ", args=" + args +
                '}';
    }
}
