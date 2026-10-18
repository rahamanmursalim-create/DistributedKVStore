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
    private final List<String> args;

    public Command(String name, List<String> args) {
        this.name = name != null ? name.toUpperCase(Locale.ROOT) : "";
        this.args = args != null ? Collections.unmodifiableList(new ArrayList<>(args)) : Collections.emptyList();
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
            List<String> cmdArgs = new ArrayList<>();
            for (int i = 1; i < elements.size(); i++) {
                RespValue elem = elements.get(i);
                cmdArgs.add(elem.getStringValue() != null ? elem.getStringValue() : (elem.getIntegerValue() != null ? elem.getIntegerValue().toString() : ""));
            }
            return new Command(cmdName, cmdArgs);
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

    public static Command of(String name, String... args) {
        return new Command(name, List.of(args));
    }

    public String getName() {
        return name;
    }

    public List<String> getArgs() {
        return args;
    }

    public int getArgCount() {
        return args.size();
    }

    public String getArg(int index) {
        if (index >= 0 && index < args.size()) {
            return args.get(index);
        }
        return null;
    }

    @Override
    public String toString() {
        return "Command{" +
                "name='" + name + '\'' +
                ", args=" + args +
                '}';
    }
}
