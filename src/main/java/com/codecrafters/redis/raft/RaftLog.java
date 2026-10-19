
package com.codecrafters.redis.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RaftLog {

    private final List<RaftLogEntry> entries = new ArrayList<>();

    public synchronized RaftLogEntry append(
            long term, byte[] command) {

        RaftLogEntry entry = new RaftLogEntry(
                entries.size() + 1L, term, command);

        entries.add(entry);
        return entry;
    }

    public synchronized Optional<RaftLogEntry> get(long index) {
        if (index <= 0 || index > entries.size()) {
            return Optional.empty();
        }

        return Optional.of(entries.get((int) index - 1));
    }

    public synchronized List<RaftLogEntry> entriesFrom(long index) {
        if (index < 1 || index > entries.size() + 1L) {
            throw new IllegalArgumentException("Invalid log index");
        }

        return List.copyOf(
                entries.subList((int) index - 1, entries.size()));
    }

    public synchronized void truncateFrom(long index) {
        if (index < 1 || index > entries.size() + 1L) {
            throw new IllegalArgumentException("Invalid log index");
        }

        entries.subList((int) index - 1, entries.size()).clear();
    }

    public synchronized long lastIndex() {
        return entries.size();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty()
                ? 0
                : entries.get(entries.size() - 1).getTerm();
    }

    public synchronized int size() {
        return entries.size();
    }
}
