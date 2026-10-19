
package com.codecrafters.redis.raft;

import java.util.Arrays;

public final class RaftLogEntry {

    private final long index;
    private final long term;
    private final byte[] command;

    public RaftLogEntry(long index, long term, byte[] command) {
        if (index <= 0) {
            throw new IllegalArgumentException("Index must be positive");
        }
        if (term < 0) {
            throw new IllegalArgumentException("Term cannot be negative");
        }

        this.index = index;
        this.term = term;
        this.command = command == null ? new byte[0] : command.clone();
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public byte[] getCommand() {
        return command.clone();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof RaftLogEntry other)) return false;

        return index == other.index
                && term == other.term
                && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + Long.hashCode(term);
        return 31 * result + Arrays.hashCode(command);
    }
}
