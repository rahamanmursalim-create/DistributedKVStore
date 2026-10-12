package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class StorageEngine {

    private static final Pattern SSTABLE_NAME =
            Pattern.compile("sstable-(\\d+)\\.sst");

    private final MemTable memTable = new MemTable();
    private final Path directory;
    private final List<SSTable> ssTables = new ArrayList<>();

    private long nextTableId = 1;

    public StorageEngine(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();
        Files.createDirectories(this.directory);
        loadExistingSSTables();
    }

    private void loadExistingSSTables() throws IOException {
        List<Path> files;

        try (Stream<Path> paths = Files.list(directory)) {
            files = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> SSTABLE_NAME.matcher(
                            path.getFileName().toString()).matches())
                    .sorted(Comparator.comparingLong(
                            StorageEngine::extractTableId))
                    .toList();
        }

        for (Path file : files) {
            long id = extractTableId(file);

            // Validate each table before accepting it.
            SSTable table = SSTable.open(file);
            table.readAll();

            ssTables.add(table);
            nextTableId = Math.max(nextTableId, id + 1);
        }
    }

    private static long extractTableId(Path file) {
        Matcher matcher = SSTABLE_NAME.matcher(
                file.getFileName().toString());

        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid SSTable filename: " + file);
        }

        return Long.parseLong(matcher.group(1));
    }

    public synchronized void put(String key, String value) {
        memTable.put(key, value);
    }

    public synchronized String get(String key) throws IOException {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        for (int i = ssTables.size() - 1; i >= 0; i--) {
            String value = ssTables.get(i).get(key);

            if (value != null) {
                return value;
            }
        }

        return null;
    }

    public synchronized void flush() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        Path tablePath = directory.resolve(
                String.format("sstable-%020d.sst", nextTableId));

        Map<String, String> snapshot = memTable.snapshot();
        SSTable written = SSTable.write(tablePath, snapshot);

        ssTables.add(written);
        memTable.clear();
        nextTableId++;
    }

    public synchronized int memTableSize() {
        return memTable.size();
    }

    public synchronized int sstableCount() {
        return ssTables.size();
    }

    public synchronized NavigableMap<String, String> snapshot()
            throws IOException {

        NavigableMap<String, String> result = new TreeMap<>();

        for (SSTable table : ssTables) {
            result.putAll(table.readAll());
        }

        result.putAll(memTable.snapshot());
        return result;
    }
}