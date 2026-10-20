package com.codecrafters.redis.storage;

import java.util.List;
import java.util.Map;

/** Key-based storage operations used by the Redis and HTTP request paths. */
public interface KeyValueAccess {
    String get(String key);
    void set(String key, String value);
    default void setBatch(Map<String, String> values) {
        values.forEach(this::set);
    }
    void set(String key, String value, long expiryMillis);
    default int delete(String... keys) {
        if (this instanceof RedisStore redisStore) return redisStore.del(keys);
        throw new UnsupportedOperationException("delete is not implemented");
    }
    List<String> keys(String pattern);
    int size();
}
