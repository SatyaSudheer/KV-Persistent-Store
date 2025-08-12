package com.kvstore.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface KVStore {
    boolean put(String key, String value);
    Optional<String> read(String key);
    Map<String, String> readKeyRange(String startKey, String endKey);
    boolean batchPut(List<String> keys, List<String> values);
    boolean delete(String key);
    void close();
}

