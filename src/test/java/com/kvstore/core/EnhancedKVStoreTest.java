package com.kvstore.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class EnhancedKVStoreTest {
    
    @TempDir
    Path tempDir;
    
    private EnhancedKVStore kvStore;
    
    @BeforeEach
    void setUp() throws IOException {
        kvStore = new EnhancedKVStore(tempDir.toString());
    }
    
    @AfterEach
    void tearDown() {
        if (kvStore != null) {
            kvStore.close();
        }
    }
    
    @Test
    void testPutAndGet() {
        // Test basic put and get operations
        assertTrue(kvStore.put("key1", "value1"));
        Optional<String> result = kvStore.read("key1");
        assertTrue(result.isPresent());
        assertEquals("value1", result.get());
    }
    
    @Test
    void testPutNullValues() {
        // Test that null keys and values are rejected
        assertFalse(kvStore.put(null, "value"));
        assertFalse(kvStore.put("key", null));
        assertFalse(kvStore.put(null, null));
    }
    
    @Test
    void testGetNonExistentKey() {
        // Test getting a key that doesn't exist
        Optional<String> result = kvStore.read("nonexistent");
        assertFalse(result.isPresent());
    }
    
    @Test
    void testDelete() {
        // Test delete operation
        kvStore.put("key1", "value1");
        assertTrue(kvStore.delete("key1"));
        
        // Verify key is deleted
        Optional<String> result = kvStore.read("key1");
        assertFalse(result.isPresent());
    }
    
    @Test
    void testDeleteNonExistentKey() {
        // Test deleting a key that doesn't exist
        assertTrue(kvStore.delete("nonexistent"));
    }
    
    @Test
    void testBatchPut() {
        // Test batch put operation
        List<String> keys = List.of("key1", "key2", "key3");
        List<String> values = List.of("value1", "value2", "value3");
        
        assertTrue(kvStore.batchPut(keys, values));
        
        // Verify all keys were stored
        assertEquals("value1", kvStore.read("key1").orElse(null));
        assertEquals("value2", kvStore.read("key2").orElse(null));
        assertEquals("value3", kvStore.read("key3").orElse(null));
    }
    
    @Test
    void testBatchPutWithMismatchedSizes() {
        // Test batch put with mismatched key and value lists
        List<String> keys = List.of("key1", "key2");
        List<String> values = List.of("value1");
        
        assertFalse(kvStore.batchPut(keys, values));
    }
    
    @Test
    void testReadKeyRange() {
        // Test reading a range of keys
        kvStore.put("a", "value1");
        kvStore.put("b", "value2");
        kvStore.put("c", "value3");
        kvStore.put("d", "value4");
        
        Map<String, String> range = kvStore.readKeyRange("b", "d");
        assertEquals(2, range.size());
        assertEquals("value2", range.get("b"));
        assertEquals("value3", range.get("c"));
        assertFalse(range.containsKey("a"));
        assertFalse(range.containsKey("d"));
    }
    
    @Test
    void testMultipleOperations() {
        // Test multiple operations in sequence
        assertTrue(kvStore.put("key1", "value1"));
        assertTrue(kvStore.put("key2", "value2"));
        assertTrue(kvStore.put("key3", "value3"));
        
        assertEquals("value1", kvStore.read("key1").orElse(null));
        assertEquals("value2", kvStore.read("key2").orElse(null));
        assertEquals("value3", kvStore.read("key3").orElse(null));
        
        assertTrue(kvStore.delete("key2"));
        assertFalse(kvStore.read("key2").isPresent());
        
        // Update existing key
        assertTrue(kvStore.put("key1", "newvalue1"));
        assertEquals("newvalue1", kvStore.read("key1").orElse(null));
    }
    
    @Test
    void testWALRecovery() {
        // Test that data persists through WAL recovery
        kvStore.put("persistent_key", "persistent_value");
        
        // Close and recreate store to trigger WAL recovery
        kvStore.close();
        
        try {
            EnhancedKVStore recoveredStore = new EnhancedKVStore(tempDir.toString());
            
            // Verify data was recovered
            Optional<String> result = recoveredStore.read("persistent_key");
            assertTrue(result.isPresent());
            assertEquals("persistent_value", result.get());
            
            recoveredStore.close();
        } catch (IOException e) {
            fail("Failed to recover from WAL: " + e.getMessage());
        }
    }
    
    @Test
    void testMemtableFlushing() {
        // Test that memtable gets flushed after threshold
        // Add many entries to trigger memtable flush
        for (int i = 0; i < 10001; i++) {
            kvStore.put("key" + i, "value" + i);
        }
        
        // Verify data is still accessible
        Optional<String> result = kvStore.read("key10000");
        assertTrue(result.isPresent());
        assertEquals("value10000", result.get());
    }
    
    @Test
    void testCompaction() {
        // Test forced compaction
        // Add some data first
        for (int i = 0; i < 100; i++) {
            kvStore.put("key" + i, "value" + i);
        }
        
        // Force compaction
        kvStore.compact();
        
        // Verify data is still accessible after compaction
        Optional<String> result = kvStore.read("key50");
        assertTrue(result.isPresent());
        assertEquals("value50", result.get());
    }
    
    @Test
    void testStoreStats() {
        // Test getting store statistics
        kvStore.put("key1", "value1");
        kvStore.put("key2", "value2");
        kvStore.delete("key1");
        
        EnhancedKVStore.StoreStats stats = kvStore.getStats();
        assertNotNull(stats);
        assertTrue(stats.getMemtableSize() >= 0);
        assertTrue(stats.getDeletedKeysCount() >= 0);
        assertTrue(stats.getSSTableCount() >= 0);
        assertTrue(stats.getTotalEntries() >= 0);
        assertTrue(stats.getTotalSize() >= 0);
        assertTrue(stats.getWALSize() >= 0);
    }
}
