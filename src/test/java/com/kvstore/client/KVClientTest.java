package com.kvstore.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

public class KVClientTest {
    
    private KVClient client;
    
    @BeforeEach
    void setUp() {
        client = new KVClient("localhost", 8080);
    }
    
    @Test
    void testClientConstruction() {
        // Test that client is constructed with correct host and port
        assertNotNull(client);
        // Note: We can't easily test the actual connection without a running server
        // This test just ensures the client object is created
    }
    
    @Test
    void testPingWithoutServer() {
        // Test ping when no server is running (should fail gracefully)
        boolean result = client.ping();
        // This will likely fail since no server is running, but shouldn't throw an exception
        assertFalse(result);
    }
    
    @Test
    void testPutWithoutServer() {
        // Test put when no server is running (should fail gracefully)
        boolean result = client.put("testkey", "testvalue");
        // This will likely fail since no server is running, but shouldn't throw an exception
        assertFalse(result);
    }
    
    @Test
    void testGetWithoutServer() {
        // Test get when no server is running (should fail gracefully)
        String result = client.get("testkey");
        // This will likely return null since no server is running, but shouldn't throw an exception
        assertNull(result);
    }
    
    @Test
    void testDeleteWithoutServer() {
        // Test delete when no server is running (should fail gracefully)
        boolean result = client.delete("testkey");
        // This will likely fail since no server is running, but shouldn't throw an exception
        assertFalse(result);
    }
}
