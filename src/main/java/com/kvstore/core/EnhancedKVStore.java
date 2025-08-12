package com.kvstore.core;
import com.kvstore.api.KVStore;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class EnhancedKVStore implements KVStore {
    private static final Logger logger = Logger.getLogger(EnhancedKVStore.class.getName());
    private static final int MEMTABLE_SIZE_THRESHOLD = 10000; // Flush memtable after 10k entries
    private static final long CHECKPOINT_INTERVAL = 60000; // Checkpoint every 60 seconds
    
    private final String dataDirectory;
    private final ReentrantReadWriteLock lock;
    private final WAL wal;
    private final SSTableManager sstableManager;
    private final Map<String, String> memtable; // In-memory table for recent writes
    private final Set<String> deletedKeys; // Track deleted keys in memtable
    
    private long lastCheckpoint;
    private int writeCount;

    public EnhancedKVStore(String dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        this.lock = new ReentrantReadWriteLock();
        this.wal = new WAL(dataDirectory);
        this.sstableManager = new SSTableManager(dataDirectory);
        this.memtable = new ConcurrentHashMap<>();
        this.deletedKeys = ConcurrentHashMap.newKeySet();
        this.lastCheckpoint = System.currentTimeMillis();
        this.writeCount = 0;
        
        // Recover from WAL on startup
        recoverFromWAL();
        
        logger.info("Enhanced KV Store initialized with WAL and SSTable support");
    }

    private void recoverFromWAL() {
        try {
            wal.replay(new WAL.RecoveryHandler() {
                @Override
                public void handleOperation(String operation, String key, String value, long timestamp) {
                    if ("PUT".equals(operation)) {
                        memtable.put(key, value);
                        deletedKeys.remove(key);
                    } else if ("DELETE".equals(operation)) {
                        memtable.remove(key);
                        deletedKeys.add(key);
                    }
                }
            });
            
            logger.info("Recovered " + memtable.size() + " entries from WAL");
            
        } catch (IOException e) {
            logger.severe("Failed to recover from WAL: " + e.getMessage());
        }
    }
    @Override
    public boolean put(String key, String value) {
        if (key == null || value == null) {
            return false;
        }
        
        lock.writeLock().lock();
        try {
            // Write to WAL first
            try {
                wal.append("PUT", key, value);
            } catch (IOException e) {
                logger.severe("Failed to write to WAL: " + e.getMessage());
                return false;
            }
            
            // Update memtable
            memtable.put(key, value);
            deletedKeys.remove(key);
            writeCount++;
            
            // Check if memtable should be flushed
            if (writeCount >= MEMTABLE_SIZE_THRESHOLD) {
                flushMemtable();
            }
            
            // Check if checkpoint is needed
            long now = System.currentTimeMillis();
            if (now - lastCheckpoint > CHECKPOINT_INTERVAL) {
                checkpoint();
            }
            
            return true;
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public Optional<String> read(String key) {
        if (key == null) {
            return Optional.empty();
        }
        
        lock.readLock().lock();
        try {
            // Check if key was deleted in memtable
            if (deletedKeys.contains(key)) {
                return Optional.empty();
            }
            
            // Check memtable first (most recent data)
            String value = memtable.get(key);
            if (value != null) {
                return Optional.of(value);
            }
            
            // Check SSTables
            try {
                value = sstableManager.get(key);
                return Optional.ofNullable(value);
            } catch (IOException e) {
                logger.severe("Error reading from SSTables: " + e.getMessage());
                return Optional.empty();
            }
            
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Map<String, String> readKeyRange(String startKey, String endKey) {
        lock.readLock().lock();
        try {
            Map<String, String> result = new TreeMap<>();
            
            // Get range from SSTables
            try {
                Map<String, String> sstableRange = sstableManager.getRange(startKey, endKey);
                result.putAll(sstableRange);
            } catch (IOException e) {
                logger.severe("Error reading range from SSTables: " + e.getMessage());
            }
            
            // Override with memtable data (newer values)
            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                String key = entry.getKey();
                if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) < 0) {
                    if (!deletedKeys.contains(key)) {
                        result.put(key, entry.getValue());
                    } else {
                        result.remove(key);
                    }
                }
            }
            
            return result;
            
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean batchPut(List<String> keys, List<String> values) {
        if (keys == null || values == null || keys.size() != values.size()) {
            return false;
        }
        
        lock.writeLock().lock();
        try {
            boolean allSuccess = true;
            
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                String value = values.get(i);
                
                if (!put(key, value)) {
                    allSuccess = false;
                }
            }
            
            return allSuccess;
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public boolean delete(String key) {
        if (key == null) {
            return false;
        }
        
        lock.writeLock().lock();
        try {
            // Write delete to WAL
            try {
                wal.append("DELETE", key, null);
            } catch (IOException e) {
                logger.severe("Failed to write delete to WAL: " + e.getMessage());
                return false;
            }
            
            // Remove from memtable and mark as deleted
            memtable.remove(key);
            deletedKeys.add(key);
            writeCount++;
            
            // Check if memtable should be flushed
            if (writeCount >= MEMTABLE_SIZE_THRESHOLD) {
                flushMemtable();
            }
            
            return true;
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Flush the memtable to SSTable.
     */
    private void flushMemtable() {
        if (memtable.isEmpty()) {
            return;
        }
        
        try {
            // Create a copy of memtable data
            Map<String, String> dataToFlush = new HashMap<>(memtable);
            
            // Create new SSTable
            sstableManager.createSSTable(dataToFlush);
            
            // Clear memtable and reset counters
            memtable.clear();
            deletedKeys.clear();
            writeCount = 0;
            
            logger.info("Flushed memtable to SSTable with " + dataToFlush.size() + " entries");
            
        } catch (IOException e) {
            logger.severe("Failed to flush memtable: " + e.getMessage());
        }
    }
    
    /**
     * Create a checkpoint by flushing memtable and truncating WAL.
     */
    private void checkpoint() {
        try {
            // Flush memtable
            flushMemtable();
            
            // Truncate WAL
            wal.truncate();
            
            lastCheckpoint = System.currentTimeMillis();
            
            logger.info("Checkpoint completed");
            
        } catch (IOException e) {
            logger.severe("Failed to create checkpoint: " + e.getMessage());
        }
    }
    
    /**
     * Force a compaction of SSTables.
     */
    public void compact() {
        lock.writeLock().lock();
        try {
            sstableManager.compact();
            logger.info("Forced SSTable compaction");
        } catch (IOException e) {
            logger.severe("Failed to compact SSTables: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get statistics about the store.
     */
    public StoreStats getStats() {
        lock.readLock().lock();
        try {
            SSTableManager.SSTableStats sstableStats = sstableManager.getStats();
            long walSize = 0;
            try {
                walSize = wal.getSize();
            } catch (IOException e) {
                logger.warning("Failed to get WAL size: " + e.getMessage());
            }
            
            return new StoreStats(
                memtable.size(),
                deletedKeys.size(),
                sstableStats.getSSTableCount(),
                sstableStats.getTotalEntries(),
                sstableStats.getTotalSize(),
                walSize
            );
            
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            // Flush memtable before closing
            flushMemtable();
            
            // Close components
            wal.close();
            sstableManager.close();
            
            logger.info("Enhanced KV Store closed");
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Statistics about the enhanced store.
     */
    public static class StoreStats {
        private final int memtableSize;
        private final int deletedKeysCount;
        private final int sstableCount;
        private final int totalEntries;
        private final long totalSize;
        private final long walSize;
        
        public StoreStats(int memtableSize, int deletedKeysCount, int sstableCount,
                         int totalEntries, long totalSize, long walSize) {
            this.memtableSize = memtableSize;
            this.deletedKeysCount = deletedKeysCount;
            this.sstableCount = sstableCount;
            this.totalEntries = totalEntries;
            this.totalSize = totalSize;
            this.walSize = walSize;
        }
        
        public int getMemtableSize() {
            return memtableSize;
        }
        
        public int getDeletedKeysCount() {
            return deletedKeysCount;
        }
        
        public int getSSTableCount() {
            return sstableCount;
        }
        
        public int getTotalEntries() {
            return totalEntries;
        }
        
        public long getTotalSize() {
            return totalSize;
        }
        
        public long getWALSize() {
            return walSize;
        }
        
        @Override
        public String toString() {
            return String.format("StoreStats{memtable=%d, deleted=%d, sstables=%d, entries=%d, size=%d bytes, wal=%d bytes}",
                               memtableSize, deletedKeysCount, sstableCount, totalEntries, totalSize, walSize);
        }
    }
}
