package com.kvstore.core;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
public class SSTableManager {
    private static final Logger logger = Logger.getLogger(SSTableManager.class.getName());
    private static final String MANIFEST_FILE = "sst_manifest";
    private static final int MAX_SSTABLES = 10; // Maximum number of SSTables before compaction
    private static final long COMPACTION_THRESHOLD = 100 * 1024 * 1024; // 100MB
    
    private final String dataDirectory;
    private final ReentrantReadWriteLock lock;
    private final List<SSTable> sstables;
    private final Map<Long, SSTable> sstableMap;
    
    public SSTableManager(String dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        this.lock = new ReentrantReadWriteLock();
        this.sstables = new ArrayList<>();
        this.sstableMap = new HashMap<>();
        
        loadExistingSSTables();
    }

    private void loadExistingSSTables() throws IOException {
        Path manifestPath = Paths.get(dataDirectory, MANIFEST_FILE);
        
        if (!Files.exists(manifestPath)) {
            logger.info("No SSTable manifest found, starting with empty state");
            return;
        }
        
        try (DataInputStream manifestIn = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(manifestPath)))) {
            
            int sstableCount = manifestIn.readInt();
            logger.info("Loading " + sstableCount + " existing SSTables");
            
            for (int i = 0; i < sstableCount; i++) {
                long fileId = manifestIn.readLong();
                try {
                    SSTable sstable = SSTable.load(dataDirectory, fileId);
                    sstables.add(sstable);
                    sstableMap.put(fileId, sstable);
                } catch (IOException e) {
                    logger.warning("Failed to load SSTable " + fileId + ": " + e.getMessage());
                }
            }
        }
        
        // Sort SSTables by creation time (oldest first)
        sstables.sort(Comparator.comparingLong(SSTable::getCreationTime));
        
        logger.info("Loaded " + sstables.size() + " SSTables");
    }

    private void saveManifest() throws IOException {
        Path manifestPath = Paths.get(dataDirectory, MANIFEST_FILE);
        
        try (DataOutputStream manifestOut = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(manifestPath)))) {
            
            manifestOut.writeInt(sstables.size());
            
            for (SSTable sstable : sstables) {
                manifestOut.writeLong(sstable.getFileId());
            }
        }
        
        logger.fine("Saved SSTable manifest with " + sstables.size() + " SSTables");
    }

    public void createSSTable(Map<String, String> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        
        lock.writeLock().lock();
        try {
            SSTable sstable = SSTable.create(dataDirectory, entries);
            sstables.add(sstable);
            sstableMap.put(sstable.getFileId(), sstable);
            
            saveManifest();
            
            // Check if compaction is needed
            if (sstables.size() > MAX_SSTABLES) {
                compact();
            }
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public String get(String key) throws IOException {
        lock.readLock().lock();
        try {
            // Search from newest to oldest SSTable (latest value wins)
            for (int i = sstables.size() - 1; i >= 0; i--) {
                SSTable sstable = sstables.get(i);
                String value = sstable.get(key);
                if (value != null) {
                    return value;
                }
            }
            return null;
            
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> getRange(String startKey, String endKey) throws IOException {
        lock.readLock().lock();
        try {
            Map<String, String> result = new TreeMap<>();
            
            // Collect from all SSTables, newest values override older ones
            for (SSTable sstable : sstables) {
                Map<String, String> range = sstable.getRange(startKey, endKey);
                result.putAll(range);
            }
            
            return result;
            
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> getAll() throws IOException {
        lock.readLock().lock();
        try {
            Map<String, String> result = new TreeMap<>();
            
            // Collect from all SSTables, newest values override older ones
            for (SSTable sstable : sstables) {
                Map<String, String> all = sstable.getAll();
                result.putAll(all);
            }
            
            return result;
            
        } finally {
            lock.readLock().unlock();
        }
    }

    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            if (sstables.size() <= 1) {
                return;
            }
            
            logger.info("Starting SSTable compaction. Current SSTables: " + sstables.size());
            
            // Get all data from all SSTables
            Map<String, String> allData = getAll();
            
            // Delete old SSTables
            for (SSTable sstable : sstables) {
                sstable.delete();
            }
            
            // Clear lists
            sstables.clear();
            sstableMap.clear();
            
            // Create new compacted SSTable
            if (!allData.isEmpty()) {
                createSSTable(allData);
            }
            
            logger.info("SSTable compaction completed");
            
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void merge(int targetCount) throws IOException {
        lock.writeLock().lock();
        try {
            if (sstables.size() <= targetCount) {
                return;
            }
            
            logger.info("Starting SSTable merge. Target: " + targetCount + ", Current: " + sstables.size());
            
            // Group SSTables for merging
            int groupSize = (int) Math.ceil((double) sstables.size() / targetCount);
            
            for (int i = 0; i < sstables.size(); i += groupSize) {
                int endIndex = Math.min(i + groupSize, sstables.size());
                List<SSTable> group = sstables.subList(i, endIndex);
                
                // Merge group
                Map<String, String> mergedData = new TreeMap<>();
                for (SSTable sstable : group) {
                    Map<String, String> data = sstable.getAll();
                    mergedData.putAll(data);
                }
                
                // Delete old SSTables in group
                for (SSTable sstable : group) {
                    sstable.delete();
                }
                
                // Create new merged SSTable
                if (!mergedData.isEmpty()) {
                    SSTable mergedSSTable = SSTable.create(dataDirectory, mergedData);
                    sstables.set(i, mergedSSTable);
                    sstableMap.put(mergedSSTable.getFileId(), mergedSSTable);
                }
            }
            
            // Remove extra entries
            while (sstables.size() > targetCount) {
                SSTable removed = sstables.remove(sstables.size() - 1);
                sstableMap.remove(removed.getFileId());
            }
            
            saveManifest();
            
            logger.info("SSTable merge completed. Current SSTables: " + sstables.size());
            
        } finally {
            lock.writeLock().unlock();
        }
    }

    public SSTableStats getStats() {
        lock.readLock().lock();
        try {
            long totalSize = 0;
            int totalEntries = 0;
            
            for (SSTable sstable : sstables) {
                totalSize += sstable.getDataSize();
                totalEntries += sstable.getEntryCount();
            }
            
            return new SSTableStats(sstables.size(), totalEntries, totalSize);
            
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() {
        lock.writeLock().lock();
        try {
            // Close all SSTables
            for (SSTable sstable : sstables) {
                try {
                    sstable.close();
                } catch (Exception e) {
                    logger.warning("Error closing SSTable: " + e.getMessage());
                }
            }
            sstables.clear();
            sstableMap.clear();
            logger.info("SSTableManager closed");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static class SSTableStats {
        private final int sstableCount;
        private final int totalEntries;
        private final long totalSize;
        
        public SSTableStats(int sstableCount, int totalEntries, long totalSize) {
            this.sstableCount = sstableCount;
            this.totalEntries = totalEntries;
            this.totalSize = totalSize;
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
        
        @Override
        public String toString() {
            return String.format("SSTableStats{sstableCount=%d, totalEntries=%d, totalSize=%d bytes}",
                               sstableCount, totalEntries, totalSize);
        }
    }
}
