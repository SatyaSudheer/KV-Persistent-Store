package com.kvstore.core;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
public class SSTable {
    private static final Logger logger = Logger.getLogger(SSTable.class.getName());
    private static final String SST_FILE_PREFIX = "sst_";
    private static final String SST_INDEX_SUFFIX = ".idx";
    private static final String SST_DATA_SUFFIX = ".dat";
    
    private final Path dataPath;
    private final Path indexPath;
    private final ReentrantReadWriteLock lock;
    private final long fileId;
    private final long creationTime;
    private final int entryCount;
    private final long dataSize;

    private final Map<String, Long> keyIndex;
    
    private SSTable(Path dataPath, Path indexPath, long fileId, long creationTime, 
                   int entryCount, long dataSize, Map<String, Long> keyIndex) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.fileId = fileId;
        this.creationTime = creationTime;
        this.entryCount = entryCount;
        this.dataSize = dataSize;
        this.keyIndex = keyIndex;
        this.lock = new ReentrantReadWriteLock();
    }

    public static SSTable create(String dataDirectory, Map<String, String> entries) throws IOException {
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("Cannot create SSTable with empty entries");
        }
        
        // Sort entries by key
        TreeMap<String, String> sortedEntries = new TreeMap<>(entries);
        
        // Generate file ID and paths
        long fileId = System.currentTimeMillis();
        Path dataPath = Paths.get(dataDirectory, SST_FILE_PREFIX + fileId + SST_DATA_SUFFIX);
        Path indexPath = Paths.get(dataDirectory, SST_FILE_PREFIX + fileId + SST_INDEX_SUFFIX);
        
        // Create directory if it doesn't exist
        Files.createDirectories(Paths.get(dataDirectory));
        
        // Write data file
        Map<String, Long> keyIndex = new HashMap<>();
        long dataSize = 0;
        
        try (DataOutputStream dataOut = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(dataPath)))) {
            
            for (Map.Entry<String, String> entry : sortedEntries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                
                // Record position for index
                keyIndex.put(key, (long) dataOut.size());
                
                // Write entry: keyLength|key|valueLength|value
                byte[] keyBytes = key.getBytes("UTF-8");
                byte[] valueBytes = value.getBytes("UTF-8");
                
                dataOut.writeInt(keyBytes.length);
                dataOut.write(keyBytes);
                dataOut.writeInt(valueBytes.length);
                dataOut.write(valueBytes);
                
                dataSize = dataOut.size();
            }
        }
        
        // Write index file
        try (DataOutputStream indexOut = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(indexPath)))) {
            
            indexOut.writeLong(fileId);
            indexOut.writeLong(System.currentTimeMillis());
            indexOut.writeInt(sortedEntries.size());
            indexOut.writeLong(dataSize);
            
            for (Map.Entry<String, Long> indexEntry : keyIndex.entrySet()) {
                String key = indexEntry.getKey();
                Long position = indexEntry.getValue();
                
                byte[] keyBytes = key.getBytes("UTF-8");
                indexOut.writeInt(keyBytes.length);
                indexOut.write(keyBytes);
                indexOut.writeLong(position);
            }
        }
        
        SSTable sstable = new SSTable(dataPath, indexPath, fileId, System.currentTimeMillis(),
                                     sortedEntries.size(), dataSize, keyIndex);
        
        logger.info("Created SSTable: " + fileId + " with " + sortedEntries.size() + " entries");
        return sstable;
    }

    public static SSTable load(String dataDirectory, long fileId) throws IOException {
        Path dataPath = Paths.get(dataDirectory, SST_FILE_PREFIX + fileId + SST_DATA_SUFFIX);
        Path indexPath = Paths.get(dataDirectory, SST_FILE_PREFIX + fileId + SST_INDEX_SUFFIX);
        
        if (!Files.exists(dataPath) || !Files.exists(indexPath)) {
            throw new IOException("SSTable files not found for fileId: " + fileId);
        }
        
        // Read index file
        Map<String, Long> keyIndex = new HashMap<>();
        long creationTime;
        int entryCount;
        long dataSize;
        
        try (DataInputStream indexIn = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(indexPath)))) {
            
            long indexFileId = indexIn.readLong();
            if (indexFileId != fileId) {
                throw new IOException("File ID mismatch in index file");
            }
            
            creationTime = indexIn.readLong();
            entryCount = indexIn.readInt();
            dataSize = indexIn.readLong();
            
            // Read key index
            for (int i = 0; i < entryCount; i++) {
                int keyLength = indexIn.readInt();
                byte[] keyBytes = new byte[keyLength];
                indexIn.readFully(keyBytes);
                String key = new String(keyBytes, "UTF-8");
                Long position = indexIn.readLong();
                keyIndex.put(key, position);
            }
        }
        
        SSTable sstable = new SSTable(dataPath, indexPath, fileId, creationTime,
                                     entryCount, dataSize, keyIndex);
        
        logger.info("Loaded SSTable: " + fileId + " with " + entryCount + " entries");
        return sstable;
    }

    public String get(String key) throws IOException {
        lock.readLock().lock();
        try {
            Long position = keyIndex.get(key);
            if (position == null) {
                return null;
            }
            
            try (RandomAccessFile dataFile = new RandomAccessFile(dataPath.toFile(), "r")) {
                dataFile.seek(position);
                
                // Read entry
                int keyLength = dataFile.readInt();
                byte[] keyBytes = new byte[keyLength];
                dataFile.readFully(keyBytes);
                
                int valueLength = dataFile.readInt();
                byte[] valueBytes = new byte[valueLength];
                dataFile.readFully(valueBytes);
                
                return new String(valueBytes, "UTF-8");
            }
            
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> getRange(String startKey, String endKey) throws IOException {
        lock.readLock().lock();
        try {
            Map<String, String> result = new TreeMap<>();
            
            // Find keys in range
            SortedMap<String, Long> rangeIndex = keyIndex.entrySet().stream()
                .filter(entry -> {
                    String key = entry.getKey();
                    return key.compareTo(startKey) >= 0 && key.compareTo(endKey) < 0;
                })
                .collect(TreeMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), TreeMap::putAll);
            
            // Read values
            try (RandomAccessFile dataFile = new RandomAccessFile(dataPath.toFile(), "r")) {
                for (Map.Entry<String, Long> entry : rangeIndex.entrySet()) {
                    String key = entry.getKey();
                    Long position = entry.getValue();
                    
                    dataFile.seek(position);
                    
                    // Read entry
                    int keyLength = dataFile.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    dataFile.readFully(keyBytes);
                    
                    int valueLength = dataFile.readInt();
                    byte[] valueBytes = new byte[valueLength];
                    dataFile.readFully(valueBytes);
                    
                    String value = new String(valueBytes, "UTF-8");
                    result.put(key, value);
                }
            }
            
            return result;
            
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> getAll() throws IOException {
        return getRange("", "\uffff"); // Use unicode range to get all keys
    }

    public boolean containsKey(String key) {
        lock.readLock().lock();
        try {
            return keyIndex.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getFileId() {
        return fileId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public long getDataSize() {
        return dataSize;
    }
    public Set<String> getKeys() {
        lock.readLock().lock();
        try {
            return new HashSet<>(keyIndex.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    public void delete() throws IOException {
        lock.writeLock().lock();
        try {
            Files.deleteIfExists(dataPath);
            Files.deleteIfExists(indexPath);
            logger.info("Deleted SSTable: " + fileId);
        } finally {
            lock.writeLock().unlock();
        }
    }
    public Path getDataPath() {
        return dataPath;
    }
    public Path getIndexPath() {
        return indexPath;
    }
    
    public void close() {
        // SSTable is immutable, so no resources to close
        // This method is provided for consistency with other components
        logger.fine("SSTable closed: " + fileId);
    }
}
