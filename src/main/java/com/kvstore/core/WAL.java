package com.kvstore.core;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
public class WAL {
    private static final Logger logger = Logger.getLogger(WAL.class.getName());
    private static final String WAL_FILE = "wal.log";
    private static final String WAL_INDEX = "wal.idx";
    
    private final Path walPath;
    private final Path indexPath;
    private final ReentrantReadWriteLock lock;
    private RandomAccessFile walFile;
    private long currentPosition;
    
    public WAL(String dataDirectory) throws IOException {
        this.walPath = Paths.get(dataDirectory, WAL_FILE);
        this.indexPath = Paths.get(dataDirectory, WAL_INDEX);
        this.lock = new ReentrantReadWriteLock();
        initializeWAL();
    }
    
    private void initializeWAL() throws IOException {
        // Create directory if it doesn't exist
        Files.createDirectories(walPath.getParent());
        
        // Initialize WAL file
        if (!Files.exists(walPath)) {
            Files.createFile(walPath);
        }
        
        // Open WAL file for appending
        walFile = new RandomAccessFile(walPath.toFile(), "rw");
        currentPosition = walFile.length();
        
        logger.info("WAL initialized at: " + walPath);
    }

    public long append(String operation, String key, String value) throws IOException {
        lock.writeLock().lock();
        try {
            // Seek to end of file
            walFile.seek(currentPosition);
            
            // Write operation record
            // Format: timestamp|operation|keyLength|key|valueLength|value
            long timestamp = System.currentTimeMillis();
            byte[] keyBytes = key.getBytes("UTF-8");
            byte[] valueBytes = value != null ? value.getBytes("UTF-8") : new byte[0];
            
            // Write record
            walFile.writeLong(timestamp);
            walFile.writeUTF(operation);
            walFile.writeInt(keyBytes.length);
            walFile.write(keyBytes);
            walFile.writeInt(valueBytes.length);
            if (valueBytes.length > 0) {
                walFile.write(valueBytes);
            }
            
            // Force write to disk
            walFile.getFD().sync();
            
            long position = currentPosition;
            currentPosition = walFile.getFilePointer();
            
            logger.fine("WAL append: " + operation + "|" + key + " at position " + position);
            return position;
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    public void replay(RecoveryHandler recoveryHandler) throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(walPath) || Files.size(walPath) == 0) {
                logger.info("WAL is empty, no recovery needed");
                return;
            }
            
            logger.info("Starting WAL replay...");
            int recoveredOperations = 0;
            
            // Open WAL file for reading
            try (RandomAccessFile reader = new RandomAccessFile(walPath.toFile(), "r")) {
                long position = 0;
                
                while (position < reader.length()) {
                    reader.seek(position);
                    
                    try {
                        // Read record
                        long timestamp = reader.readLong();
                        String operation = reader.readUTF();
                        int keyLength = reader.readInt();
                        byte[] keyBytes = new byte[keyLength];
                        reader.readFully(keyBytes);
                        String key = new String(keyBytes, "UTF-8");
                        
                        int valueLength = reader.readInt();
                        String value = null;
                        if (valueLength > 0) {
                            byte[] valueBytes = new byte[valueLength];
                            reader.readFully(valueBytes);
                            value = new String(valueBytes, "UTF-8");
                        }
                        
                        // Process the operation
                        recoveryHandler.handleOperation(operation, key, value, timestamp);
                        recoveredOperations++;
                        
                        position = reader.getFilePointer();
                        
                    } catch (EOFException e) {
                        // End of file reached
                        break;
                    } catch (Exception e) {
                        logger.warning("Error reading WAL record at position " + position + ": " + e.getMessage());
                        // Try to continue from next position
                        position = reader.getFilePointer();
                    }
                }
            }
            
            logger.info("WAL replay completed. Recovered " + recoveredOperations + " operations");
            
        } finally {
            lock.readLock().unlock();
        }
    }
    public void truncate() throws IOException {
        lock.writeLock().lock();
        try {
            // Create a new empty WAL file
            Files.deleteIfExists(walPath);
            Files.createFile(walPath);
            
            // Reopen the file
            if (walFile != null) {
                walFile.close();
            }
            walFile = new RandomAccessFile(walPath.toFile(), "rw");
            currentPosition = 0;
            
            logger.info("WAL truncated successfully");
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    public long getSize() throws IOException {
        lock.readLock().lock();
        try {
            return Files.exists(walPath) ? Files.size(walPath) : 0;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void close() {
        lock.writeLock().lock();
        try {
            if (walFile != null) {
                try {
                    walFile.close();
                } catch (IOException e) {
                    logger.warning("Error closing WAL file: " + e.getMessage());
                }
            }
            logger.info("WAL closed");
        } finally {
            lock.writeLock().unlock();
        }
    }
    public interface RecoveryHandler {
        /**
         * Handle a recovered operation from the WAL.
         * 
         * @param operation The operation type (PUT, DELETE)
         * @param key The key
         * @param value The value (can be null for DELETE operations)
         * @param timestamp The timestamp when the operation was logged
         */
        void handleOperation(String operation, String key, String value, long timestamp);
    }
    
}
