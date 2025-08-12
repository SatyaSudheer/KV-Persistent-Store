package com.kvstore.core;
import com.kvstore.api.KVStore;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class PersistentKVStore implements KVStore {
    private static final Logger logger = Logger.getLogger(PersistentKVStore.class.getName());

    private static final String DATA_FILE = "kvstore.dat";
    private static final String INDEX_FILE = "kvstore.idx";
    private static final String LOCK_FILE = "kvstore.lock";

    private final Path dataPath;
    private final Path indexPath;
    private final Path lockPath;
    private final Map<String, Long> keyIndex; // key -> file position
    private final ReadWriteLock indexLock;
    private RandomAccessFile dataFile;
    private FileLock fileLock;

    public PersistentKVStore(String directory) throws IOException {
        this.dataPath = Paths.get(directory, DATA_FILE);
        this.indexPath = Paths.get(directory, INDEX_FILE);
        this.lockPath = Paths.get(directory, LOCK_FILE);
        this.keyIndex = new ConcurrentHashMap<>();
        this.indexLock = new ReentrantReadWriteLock();

        initializeStore();
    }
    private void initializeStore() throws IOException {
        // Create directory if it doesn't exist
        Files.createDirectories(dataPath.getParent());

        // Acquire file lock to ensure single instance
        try (FileChannel lockChannel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileLock = lockChannel.tryLock();
            if (fileLock == null) {
                throw new IOException("Another instance is already running");
            }
        }

        // Initialize data file
        if (!Files.exists(dataPath)) {
            Files.createFile(dataPath);
        }

        // Open data file for random access
        dataFile = new RandomAccessFile(dataPath.toFile(), "rw");

        // Load index from disk
        loadIndex();

        logger.info("KV Store initialized successfully");
    }

    private void loadIndex() throws IOException {
        if (!Files.exists(indexPath)) {
            return;
        }

        try (BufferedReader reader = Files.newBufferedReader(indexPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length == 2) {
                    keyIndex.put(parts[0], Long.parseLong(parts[1]));
                }
            }
        }
        logger.info("Loaded " + keyIndex.size() + " keys from index");
    }

    private void saveIndex() throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(indexPath,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Map.Entry<String, Long> entry : keyIndex.entrySet()) {
                writer.write(entry.getKey() + "|" + entry.getValue());
                writer.newLine();
            }
        }
    }

    @Override
    public boolean put(String key, String value) {
        if (key == null || value == null) {
            return false;
        }

        try {
            indexLock.writeLock().lock();

            // Write to data file
            long position = dataFile.length();
            dataFile.seek(position);

            // Write key length, key, value length, value
            byte[] keyBytes = key.getBytes("UTF-8");
            byte[] valueBytes = value.getBytes("UTF-8");

            dataFile.writeInt(keyBytes.length);
            dataFile.write(keyBytes);
            dataFile.writeInt(valueBytes.length);
            dataFile.write(valueBytes);

            // Update index
            keyIndex.put(key, position);

            // Force write to disk
            dataFile.getFD().sync();

            return true;
        } catch (IOException e) {
            logger.severe("Error writing key-value pair: " + e.getMessage());
            return false;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public Optional<String> read(String key) {
        if (key == null) {
            return Optional.empty();
        }

        try {
            indexLock.readLock().lock();

            Long position = keyIndex.get(key);
            if (position == null) {
                return Optional.empty();
            }

            if (dataFile == null) {
                logger.severe("Data file is null for key: " + key);
                return Optional.empty();
            }

            dataFile.seek(position);

            // Read key length and key
            int keyLength = dataFile.readInt();
            byte[] keyBytes = new byte[keyLength];
            dataFile.readFully(keyBytes);

            // Read value length and value
            int valueLength = dataFile.readInt();
            byte[] valueBytes = new byte[valueLength];
            dataFile.readFully(valueBytes);

            return Optional.of(new String(valueBytes, "UTF-8"));
        } catch (IOException e) {
            logger.severe("Error reading key-value pair for key '" + key + "': " + e.getMessage());
            return Optional.empty();
        } finally {
            indexLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, String> readKeyRange(String startKey, String endKey) {
        Map<String, String> result = new HashMap<>();

        try {
            indexLock.readLock().lock();

            for (Map.Entry<String, Long> entry : keyIndex.entrySet()) {
                String key = entry.getKey();
                if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) < 0) {
                    Long position = entry.getValue();
                    try {
                        dataFile.seek(position);

                        // Read key length and key
                        int keyLength = dataFile.readInt();
                        byte[] keyBytes = new byte[keyLength];
                        dataFile.readFully(keyBytes);

                        // Read value length and value
                        int valueLength = dataFile.readInt();
                        byte[] valueBytes = new byte[valueLength];
                        dataFile.readFully(valueBytes);

                        result.put(key, new String(valueBytes, "UTF-8"));
                    } catch (IOException e) {
                        logger.severe("Error reading key-value pair in range: " + e.getMessage());
                    }
                }
            }
        } finally {
            indexLock.readLock().unlock();
        }

        return result;
    }

    @Override
    public boolean batchPut(List<String> keys, List<String> values) {
        if (keys == null || values == null || keys.size() != values.size()) {
            return false;
        }

        try {
            indexLock.writeLock().lock();

            // Start transaction
            long startPosition = dataFile.length();

            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                String value = values.get(i);

                if (key == null || value == null) {
                    // Rollback by truncating file
                    dataFile.setLength(startPosition);
                    return false;
                }

                // Write to data file
                long position = dataFile.length();
                dataFile.seek(position);

                byte[] keyBytes = key.getBytes("UTF-8");
                byte[] valueBytes = value.getBytes("UTF-8");

                dataFile.writeInt(keyBytes.length);
                dataFile.write(keyBytes);
                dataFile.writeInt(valueBytes.length);
                dataFile.write(valueBytes);

                // Update index
                keyIndex.put(key, position);
            }

            // Force write to disk
            dataFile.getFD().sync();

            return true;
        } catch (IOException e) {
            logger.severe("Error in batch put: " + e.getMessage());
            return false;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        if (key == null) {
            return false;
        }

        try {
            indexLock.writeLock().lock();

            if (keyIndex.remove(key) != null) {
                return true;
            }

            return false;
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            if (dataFile != null) {
                saveIndex();
                dataFile.close();
            }
            if (fileLock != null && fileLock.isValid()) {
                fileLock.release();
            }
            logger.info("KV Store closed successfully");
        } catch (IOException e) {
            logger.severe("Error closing KV Store: " + e.getMessage());
        }
    }
}
