package com.kvstore.api;

public interface KVServer {
    void start(int port) throws Exception;
    void stop();
    boolean isRunning();
}
