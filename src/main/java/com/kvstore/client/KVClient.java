package com.kvstore.client;

import java.io.*;
import java.net.Socket;
import java.util.logging.Logger;

public class KVClient {
    private static final Logger logger = Logger.getLogger(KVClient.class.getName());
    private final String host;
    private final int port;
    
    public KVClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public String sendCommand(String command) {
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            out.println(command);
            return in.readLine();
        } catch (IOException e) {
            logger.severe("Error sending command: " + e.getMessage());
            return "ERROR|Connection failed";
        }
    }
    
    public boolean put(String key, String value) {
        String response = sendCommand("PUT|" + key + "|" + value);
        return "OK".equals(response);
    }
    
    public String get(String key) {
        String response = sendCommand("GET|" + key);
        if (response.startsWith("OK|")) {
            return response.substring(3);
        }
        return null;
    }
    
    public boolean delete(String key) {
        String response = sendCommand("DELETE|" + key);
        return "OK".equals(response);
    }
    
    public boolean ping() {
        String response = sendCommand("PING");
        return "PONG".equals(response);
    }
}
