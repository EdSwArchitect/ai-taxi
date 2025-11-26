package com.example;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Simple HTTP server to expose Prometheus metrics for scraping.
 * Combines metrics from both ParquetToOpenSearchUtil and ParquetToPostgresTableUtil.
 * 
 * Usage:
 *   MetricsServer server = MetricsServer.start(8080);
 *   // ... your application code ...
 *   server.stop();
 */
public class MetricsServer {
    
    private HttpServer httpServer;
    private final int port;
    
    private MetricsServer(int port) {
        this.port = port;
    }
    
    /**
     * Starts the metrics server on the specified port.
     * 
     * @param port The port to bind to (default: 8080)
     * @return MetricsServer instance
     * @throws IOException if the server cannot be started
     */
    public static MetricsServer start(int port) throws IOException {
        MetricsServer server = new MetricsServer(port);
        server.startServer();
        return server;
    }
    
    /**
     * Starts the metrics server on the default port 8080.
     * 
     * @return MetricsServer instance
     * @throws IOException if the server cannot be started
     */
    public static MetricsServer start() throws IOException {
        return start(8080);
    }
    
    /**
     * Starts the HTTP server and registers the /metrics endpoint.
     */
    private void startServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/metrics", new MetricsHandler());
        httpServer.createContext("/health", new HealthHandler());
        httpServer.setExecutor(null); // Use default executor
        httpServer.start();
        
        System.out.println("Metrics server started on http://localhost:" + port + "/metrics");
        System.out.println("Health check available at http://localhost:" + port + "/health");
    }
    
    /**
     * Stops the metrics server.
     */
    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            System.out.println("Metrics server stopped");
        }
    }
    
    /**
     * Gets the port the server is running on.
     * 
     * @return port number
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Handler for /metrics endpoint that returns Prometheus metrics format.
     */
    private static class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }
            
            try {
                // Combine metrics from both utility classes
                StringBuilder metrics = new StringBuilder();
                
                // Add OpenSearch metrics
                String openSearchMetrics = ParquetToOpenSearchUtil.Metrics.scrape();
                if (openSearchMetrics != null && !openSearchMetrics.isEmpty()) {
                    metrics.append(openSearchMetrics);
                    metrics.append("\n");
                }
                
                // Add PostgreSQL metrics
                String postgresMetrics = ParquetToPostgresTableUtil.Metrics.scrape();
                if (postgresMetrics != null && !postgresMetrics.isEmpty()) {
                    metrics.append(postgresMetrics);
                    metrics.append("\n");
                }
                
                // If no metrics yet, return empty response (Prometheus format)
                String response = metrics.length() > 0 ? metrics.toString() : "";
                
                // Set Content-Type header for Prometheus
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                sendResponse(exchange, 200, response);
                
            } catch (Exception e) {
                System.err.println("Error generating metrics: " + e.getMessage());
                e.printStackTrace();
                sendResponse(exchange, 500, "Internal Server Error: " + e.getMessage());
            }
        }
    }
    
    /**
     * Handler for /health endpoint.
     */
    private static class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }
            
            String response = "{\"status\":\"UP\"}";
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            sendResponse(exchange, 200, response);
        }
    }
    
    /**
     * Sends an HTTP response.
     */
    private static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }
    
    /**
     * Main method to run the metrics server standalone.
     * 
     * Usage: java -cp ... com.example.MetricsServer [port]
     */
    public static void main(String[] args) {
        int port = 8080;
        
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.err.println("Usage: MetricsServer [port]");
                System.exit(1);
            }
        }
        
        try {
            MetricsServer server = MetricsServer.start(port);
            System.out.println("Metrics server is running. Press Ctrl+C to stop.");
            
            // Keep the server running
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down metrics server...");
                server.stop();
            }));
            
            // Wait indefinitely
            Thread.currentThread().join();
            
        } catch (IOException e) {
            System.err.println("Failed to start metrics server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            System.out.println("Server interrupted");
        }
    }
}

