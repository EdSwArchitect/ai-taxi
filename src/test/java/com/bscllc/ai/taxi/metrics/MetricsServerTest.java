package com.bscllc.ai.taxi.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for MetricsServer.
 * Tests server lifecycle, HTTP endpoints, and error handling.
 */
@DisplayName("MetricsServer Tests")
class MetricsServerTest {

    private MetricsServer server;
    private HttpClient httpClient;
    private int testPort;

    @BeforeEach
    void setUp() {
        // Use a random port to avoid conflicts
        testPort = 9000 + ThreadLocalRandom.current().nextInt(1000);
        httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Test
    @DisplayName("Should start server on specified port")
    void testStartOnSpecifiedPort() throws IOException {
        server = MetricsServer.start(testPort);
        assertNotNull(server);
        assertEquals(testPort, server.getPort());
    }

    @Test
    @DisplayName("Should start server on default port")
    void testStartOnDefaultPort() throws IOException {
        server = MetricsServer.start();
        assertNotNull(server);
        assertEquals(8080, server.getPort());
        server.stop(); // Clean up before next test
        server = null;
    }

    @Test
    @DisplayName("Should get correct port number")
    void testGetPort() throws IOException {
        int customPort = testPort + 100;
        server = MetricsServer.start(customPort);
        assertEquals(customPort, server.getPort());
    }

    @Test
    @DisplayName("Should stop server successfully")
    void testStopServer() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        assertNotNull(server);
        
        // Verify server is running by making a request
        URI healthUri = URI.create("http://localhost:" + testPort + "/health");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(healthUri)
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        
        // Stop the server
        server.stop();
        
        // Wait a bit for server to stop
        Thread.sleep(100);
        
        // Verify server is stopped (subsequent requests should fail)
        // Note: This might throw IOException if connection is refused, which is expected
        assertThrows(Exception.class, () -> {
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        });
    }

    @Test
    @DisplayName("Should respond to GET /health endpoint")
    void testHealthEndpoint() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        URI healthUri = URI.create("http://localhost:" + testPort + "/health");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(healthUri)
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\""));
        assertTrue(response.body().contains("UP"));
        
        // Verify Content-Type header
        String contentType = response.headers().firstValue("Content-Type").orElse("");
        assertTrue(contentType.contains("application/json"));
    }

    @Test
    @DisplayName("Should reject non-GET requests to /health endpoint")
    void testHealthEndpointNonGetMethod() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        URI healthUri = URI.create("http://localhost:" + testPort + "/health");
        HttpRequest postRequest = HttpRequest.newBuilder()
            .uri(healthUri)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
        
        HttpResponse<String> response = httpClient.send(postRequest, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(405, response.statusCode());
        assertEquals("Method Not Allowed", response.body());
    }

    @Test
    @DisplayName("Should respond to GET /metrics endpoint")
    void testMetricsEndpoint() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        // Wait a bit for server to be fully ready
        Thread.sleep(100);
        
        URI metricsUri = URI.create("http://localhost:" + testPort + "/metrics");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(metricsUri)
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(200, response.statusCode());
        
        // Verify Content-Type header for Prometheus format
        String contentType = response.headers().firstValue("Content-Type").orElse("");
        assertTrue(contentType.contains("text/plain"));
        assertTrue(contentType.contains("version=0.0.4"));
        
        // Response body may be empty if no metrics have been collected yet, which is valid
        String body = response.body();
        assertNotNull(body);
    }

    @Test
    @DisplayName("Should reject non-GET requests to /metrics endpoint")
    void testMetricsEndpointNonGetMethod() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        URI metricsUri = URI.create("http://localhost:" + testPort + "/metrics");
        HttpRequest postRequest = HttpRequest.newBuilder()
            .uri(metricsUri)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
        
        HttpResponse<String> response = httpClient.send(postRequest, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(405, response.statusCode());
        assertEquals("Method Not Allowed", response.body());
    }

    @Test
    @DisplayName("Should return 404 for unknown endpoints")
    void testUnknownEndpoint() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        URI unknownUri = URI.create("http://localhost:" + testPort + "/unknown");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(unknownUri)
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        // HttpServer typically returns 404 for unknown paths
        assertEquals(404, response.statusCode());
    }

    @Test
    @DisplayName("Should handle multiple concurrent requests")
    void testConcurrentRequests() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        URI healthUri = URI.create("http://localhost:" + testPort + "/health");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(healthUri)
            .GET()
            .build();
        
        // Send multiple concurrent requests
        Thread thread1 = new Thread(() -> {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
            } catch (Exception e) {
                fail("Thread 1 failed: " + e.getMessage());
            }
        });
        
        Thread thread2 = new Thread(() -> {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
            } catch (Exception e) {
                fail("Thread 2 failed: " + e.getMessage());
            }
        });
        
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
    }

    @Test
    @DisplayName("Should stop server multiple times safely")
    void testStopMultipleTimes() throws IOException {
        server = MetricsServer.start(testPort);
        assertNotNull(server);
        
        // Stop multiple times should not throw exception
        assertDoesNotThrow(() -> server.stop());
        assertDoesNotThrow(() -> server.stop());
        assertDoesNotThrow(() -> server.stop());
    }

    @Test
    @DisplayName("Should handle server startup failure on invalid port")
    void testStartOnInvalidPort() {
        // Port -1 should fail
        assertThrows(IllegalArgumentException.class, () -> {
            MetricsServer.start(-1);
        });
    }

    @Test
    @DisplayName("Should be able to restart server after stopping")
    void testRestartServer() throws IOException, InterruptedException {
        // Start first server
        server = MetricsServer.start(testPort);
        URI healthUri = URI.create("http://localhost:" + testPort + "/health");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(healthUri)
            .GET()
            .build();
        
        HttpResponse<String> response1 = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response1.statusCode());
        
        // Stop server
        server.stop();
        Thread.sleep(200); // Wait for port to be released
        
        // Start server again on same port
        server = MetricsServer.start(testPort);
        HttpResponse<String> response2 = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response2.statusCode());
    }

    @Test
    @DisplayName("Should have both /metrics and /health endpoints available")
    void testBothEndpointsAvailable() throws IOException, InterruptedException {
        server = MetricsServer.start(testPort);
        
        URI healthUri = URI.create("http://localhost:" + testPort + "/health");
        URI metricsUri = URI.create("http://localhost:" + testPort + "/metrics");
        
        HttpRequest healthRequest = HttpRequest.newBuilder()
            .uri(healthUri)
            .GET()
            .build();
        
        HttpRequest metricsRequest = HttpRequest.newBuilder()
            .uri(metricsUri)
            .GET()
            .build();
        
        HttpResponse<String> healthResponse = httpClient.send(healthRequest, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> metricsResponse = httpClient.send(metricsRequest, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(200, healthResponse.statusCode());
        assertEquals(200, metricsResponse.statusCode());
    }
}

