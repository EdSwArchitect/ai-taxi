package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for OpenSearchIndexUtil using Testcontainers.
 * This test class creates an OpenSearch container for testing.
 */
@Testcontainers
class OpenSearchIndexUtilTest {

    @Container
    private static final GenericContainer<?> opensearch = new GenericContainer<>(
            DockerImageName.parse("opensearchproject/opensearch:3.3.0"))
            .withEnv("discovery.type", "single-node")
            .withEnv("DISABLE_SECURITY_PLUGIN", "true")
            .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
            .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
            .withEnv("bootstrap.memory_lock", "false")
            .withExposedPorts(9200)
            .withStartupTimeout(java.time.Duration.ofMinutes(3))
            .waitingFor(Wait.forHttp("/_cluster/health")
                    .forPort(9200)
                    .forStatusCodeMatching(response -> response == 200 || response == 401)
                    .withStartupTimeout(java.time.Duration.ofMinutes(3)));

    private OpenSearchIndexUtil util;

    @BeforeEach
    void setUp() throws IOException {
        // Wait for container to be ready
        if (!opensearch.isRunning()) {
            throw new IllegalStateException("OpenSearch container is not running");
        }
        
        // Create OpenSearch client connected to the test container
        // Testcontainers maps container ports to localhost on the host machine
        String host = "localhost";
        Integer port = opensearch.getMappedPort(9200);
        
        if (port == null) {
            throw new IllegalStateException("OpenSearch container port not available. Is the container running?");
        }
        
        // Wait for container to be fully ready - OpenSearch can take time to start
        int maxRetries = 30;
        int retryCount = 0;
        boolean isReady = false;
        
        while (retryCount < maxRetries && !isReady) {
            try {
                // Try to create client and check if it works
                OpenSearchClient testClient = OpenSearchIndexUtil.createClient(host, port, "http");
                // Try a simple operation to verify connection
                try {
                    testClient.cluster().health();
                    isReady = true;
                    testClient._transport().close();
                } catch (Exception e) {
                    retryCount++;
                    if (retryCount < maxRetries) {
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                retryCount++;
                if (retryCount < maxRetries) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for OpenSearch container", ie);
                    }
                } else {
                    throw new IOException("OpenSearch container not ready after " + maxRetries + " attempts. Last error: " + e.getMessage(), e);
                }
            }
        }
        
        if (!isReady) {
            throw new IllegalStateException("OpenSearch container did not become ready after " + maxRetries + " attempts");
        }
        
        OpenSearchClient client = OpenSearchIndexUtil.createClient(host, port, "http");
        util = new OpenSearchIndexUtil(client);
    
        System.out.println("OpenSearch client created?: " + (client != null));
        // Note: OpenSearchClient will be closed in tearDown via util.close()
        
        System.out.println("OpenSearch container started at: " + host + ":" + port);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (util != null) {
            util.close();
        }
    }

    @Test
    void testCreateIndex() throws IOException {
        String indexName = "test-index";
        
        // Verify index doesn't exist initially
        assertFalse(util.indexExists(indexName), "Index should not exist initially");
        
        // Create the index
        boolean created = util.createIndex(indexName);
        assertTrue(created, "Index should be created successfully");
        
        // Verify index exists
        assertTrue(util.indexExists(indexName), "Index should exist after creation");
    }

    @Test
    void testCreateIndexWithSettings() throws IOException {
        String indexName = "test-index-settings";
        
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", "1");
        settings.put("number_of_replicas", "0");
        
        boolean created = util.createIndex(indexName, settings);
        assertTrue(created, "Index with settings should be created successfully");
        assertTrue(util.indexExists(indexName), "Index should exist after creation");
    }

    @Test
    void testCreateIndexAlreadyExists() throws IOException {
        String indexName = "test-index-duplicate";
        
        // Create index first time
        boolean firstCreation = util.createIndex(indexName);
        assertTrue(firstCreation, "First index creation should succeed");
        
        // Try to create again - should return false
        boolean secondCreation = util.createIndex(indexName);
        assertFalse(secondCreation, "Second index creation should return false (already exists)");
    }

    @Test
    void testIndexExists() throws IOException {
        String indexName = "test-index-exists";
        
        // Index should not exist initially
        assertFalse(util.indexExists(indexName), "Index should not exist");
        
        // Create index
        util.createIndex(indexName);
        
        // Index should exist now
        assertTrue(util.indexExists(indexName), "Index should exist after creation");
    }

    @Test
    void testIndexExistsWithInvalidName() throws IOException {
        // Test with null
        assertFalse(util.indexExists(null), "Null index name should return false");
        
        // Test with empty string
        assertFalse(util.indexExists(""), "Empty index name should return false");
        
        // Test with non-existent index
        assertFalse(util.indexExists("non-existent-index"), "Non-existent index should return false");
    }

    @Test
    void testDeleteIndex() throws IOException {
        String indexName = "test-index-delete";
        
        // Create index first
        util.createIndex(indexName);
        assertTrue(util.indexExists(indexName), "Index should exist before deletion");
        
        // Delete the index
        boolean deleted = util.deleteIndex(indexName);
        assertTrue(deleted, "Index should be deleted successfully");
        
        // Verify index no longer exists
        assertFalse(util.indexExists(indexName), "Index should not exist after deletion");
    }

    @Test
    void testDeleteNonExistentIndex() throws IOException {
        String indexName = "non-existent-index";
        
        // Try to delete non-existent index
        boolean deleted = util.deleteIndex(indexName);
        assertFalse(deleted, "Deleting non-existent index should return false");
    }

    @Test
    void testCreateIndexWithEmptyName() {
        // Test with null index name
        assertThrows(IllegalArgumentException.class, () -> {
            util.createIndex(null);
        }, "Creating index with null name should throw IllegalArgumentException");
        
        // Test with empty index name
        assertThrows(IllegalArgumentException.class, () -> {
            util.createIndex("");
        }, "Creating index with empty name should throw IllegalArgumentException");
        
        // Test with whitespace-only index name
        assertThrows(IllegalArgumentException.class, () -> {
            util.createIndex("   ");
        }, "Creating index with whitespace-only name should throw IllegalArgumentException");
    }

    @Test
    void testCreateIndexWithSettingsAndMappings() throws IOException {
        String indexName = "test-index-full";
        
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", "1");
        settings.put("number_of_replicas", "0");
        
        Map<String, Object> mappings = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        Map<String, String> field1 = new HashMap<>();
        field1.put("type", "keyword");
        properties.put("field1", field1);
        mappings.put("properties", properties);
        
        boolean created = util.createIndex(indexName, settings, mappings);
        assertTrue(created, "Index with settings and mappings should be created successfully");
        assertTrue(util.indexExists(indexName), "Index should exist after creation");
    }

    @Test
    void testMultipleIndices() throws IOException {
        // Create multiple indices
        String index1 = "test-index-1";
        String index2 = "test-index-2";
        String index3 = "test-index-3";
        
        assertTrue(util.createIndex(index1), "First index should be created");
        assertTrue(util.createIndex(index2), "Second index should be created");
        assertTrue(util.createIndex(index3), "Third index should be created");
        
        // Verify all exist
        assertTrue(util.indexExists(index1), "First index should exist");
        assertTrue(util.indexExists(index2), "Second index should exist");
        assertTrue(util.indexExists(index3), "Third index should exist");
        
        // Delete all
        assertTrue(util.deleteIndex(index1), "First index should be deleted");
        assertTrue(util.deleteIndex(index2), "Second index should be deleted");
        assertTrue(util.deleteIndex(index3), "Third index should be deleted");
        
        // Verify all deleted
        assertFalse(util.indexExists(index1), "First index should not exist");
        assertFalse(util.indexExists(index2), "Second index should not exist");
        assertFalse(util.indexExists(index3), "Third index should not exist");
    }

    @Test
    void testListIndices() throws IOException {
        // Initially, there should be no indices (or only system indices)
        List<String> initialIndices = util.listIndices();
        assertNotNull(initialIndices, "List of indices should not be null");
        
        // Create multiple test indices
        String index1 = "test-list-index-1";
        String index2 = "test-list-index-2";
        String index3 = "test-list-index-3";
        
        assertTrue(util.createIndex(index1), "First index should be created");
        assertTrue(util.createIndex(index2), "Second index should be created");
        assertTrue(util.createIndex(index3), "Third index should be created");
        
        // List all indices
        List<String> allIndices = util.listIndices();
        assertNotNull(allIndices, "List of indices should not be null");
        
        // Verify our created indices are in the list
        assertTrue(allIndices.contains(index1), "List should contain first index");
        assertTrue(allIndices.contains(index2), "List should contain second index");
        assertTrue(allIndices.contains(index3), "List should contain third index");
        
        // Verify we have at least 3 user indices now
        long userIndicesAfterCreation = allIndices.stream()
                .filter(index -> !index.startsWith("."))
                .count();
        assertTrue(userIndicesAfterCreation >= 3, 
                "Should have at least 3 user indices after creation");
        
        // Clean up
        util.deleteIndex(index1);
        util.deleteIndex(index2);
        util.deleteIndex(index3);
        
        // Verify indices are removed from the list
        List<String> indicesAfterDeletion = util.listIndices();
        assertFalse(indicesAfterDeletion.contains(index1), 
                "First index should not be in list after deletion");
        assertFalse(indicesAfterDeletion.contains(index2), 
                "Second index should not be in list after deletion");
        assertFalse(indicesAfterDeletion.contains(index3), 
                "Third index should not be in list after deletion");
    }
}

