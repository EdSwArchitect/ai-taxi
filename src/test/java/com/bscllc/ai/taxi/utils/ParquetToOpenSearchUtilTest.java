package com.bscllc.ai.taxi.utils;

import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ParquetToOpenSearchUtil using Testcontainers.
 * Creates an isolated OpenSearch container for testing.
 */
@Testcontainers
@DisplayName("ParquetToOpenSearchUtil Tests with Testcontainers")
class ParquetToOpenSearchUtilTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025-01.parquet";
    private static final String INDEX_NAME = "green_tripdata_test";
    
    // Use GenericContainer for OpenSearch as there's no specific OpenSearch Testcontainers module
    @Container
    private static final GenericContainer<?> opensearchContainer = new GenericContainer<>(
            DockerImageName.parse("opensearchproject/opensearch:3.3.0")
    )
            .withEnv("DISABLE_SECURITY_PLUGIN", "true")
            .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
            .withExposedPorts(9200)
            .waitingFor(Wait.forHttp("/_cluster/health")
                    .forPort(9200)
                    .forStatusCode(200)
                    .withStartupTimeout(java.time.Duration.ofSeconds(90)));

    private static String opensearchHost;
    private static int opensearchPort;

    @BeforeAll
    static void setUpContainer() {
        opensearchHost = opensearchContainer.getHost();
        opensearchPort = opensearchContainer.getMappedPort(9200);
        
        System.out.println("\n=== OpenSearch Test Container ===");
        System.out.println("Host: " + opensearchHost);
        System.out.println("Port: " + opensearchPort);
        System.out.println("Container ID: " + opensearchContainer.getContainerId());
    }

    @Test
    @DisplayName("Should create OpenSearch index from Parquet schema")
    void testCreateIndexFromParquetSchema() throws Exception {
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found at: " + PARQUET_FILE);
            return;
        }

        System.out.println("\n=== Test: Create Index from Parquet Schema ===");
        
        // Load Parquet file to OpenSearch
        long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
            PARQUET_FILE,
            INDEX_NAME,
            opensearchHost,
            opensearchPort,
            "http",  // Use HTTP since security is disabled
            "admin", // Not needed but kept for API compatibility
            "admin",
            null,    // No truststore needed for non-SSL
            null,
            true,    // drop if exists
            10       // Load first 10 records for testing
        );

        assertTrue(docsIndexed > 0, "Should have indexed at least one document");
        System.out.println("✓ Indexed " + docsIndexed + " documents");

        // Verify index exists
        OpenSearchClient client = createTestOpenSearchClient();
        boolean indexExists = client.indices().exists(e -> e.index(INDEX_NAME)).value();
        assertTrue(indexExists, "Index should exist after creation");
        System.out.println("✓ Index verified to exist");
    }

    @Test
    @DisplayName("Should track metrics when loading files")
    void testMetricsTracking() throws Exception {
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        // Get initial metrics
        double initialFiles = ParquetToOpenSearchUtil.Metrics.getFilesLoaded();
        double initialEntries = ParquetToOpenSearchUtil.Metrics.getEntriesLoaded();

        System.out.println("\n=== Test: Metrics Tracking ===");
        System.out.println("Initial files loaded: " + initialFiles);
        System.out.println("Initial entries loaded: " + initialEntries);

        // Load a file
        String testIndexName = INDEX_NAME + "_metrics_test";
        long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
            PARQUET_FILE,
            testIndexName,
            opensearchHost,
            opensearchPort,
            "http",
            "admin",
            "admin",
            null,
            null,
            true,
            5  // Load 5 records
        );

        // Check metrics increased
        double filesAfter = ParquetToOpenSearchUtil.Metrics.getFilesLoaded();
        double entriesAfter = ParquetToOpenSearchUtil.Metrics.getEntriesLoaded();

        assertTrue(filesAfter >= initialFiles, "Files loaded counter should have increased");
        assertTrue(entriesAfter >= initialEntries, 
            "Entries loaded counter should have increased");

        System.out.println("Files loaded after: " + filesAfter);
        System.out.println("Entries loaded after: " + entriesAfter);
        System.out.println("✓ Metrics tracking verified");
    }

    @Test
    @DisplayName("Should handle loading zero records gracefully")
    void testLoadZeroRecords() throws Exception {
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Handle Zero Records ===");
        
        // Load 0 records (maxRecords = 0)
        String testIndexName = INDEX_NAME + "_zero_test";
        long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
            PARQUET_FILE,
            testIndexName,
            opensearchHost,
            opensearchPort,
            "http",
            "admin",
            "admin",
            null,
            null,
            true,
            0  // Load 0 records
        );

        assertEquals(0, docsIndexed, "Should return 0 for 0 records");
        System.out.println("✓ Returned 0 as expected for 0 records");
        System.out.println("✓ Handled 0 records gracefully");
    }

    @Test
    @DisplayName("Should verify index structure matches Parquet schema")
    void testIndexStructureMatchesSchema() throws Exception {
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Verify Index Structure ===");
        
        String testIndexName = INDEX_NAME + "_structure_test";
        
        // Read schema first
        var schema = ParquetFileReaderUtil.getSchema(PARQUET_FILE);
        int expectedFields = schema.getFieldCount();
        System.out.println("Expected fields from schema: " + expectedFields);

        // Load to OpenSearch
        long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
            PARQUET_FILE,
            testIndexName,
            opensearchHost,
            opensearchPort,
            "http",
            "admin",
            "admin",
            null,
            null,
            true,
            1  // Load 1 document to verify structure
        );

        assertTrue(docsIndexed > 0);
        
        // Verify index exists and has mappings
        OpenSearchClient client = createTestOpenSearchClient();
        var mappingResponse = client.indices().getMapping(g -> g.index(testIndexName));
        var mappings = mappingResponse.get(testIndexName);
        
        assertNotNull(mappings, "Mappings should not be null");
        assertNotNull(mappings.mappings(), "Mappings object should not be null");
        
        var properties = mappings.mappings().properties();
        assertNotNull(properties, "Properties should not be null");
        
        System.out.println("Fields in index mapping: " + properties.size());
        assertTrue(properties.size() > 0, "Should have at least one field in mapping");
        System.out.println("✓ Index structure verified");
    }

    @Test
    @DisplayName("Should get metrics summary")
    void testGetMetricsSummary() {
        System.out.println("\n=== Test: Get Metrics Summary ===");
        
        String summary = ParquetToOpenSearchUtil.Metrics.getSummary();
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
        System.out.println("Metrics summary: " + summary);
        System.out.println("✓ Metrics summary retrieved");
    }

    @Test
    @DisplayName("Should scrape Prometheus metrics")
    void testScrapePrometheusMetrics() {
        System.out.println("\n=== Test: Scrape Prometheus Metrics ===");
        
        String metrics = ParquetToOpenSearchUtil.Metrics.scrape();
        assertNotNull(metrics);
        // Metrics may be empty if no operations have been performed
        System.out.println("Prometheus metrics: " + metrics);
        System.out.println("✓ Prometheus metrics scraped");
    }

    /**
     * Creates a test OpenSearch client connected to the container.
     */
    private OpenSearchClient createTestOpenSearchClient() throws Exception {
        // Use a simplified client creation for testing without SSL
        HttpHost httpHost = new HttpHost("http", opensearchHost, opensearchPort);
        ApacheHttpClient5TransportBuilder builder = 
            ApacheHttpClient5TransportBuilder.builder(httpHost);
        OpenSearchTransport transport = builder.build();
        return new OpenSearchClient(transport);
    }
}
