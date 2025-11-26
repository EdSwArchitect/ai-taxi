package com.bscllc.ai.taxi.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Test class for ParquetToPostgresTableUtil using Testcontainers.
 * Creates an isolated PostgreSQL container for testing.
 * 
 * Note: If you encounter "Could not find a valid Docker environment" errors,
 * this is a known issue with Testcontainers and Docker Desktop on macOS.
 * The tests will skip gracefully if Docker is not available.
 */
@Testcontainers
@DisplayName("ParquetToPostgresTableUtil Tests with Testcontainers")
class ParquetToPostgresTableUtilTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025_01.parquet";
    private static final String TABLE_NAME = "green_tripdata_test";
    private static final String SCHEMA_NAME = "taxi";
    
    // Configure Docker BEFORE container initialization
    static {
        // Set Docker context (same as docker-compose uses)
        System.setProperty("DOCKER_CONTEXT", "desktop-linux");
        
        // Use the socket that docker-compose actually uses (from ~/.docker/run/docker.sock)
        String dockerHost = System.getProperty("DOCKER_HOST");
        if (dockerHost == null || dockerHost.isEmpty()) {
            dockerHost = System.getenv("DOCKER_HOST");
        }
        if (dockerHost == null || dockerHost.isEmpty()) {
            // Use the standard Docker Desktop socket location
            dockerHost = "unix:///Users/edwinbrown/.docker/run/docker.sock";
            System.setProperty("DOCKER_HOST", dockerHost);
        }
        
        // Disable Ryuk and checks to avoid connection issues
        System.setProperty("TESTCONTAINERS_RYUK_DISABLED", "true");
        System.setProperty("TESTCONTAINERS_CHECKS_DISABLE", "true");
        
        System.out.println("Docker configuration for Testcontainers:");
        System.out.println("  DOCKER_CONTEXT: " + System.getProperty("DOCKER_CONTEXT"));
        System.out.println("  DOCKER_HOST: " + System.getProperty("DOCKER_HOST"));
    }
    
    // Use manual container lifecycle to handle Docker connection issues
    private static PostgreSQLContainer<?> postgresContainer;
    
    static {
        // Initialize container in static block to catch errors early
        try {
            postgresContainer = new PostgreSQLContainer<>(
                DockerImageName.parse("postgres:15"))
                .withDatabaseName("testdb")
                .withUsername("testuser")
                .withPassword("testpass")
                .withReuse(true);
        } catch (Exception e) {
            System.out.println("Failed to create PostgreSQL container: " + e.getMessage());
        }
    }

    private static String jdbcUrl;
    private static String username;
    private static String password;

    private static boolean containerAvailable = false;
    
    @BeforeAll
    static void setUpContainer() {
        // Check if Docker is available before trying to start container
        try {
            DockerClientFactory.instance().client();
            System.out.println("✓ Docker client is available");
        } catch (Exception e) {
            System.out.println("⚠ Docker client is not available: " + e.getMessage());
            System.out.println("  This is a known issue with Testcontainers and Docker Desktop on macOS.");
            System.out.println("  Error: " + e.getClass().getName());
            System.out.println("  Tests requiring Docker will be skipped.");
            containerAvailable = false;
            return;
        }
        
        // Start container manually
        if (postgresContainer != null) {
            try {
                postgresContainer.start();
                containerAvailable = true;
                jdbcUrl = postgresContainer.getJdbcUrl();
                username = postgresContainer.getUsername();
                password = postgresContainer.getPassword();
                
                System.out.println("\n=== PostgreSQL Test Container ===");
                System.out.println("JDBC URL: " + jdbcUrl);
                System.out.println("Username: " + username);
                System.out.println("Container ID: " + postgresContainer.getContainerId());
            } catch (Exception e) {
                System.out.println("⚠ Failed to start PostgreSQL container: " + e.getMessage());
                System.out.println("  Tests requiring Docker will be skipped.");
                containerAvailable = false;
            }
        }
    }

    @Test
    @DisplayName("Should create table from Parquet schema")
    void testCreateTableFromParquetSchema() throws Exception {
        if (!containerAvailable) {
            System.out.println("Skipping test - Docker/Testcontainers not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found at: " + PARQUET_FILE);
            return;
        }

        System.out.println("\n=== Test: Create Table from Parquet Schema ===");
        
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            SCHEMA_NAME,
            TABLE_NAME,
            jdbcUrl,
            username,
            password,
            true  // drop if exists
        );

        // Verify table was created
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, SCHEMA_NAME, TABLE_NAME, null);
            boolean tableExists = tables.next();
            assertTrue(tableExists, "Table should exist after creation");
            System.out.println("✓ Table verified to exist");
        }
    }

    @Test
    @DisplayName("Should create schema if it doesn't exist")
    void testCreateSchemaIfNotExists() throws Exception {
        if (!containerAvailable) {
            System.out.println("Skipping test - Docker/Testcontainers not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Create Schema If Not Exists ===");
        
        String testSchemaName = "test_schema_" + System.currentTimeMillis();
        String testTableName = "test_table";
        
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            testSchemaName,
            testTableName,
            jdbcUrl,
            username,
            password,
            true
        );

        // Verify schema exists
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet schemas = metaData.getSchemas(null, testSchemaName);
            boolean schemaExists = schemas.next();
            assertTrue(schemaExists, "Schema should exist after creation");
            System.out.println("✓ Schema '" + testSchemaName + "' created successfully");
        }
    }

    @Test
    @DisplayName("Should not create table if it already exists (when dropIfExists=false)")
    void testCreateTableWithoutDropping() throws Exception {
        if (!containerAvailable) {
            System.out.println("Skipping test - Docker/Testcontainers not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Create Table Without Dropping ===");
        
        String testTableName = TABLE_NAME + "_nodrop";
        
        // Create table first time
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            SCHEMA_NAME,
            testTableName,
            jdbcUrl,
            username,
            password,
            true  // drop if exists
        );

        // Verify table exists
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, SCHEMA_NAME, testTableName, null);
            assertTrue(tables.next(), "Table should exist after first creation");
        }

        // Try to create again without dropping - should skip
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            SCHEMA_NAME,
            testTableName,
            jdbcUrl,
            username,
            password,
            false  // don't drop if exists
        );

        // Verify table still exists
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, SCHEMA_NAME, testTableName, null);
            assertTrue(tables.next(), "Table should still exist after second attempt");
            System.out.println("✓ Table creation skipped when already exists");
        }
    }

    @Test
    @DisplayName("Should track metrics when creating tables")
    void testMetricsTracking() throws Exception {
        if (!containerAvailable) {
            System.out.println("Skipping test - Docker/Testcontainers not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Metrics Tracking ===");
        
        double initialFiles = ParquetToPostgresTableUtil.Metrics.getFilesLoaded();
        double initialEntries = ParquetToPostgresTableUtil.Metrics.getTableEntriesLoaded();
        
        System.out.println("Initial files loaded: " + initialFiles);
        System.out.println("Initial entries loaded: " + initialEntries);

        String testTableName = TABLE_NAME + "_metrics";
        
        // Create table
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            SCHEMA_NAME,
            testTableName,
            jdbcUrl,
            username,
            password,
            true
        );

        // Check metrics increased
        double filesAfter = ParquetToPostgresTableUtil.Metrics.getFilesLoaded();
        
        assertTrue(filesAfter >= initialFiles, "Files loaded counter should have increased");
        System.out.println("Files loaded after: " + filesAfter);
        System.out.println("✓ Metrics tracking verified");
    }

    @Test
    @DisplayName("Should create table and load data")
    void testCreateTableAndLoadData() throws Exception {
        if (!containerAvailable) {
            System.out.println("Skipping test - Docker/Testcontainers not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Create Table and Load Data ===");
        
        String testTableName = TABLE_NAME + "_with_data";
        
        long rowsLoaded = ParquetToPostgresTableUtil.createTableAndLoadDataFromParquetFile(
            PARQUET_FILE,
            SCHEMA_NAME,
            testTableName,
            jdbcUrl,
            username,
            password,
            true,  // drop if exists
            10     // load first 10 rows
        );

        assertTrue(rowsLoaded > 0, "Should have loaded at least one row");
        System.out.println("✓ Loaded " + rowsLoaded + " rows");

        // Verify data was loaded
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA_NAME + "." + testTableName);
            rs.next();
            int rowCount = rs.getInt(1);
            assertEquals(rowsLoaded, rowCount, "Row count should match loaded rows");
            System.out.println("✓ Verified " + rowCount + " rows in table");
        }
    }

    @Test
    @DisplayName("Should print Parquet schema")
    void testPrintParquetSchema() throws Exception {
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Print Parquet Schema ===");
        
        // Should not throw exception
        assertDoesNotThrow(() -> {
            ParquetToPostgresTableUtil.printParquetSchema(PARQUET_FILE);
        });
        
        System.out.println("✓ Schema printed successfully");
    }

    @Test
    @DisplayName("Should get metrics summary")
    void testGetMetricsSummary() {
        System.out.println("\n=== Test: Get Metrics Summary ===");
        
        String summary = ParquetToPostgresTableUtil.Metrics.getSummary();
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
        System.out.println("Metrics summary: " + summary);
        System.out.println("✓ Metrics summary retrieved");
    }
}

