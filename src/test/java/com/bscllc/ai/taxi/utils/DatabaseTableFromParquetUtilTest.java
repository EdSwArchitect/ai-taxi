package com.bscllc.ai.taxi.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Test class for DatabaseTableFromParquetUtil using Testcontainers.
 * Creates an isolated PostgreSQL container for testing.
 * Uses manual container lifecycle management to handle Docker connection issues gracefully.
 */
@DisplayName("DatabaseTableFromParquetUtil Tests with Testcontainers")
class DatabaseTableFromParquetUtilTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025-01.parquet";
    private static final String TABLE_NAME = "green_tripdata_db_test";
    private static final String SCHEMA_NAME = "test_schema";
    
    private static boolean dockerAvailable = false;
    
    // Configure Docker BEFORE container initialization
    static {
        // Try multiple approaches to connect to Docker
        // 1. Set Docker context (like docker-compose uses)
        String dockerContext = System.getProperty("DOCKER_CONTEXT");
        if (dockerContext == null || dockerContext.isEmpty()) {
            dockerContext = System.getenv("DOCKER_CONTEXT");
        }
        if (dockerContext == null || dockerContext.isEmpty()) {
            // Use the active Docker context (desktop-linux)
            dockerContext = "desktop-linux";
            System.setProperty("DOCKER_CONTEXT", dockerContext);
        }
        
        // 2. Set Docker host as fallback
        String dockerHost = System.getProperty("DOCKER_HOST");
        if (dockerHost == null || dockerHost.isEmpty()) {
            dockerHost = System.getenv("DOCKER_HOST");
        }
        if (dockerHost == null || dockerHost.isEmpty()) {
            // Default to the macOS Docker Desktop location
            dockerHost = "unix:///Users/edwinbrown/.docker/run/docker.sock";
            System.setProperty("DOCKER_HOST", dockerHost);
        }
        
        // 3. Also set the socket path directly for Testcontainers
        System.setProperty("docker.socket.override", "/Users/edwinbrown/.docker/run/docker.sock");
        
        // 4. Disable Ryuk to avoid connection issues
        System.setProperty("TESTCONTAINERS_RYUK_DISABLED", "true");
        System.setProperty("TESTCONTAINERS_CHECKS_DISABLE", "true");
        
        // 5. Enable verbose logging to debug connection issues
        System.setProperty("org.slf4j.simpleLogger.log.org.testcontainers", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.com.github.dockerjava", "debug");
        
        System.out.println("Docker configuration in static block:");
        System.out.println("  DOCKER_CONTEXT: " + System.getProperty("DOCKER_CONTEXT"));
        System.out.println("  DOCKER_HOST: " + System.getProperty("DOCKER_HOST"));
        System.out.println("  docker.socket.override: " + System.getProperty("docker.socket.override"));
    }
    
    // Manual container management instead of @Container
    private static PostgreSQLContainer<?> postgresContainer;

    private static String jdbcUrl;
    private static String username;
    private static String password;

    @BeforeAll
    static void setUpContainer() {
        // First, verify Docker socket is accessible by checking file
        java.io.File dockerSocket = new java.io.File("/Users/edwinbrown/.docker/run/docker.sock");
        if (!dockerSocket.exists()) {
            System.out.println("⚠ Docker socket file does not exist at: " + dockerSocket.getAbsolutePath());
            dockerAvailable = false;
            return;
        }
        if (!dockerSocket.canRead() || !dockerSocket.canWrite()) {
            System.out.println("⚠ Docker socket file exists but is not readable/writable");
            dockerAvailable = false;
            return;
        }
        System.out.println("✓ Docker socket file is accessible: " + dockerSocket.getAbsolutePath());
        
        // Now try to connect using Testcontainers
        // Use reflection to reset the DockerClientFactory if needed
        try {
            // Set properties before first access
            System.setProperty("docker.client.strategy", "org.testcontainers.dockerclient.UnixSocketClientProviderStrategy");
            System.setProperty("DOCKER_HOST", "unix:///Users/edwinbrown/.docker/run/docker.sock");
            
            // Try to get Docker client
            var dockerClient = DockerClientFactory.instance().client();
            
            // Test the connection by pinging Docker
            dockerClient.pingCmd().exec();
            
            dockerAvailable = true;
            System.out.println("✓ Docker is available and responsive");
        } catch (Exception e) {
            System.out.println("⚠ Docker is not available: " + e.getMessage());
            System.out.println("  Error details: " + e.getClass().getName());
            if (e.getCause() != null) {
                System.out.println("  Cause: " + e.getCause().getMessage());
            }
            // Print full stack trace for debugging
            System.out.println("  Stack trace:");
            e.printStackTrace();
            System.out.println("  Tests requiring Docker will be skipped.");
            dockerAvailable = false;
            return;
        }
        
        // Only create and start container if Docker is available
        if (dockerAvailable) {
            try {
                postgresContainer = new PostgreSQLContainer<>(
                    DockerImageName.parse("postgres:15"))
                    .withDatabaseName("testdb")
                    .withUsername("testuser")
                    .withPassword("testpass")
                    .withReuse(true);
                
                postgresContainer.start();
                
                jdbcUrl = postgresContainer.getJdbcUrl();
                username = postgresContainer.getUsername();
                password = postgresContainer.getPassword();
                
                System.out.println("\n=== PostgreSQL Test Container for DatabaseTableFromParquetUtil ===");
                System.out.println("JDBC URL: " + jdbcUrl);
                System.out.println("Username: " + username);
                System.out.println("Container ID: " + postgresContainer.getContainerId());
            } catch (Exception e) {
                System.out.println("⚠ Error starting container: " + e.getMessage());
                System.out.println("  Stack trace: " + java.util.Arrays.toString(e.getStackTrace()).substring(0, Math.min(200, java.util.Arrays.toString(e.getStackTrace()).length())));
                dockerAvailable = false;
            }
        }
    }
    
    @AfterAll
    static void tearDownContainer() {
        if (postgresContainer != null && postgresContainer.isRunning()) {
            postgresContainer.stop();
            System.out.println("✓ Container stopped");
        }
    }

    @Test
    @DisplayName("Should create table from Parquet schema with JDBC parameters")
    void testCreateTableWithJdbcParameters() throws Exception {
        if (!dockerAvailable) {
            System.out.println("Skipping test - Docker not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found at: " + PARQUET_FILE);
            return;
        }

        System.out.println("\n=== Test: Create Table with JDBC Parameters ===");
        
        DatabaseTableFromParquetUtil.createTable(
            jdbcUrl,
            SCHEMA_NAME,
            username,
            password,
            PARQUET_FILE,
            TABLE_NAME,
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
        if (!dockerAvailable) {
            System.out.println("Skipping test - Docker not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Create Schema If Not Exists ===");
        
        String testSchemaName = "new_schema_" + System.currentTimeMillis();
        String testTableName = "test_table";
        
        DatabaseTableFromParquetUtil.createTable(
            jdbcUrl,
            testSchemaName,
            username,
            password,
            PARQUET_FILE,
            testTableName,
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
        if (!dockerAvailable) {
            System.out.println("Skipping test - Docker not available");
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
        DatabaseTableFromParquetUtil.createTable(
            jdbcUrl,
            SCHEMA_NAME,
            username,
            password,
            PARQUET_FILE,
            testTableName,
            true  // drop if exists
        );

        // Verify table exists
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, SCHEMA_NAME, testTableName, null);
            assertTrue(tables.next(), "Table should exist after first creation");
        }

        // Try to create again without dropping - should skip
        DatabaseTableFromParquetUtil.createTable(
            jdbcUrl,
            SCHEMA_NAME,
            username,
            password,
            PARQUET_FILE,
            testTableName,
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
    @DisplayName("Should create table without dropIfExists parameter (safe mode)")
    void testCreateTableSafeMode() throws Exception {
        if (!dockerAvailable) {
            System.out.println("Skipping test - Docker not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Create Table in Safe Mode ===");
        
        String testTableName = TABLE_NAME + "_safe";
        
        // Create table using overloaded method without dropIfExists (defaults to false)
        DatabaseTableFromParquetUtil.createTable(
            jdbcUrl,
            SCHEMA_NAME,
            username,
            password,
            PARQUET_FILE,
            testTableName
        );

        // Verify table exists
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, SCHEMA_NAME, testTableName, null);
            assertTrue(tables.next(), "Table should exist after creation");
            System.out.println("✓ Table created in safe mode");
        }
    }

    @Test
    @DisplayName("Should verify table columns match Parquet schema")
    void testTableColumnsMatchSchema() throws Exception {
        if (!dockerAvailable) {
            System.out.println("Skipping test - Docker not available");
            return;
        }
        
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Verify Table Columns Match Schema ===");
        
        String testTableName = TABLE_NAME + "_columns";
        
        // Read schema first
        var schema = ParquetFileReaderUtil.getSchema(PARQUET_FILE);
        int expectedFields = schema.getFieldCount();
        System.out.println("Expected columns from schema: " + expectedFields);

        // Create table
        DatabaseTableFromParquetUtil.createTable(
            jdbcUrl,
            SCHEMA_NAME,
            username,
            password,
            PARQUET_FILE,
            testTableName,
            true
        );

        // Verify column count matches
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, SCHEMA_NAME, testTableName, null);
            
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }
            
            assertEquals(expectedFields, columnCount, 
                "Number of columns should match Parquet schema fields");
            System.out.println("✓ Verified " + columnCount + " columns match schema");
        }
    }

    @Test
    @DisplayName("Should print Parquet schema")
    void testPrintParquetSchema() throws Exception {
        // This test doesn't require Docker, so we can run it regardless
        File parquetFile = new File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Test: Print Parquet Schema ===");
        
        // Should not throw exception
        assertDoesNotThrow(() -> {
            DatabaseTableFromParquetUtil.printSchema(PARQUET_FILE);
        });
        
        System.out.println("✓ Schema printed successfully");
    }
}

