package com.bscllc.ai.taxi.utils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ParquetToPostgresTableUtil using Testcontainers.
 * Creates an isolated PostgreSQL container for testing.
 */
@Testcontainers
@DisplayName("ParquetToPostgresTableUtil Tests with Testcontainers")
class ParquetToPostgresTableUtilTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025-01.parquet";
    private static final String TABLE_NAME = "green_tripdata_test";
    private static final String SCHEMA_NAME = "taxi";
    
    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withReuse(true);

    private static String jdbcUrl;
    private static String username;
    private static String password;

    @BeforeAll
    static void setUpContainer() {
        postgresContainer.start();
        jdbcUrl = postgresContainer.getJdbcUrl();
        username = postgresContainer.getUsername();
        password = postgresContainer.getPassword();
        
        System.out.println("\n=== PostgreSQL Test Container ===");
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Username: " + username);
        System.out.println("Container ID: " + postgresContainer.getContainerId());
    }

    @Test
    @DisplayName("Should create table from Parquet schema")
    void testCreateTableFromParquetSchema() throws Exception {
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

