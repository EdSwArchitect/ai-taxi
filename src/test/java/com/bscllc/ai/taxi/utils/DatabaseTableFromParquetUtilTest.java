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
 * Test class for DatabaseTableFromParquetUtil using Testcontainers.
 * Creates an isolated PostgreSQL container for testing.
 */
@Testcontainers
@DisplayName("DatabaseTableFromParquetUtil Tests with Testcontainers")
class DatabaseTableFromParquetUtilTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025-01.parquet";
    private static final String TABLE_NAME = "green_tripdata_db_test";
    private static final String SCHEMA_NAME = "test_schema";
    
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
        
        System.out.println("\n=== PostgreSQL Test Container for DatabaseTableFromParquetUtil ===");
        System.out.println("JDBC URL: " + jdbcUrl);
        System.out.println("Username: " + username);
        System.out.println("Container ID: " + postgresContainer.getContainerId());
    }

    @Test
    @DisplayName("Should create table from Parquet schema with JDBC parameters")
    void testCreateTableWithJdbcParameters() throws Exception {
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

