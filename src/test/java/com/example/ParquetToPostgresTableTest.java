package com.example;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class demonstrating how to use ParquetToPostgresTableUtil
 * to create PostgreSQL tables from Parquet file schemas.
 */
public class ParquetToPostgresTableTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025-01.parquet";
    private static final String TABLE_NAME = "green_tripdata_2025_01";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/ai_taxi";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "postgres";

    @BeforeAll
    static void checkParquetFileExists() {
        File file = new File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Warning: Parquet file not found at " + PARQUET_FILE);
            System.out.println("Some tests may be skipped.");
        }
    }

    @BeforeAll
    static void checkDatabaseConnection() {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("✓ Database connection successful");
        } catch (SQLException e) {
            System.out.println("⚠ Warning: Cannot connect to database: " + e.getMessage());
            System.out.println("Make sure PostgreSQL is running via docker-compose");
        }
    }

    @Test
    void testPrintParquetSchema() throws Exception {
        File file = new File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        System.out.println("\n=== Printing Parquet Schema ===");
        ParquetToPostgresTableUtil.printParquetSchema(PARQUET_FILE);
    }

    @Test
    void testCreateTableFromParquetFile() throws Exception {
        File file = new File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        // Test database connection first
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            // Connection successful, proceed with test
        } catch (SQLException e) {
            System.out.println("Skipping test - Cannot connect to database: " + e.getMessage());
            System.out.println("Make sure PostgreSQL is running: docker-compose -f docker-compose-ssl-2.yaml up -d postgres");
            return;
        }

        System.out.println("\n=== Creating PostgreSQL Table from Parquet Schema ===");
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            TABLE_NAME,
            true // drop if exists
        );

        // Verify table was created
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            var metaData = conn.getMetaData();
            var tables = metaData.getTables(null, "taxi", TABLE_NAME, null);
            boolean tableExists = tables.next();
            assertTrue(tableExists, "Table should exist after creation");
            System.out.println("✓ Table verified to exist in database");
        }
    }

    @Test
    void testCreateTableWithCustomSchema() throws Exception {
        File file = new File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping test - Parquet file not found");
            return;
        }

        // Test database connection first
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            // Connection successful, proceed with test
        } catch (SQLException e) {
            System.out.println("Skipping test - Cannot connect to database");
            return;
        }

        System.out.println("\n=== Creating Table with Custom Parameters ===");
        ParquetToPostgresTableUtil.createTableFromParquetFile(
            PARQUET_FILE,
            "taxi",              // schema name
            "green_tripdata_test", // table name
            DB_URL,
            DB_USER,
            DB_PASSWORD,
            true // drop if exists
        );

        System.out.println("✓ Table created successfully");
    }
}

