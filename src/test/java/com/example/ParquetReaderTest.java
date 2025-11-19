package com.example;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test class for ParquetFileReaderUtil demonstrating how to read Parquet files.
 */
class ParquetReaderTest {

    private static final String PARQUET_FILE = "src/main/resources/fhvhv_tripdata_2025-01.parquet";

    @BeforeAll
    static void checkParquetFileExists() {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Warning: Parquet file not found at " + PARQUET_FILE);
            System.out.println("Some tests may be skipped.");
        }
    }

    @Test
    void testGetSchema() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testGetSchema - file not found");
            return;
        }

        MessageType schema = ParquetFileReaderUtil.getSchema(PARQUET_FILE);
        assertNotNull(schema, "Schema should not be null");
        assertFalse(schema.getFields().isEmpty(), "Schema should have fields");
        
        System.out.println("\n=== Parquet File Schema ===");
        for (org.apache.parquet.schema.Type field : schema.getFields()) {
            System.out.println(field.toString());
        }
    }

    @Test
    void testReadParquetFileFirstFewRecords() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testReadParquetFileFirstFewRecords - file not found");
            return;
        }

        // Read first 5 records
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 5);
        
        assertNotNull(records, "Records should not be null");
        assertFalse(records.isEmpty(), "Should have at least one record");
        assertTrue(records.size() <= 5, "Should not read more than 5 records");
        
        // Print first record
        if (!records.isEmpty()) {
            Map<String, Object> firstRecord = records.get(0);
            System.out.println("\n=== First Record ===");
            firstRecord.forEach((key, value) -> 
                System.out.println(key + ": " + value)
            );
            
            // Verify record has data
            assertFalse(firstRecord.isEmpty(), "First record should have data");
        }
    }

    @Test
    void testReadParquetFileStructure() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testReadParquetFileStructure - file not found");
            return;
        }

        // Read first record to understand structure
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 1);
        
        if (!records.isEmpty()) {
            Map<String, Object> record = records.get(0);
            
            System.out.println("\n=== Record Structure ===");
            System.out.println("Number of columns: " + record.size());
            System.out.println("\nColumn names:");
            record.keySet().forEach(System.out::println);
        }
    }

    @Test
    void testPrintSchema() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testPrintSchema - file not found");
            return;
        }

        System.out.println("\n=== Printing Schema ===");
        ParquetFileReaderUtil.printSchema(PARQUET_FILE);
    }

    @Test
    void testReadMultipleRecords() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testReadMultipleRecords - file not found");
            return;
        }

        // Read first 10 records
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 10);
        
        assertNotNull(records);
        assertTrue(records.size() <= 10, "Should not read more than 10 records");
        
        System.out.println("\n=== Reading 10 Records ===");
        System.out.println("Total records read: " + records.size());
        
        // Print summary of first 3 records
        for (int i = 0; i < Math.min(3, records.size()); i++) {
            System.out.println("\nRecord " + (i + 1) + ":");
            Map<String, Object> record = records.get(i);
            record.forEach((key, value) -> 
                System.out.println("  " + key + ": " + 
                    (value != null && value.toString().length() > 100 
                        ? value.toString().substring(0, 100) + "..." 
                        : value))
            );
        }
    }

    @Test
    void testParquetFileExists() {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (file.exists()) {
            System.out.println("\n=== Parquet File Info ===");
            System.out.println("File path: " + file.getAbsolutePath());
            System.out.println("File size: " + (file.length() / 1024 / 1024) + " MB");
            System.out.println("File exists: " + file.exists());
        } else {
            System.out.println("\nParquet file not found at: " + PARQUET_FILE);
            System.out.println("Please ensure the file exists for full test coverage.");
        }
    }
}

