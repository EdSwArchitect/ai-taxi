package com.example;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test class for reading fhv_tripdata_2025-01.parquet file.
 */
class FhvTripDataTest {

    private static final String PARQUET_FILE = "src/main/resources/fhv_tripdata_2025-01.parquet";

    @BeforeAll
    static void checkParquetFileExists() {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Warning: Parquet file not found at " + PARQUET_FILE);
            System.out.println("Some tests may be skipped.");
        }
    }

    @Test
    void testFhvTripDataFileExists() {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (file.exists()) {
            System.out.println("\n=== FHV Trip Data File Info ===");
            System.out.println("File path: " + file.getAbsolutePath());
            System.out.println("File size: " + (file.length() / 1024 / 1024) + " MB");
            System.out.println("File exists: " + file.exists());
            assertTrue(file.exists(), "FHV trip data file should exist");
        } else {
            System.out.println("\nFHV trip data file not found at: " + PARQUET_FILE);
            fail("FHV trip data file does not exist");
        }
    }

    @Test
    void testGetFhvTripDataSchema() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testGetFhvTripDataSchema - file not found");
            return;
        }

        MessageType schema = ParquetFileReaderUtil.getSchema(PARQUET_FILE);
        assertNotNull(schema, "Schema should not be null");
        assertFalse(schema.getFields().isEmpty(), "Schema should have fields");
        
        System.out.println("\n=== FHV Trip Data Schema ===");
        System.out.println("Number of fields: " + schema.getFields().size());
        for (org.apache.parquet.schema.Type field : schema.getFields()) {
            System.out.println(field.toString());
        }
    }

    @Test
    void testReadFhvTripDataRecords() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testReadFhvTripDataRecords - file not found");
            return;
        }

        // Read first 10 records
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 10);
        
        assertNotNull(records, "Records should not be null");
        assertFalse(records.isEmpty(), "Should have at least one record");
        assertTrue(records.size() <= 10, "Should not read more than 10 records");
        
        System.out.println("\n=== FHV Trip Data Records ===");
        System.out.println("Total records read: " + records.size());
        
        // Print first record details
        if (!records.isEmpty()) {
            Map<String, Object> firstRecord = records.get(0);
            System.out.println("\nFirst Record:");
            System.out.println("Number of columns: " + firstRecord.size());
            System.out.println("\nColumn names:");
            firstRecord.keySet().forEach(System.out::println);
            
            System.out.println("\nFirst Record Data:");
            firstRecord.forEach((key, value) -> 
                System.out.println("  " + key + ": " + 
                    (value != null && value.toString().length() > 100 
                        ? value.toString().substring(0, 100) + "..." 
                        : value))
            );
            
            // Verify record has data
            assertFalse(firstRecord.isEmpty(), "First record should have data");
        }
    }

    @Test
    void testReadFhvTripDataFirstRecord() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testReadFhvTripDataFirstRecord - file not found");
            return;
        }

        // Read first record only
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 1);
        
        assertNotNull(records, "Records should not be null");
        assertEquals(1, records.size(), "Should read exactly one record");
        
        Map<String, Object> record = records.get(0);
        assertNotNull(record, "Record should not be null");
        assertFalse(record.isEmpty(), "Record should have data");
        
        System.out.println("\n=== First FHV Trip Data Record ===");
        record.forEach((key, value) -> 
            System.out.println(key + ": " + value)
        );
    }

    @Test
    void testFhvTripDataColumnNames() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testFhvTripDataColumnNames - file not found");
            return;
        }

        // Read first record to get column names
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 1);
        
        if (!records.isEmpty()) {
            Map<String, Object> record = records.get(0);
            
            System.out.println("\n=== FHV Trip Data Column Names ===");
            System.out.println("Total columns: " + record.size());
            System.out.println("\nColumns:");
            record.keySet().stream().sorted().forEach(column -> 
                System.out.println("  - " + column)
            );
            
            // Verify we have columns
            assertFalse(record.isEmpty(), "Should have at least one column");
        }
    }

    @Test
    void testPrintFhvTripDataSchema() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testPrintFhvTripDataSchema - file not found");
            return;
        }

        System.out.println("\n=== Printing FHV Trip Data Schema ===");
        ParquetFileReaderUtil.printSchema(PARQUET_FILE);
    }

    @Test
    void testReadFhvTripDataMultipleRecords() throws IOException {
        java.io.File file = new java.io.File(PARQUET_FILE);
        if (!file.exists()) {
            System.out.println("Skipping testReadFhvTripDataMultipleRecords - file not found");
            return;
        }

        // Read first 20 records
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(PARQUET_FILE, 20);
        
        assertNotNull(records);
        assertTrue(records.size() <= 20, "Should not read more than 20 records");
        assertTrue(records.size() > 0, "Should read at least one record");
        
        System.out.println("\n=== Reading Multiple FHV Trip Data Records ===");
        System.out.println("Total records read: " + records.size());
        
        // Print summary of first 5 records
        int recordsToPrint = Math.min(5, records.size());
        for (int i = 0; i < recordsToPrint; i++) {
            System.out.println("\nRecord " + (i + 1) + " (showing first 5 fields):");
            Map<String, Object> record = records.get(i);
            record.entrySet().stream()
                .limit(5)
                .forEach(entry -> 
                    System.out.println("  " + entry.getKey() + ": " + 
                        (entry.getValue() != null && entry.getValue().toString().length() > 80 
                            ? entry.getValue().toString().substring(0, 80) + "..." 
                            : entry.getValue()))
                );
        }
    }
}


