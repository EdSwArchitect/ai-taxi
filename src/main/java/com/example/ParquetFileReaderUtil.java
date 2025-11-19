package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for reading Parquet files.
 */
public class ParquetFileReaderUtil {

    /**
     * Creates a Hadoop Configuration with settings to avoid security manager issues.
     */
    private static Configuration createConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.setBoolean("fs.file.impl.disable.cache", true);
        // Disable Parquet metadata JSON serialization to avoid Jackson issues
        System.setProperty("parquet.metadata.serialization", "false");
        return conf;
    }

    /**
     * Reads a Parquet file and returns the schema information.
     *
     * @param filePath Path to the Parquet file
     * @return MessageType representing the schema
     * @throws IOException if the file cannot be read
     */
    public static MessageType getSchema(String filePath) throws IOException {
        Configuration conf = createConfiguration();
        Path path = new Path(filePath);
        
        // Read schema from the first record
        org.apache.parquet.hadoop.ParquetReader<Group> reader = null;
        RuntimeException lastException = null;
        
        // Try to open the file - may fail due to Jackson serialization issues in Parquet metadata
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                reader = org.apache.parquet.hadoop.ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(conf)
                        .build();
                break; // Success, exit loop
            } catch (RuntimeException e) {
                lastException = e;
                // Check if it's the Jackson serialization error
                String msg = e.getMessage();
                Throwable cause = e.getCause();
                if ((msg != null && (msg.contains("No serializer found") || msg.contains("LogicalTypeAnnotation"))) ||
                    (cause != null && cause.getMessage() != null && 
                     (cause.getMessage().contains("No serializer found") || cause.getMessage().contains("LogicalTypeAnnotation")))) {
                    // This is a known non-fatal error - try once more with fresh config
                    if (attempt == 0) {
                        conf = createConfiguration();
                        continue;
                    }
                }
                // If not the Jackson error or second attempt failed, throw
                throw new IOException("Failed to open Parquet file: " + filePath, e);
            }
        }
        
        if (reader == null) {
            throw new IOException("Failed to open Parquet file after retries: " + filePath, 
                lastException != null ? lastException : new RuntimeException("Unknown error"));
        }
        
        final org.apache.parquet.hadoop.ParquetReader<Group> finalReader = reader;
        try (finalReader) {
            
            Group firstGroup;
            try {
                firstGroup = finalReader.read();
            } catch (RuntimeException e) {
                // Catch RuntimeException that may occur when reading first group
                // This can happen due to Jackson serialization issues when reading metadata
                String msg = e.getMessage();
                Throwable cause = e.getCause();
                if ((msg != null && (msg.contains("No serializer found") || msg.contains("LogicalTypeAnnotation"))) ||
                    (cause != null && cause.getMessage() != null && 
                     (cause.getMessage().contains("No serializer found") || cause.getMessage().contains("LogicalTypeAnnotation")))) {
                    // This is a known issue - try to get schema from reader's internal state if possible
                    // For now, wrap as IOException
                    throw new IOException("Failed to read schema due to Jackson serialization issues. " +
                        "The parquet file appears to have a schema but cannot be read due to a library limitation: " + filePath, e);
                }
                // Re-throw if not a Jackson serialization error
                throw e;
            }
            
            if (firstGroup != null) {
                org.apache.parquet.schema.GroupType groupType = firstGroup.getType();
                if (groupType instanceof MessageType) {
                    return (MessageType) groupType;
                }
                // Convert GroupType to MessageType
                return new MessageType(groupType.getName(), groupType.getFields());
            }
            
            // If no records, throw an exception
            throw new IOException("Parquet file is empty or cannot be read: " + filePath);
        }
    }

    /**
     * Reads all records from a Parquet file and returns them as a list of maps.
     *
     * @param filePath Path to the Parquet file
     * @param maxRecords Maximum number of records to read (use -1 for all)
     * @return List of maps, where each map represents a row with column names as keys
     * @throws IOException if the file cannot be read
     */
    public static List<Map<String, Object>> readParquetFile(String filePath, int maxRecords) throws IOException {
        Configuration conf = createConfiguration();
        Path path = new Path(filePath);
        List<Map<String, Object>> records = new ArrayList<>();
        MessageType schema = null;

        org.apache.parquet.hadoop.ParquetReader<Group> reader = null;
        RuntimeException lastException = null;
        
        // Try to open the file - may fail due to Jackson serialization issues in Parquet metadata
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                reader = org.apache.parquet.hadoop.ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(conf)
                        .build();
                break; // Success, exit loop
            } catch (RuntimeException e) {
                lastException = e;
                // Check if it's the Jackson serialization error
                String msg = e.getMessage();
                Throwable cause = e.getCause();
                if ((msg != null && (msg.contains("No serializer found") || msg.contains("LogicalTypeAnnotation"))) ||
                    (cause != null && cause.getMessage() != null && 
                     (cause.getMessage().contains("No serializer found") || cause.getMessage().contains("LogicalTypeAnnotation")))) {
                    // This is a known non-fatal error - try once more with fresh config
                    if (attempt == 0) {
                        conf = createConfiguration();
                        continue;
                    }
                }
                // If not the Jackson error or second attempt failed, throw
                throw new IOException("Failed to open Parquet file: " + filePath, e);
            }
        }
        
        if (reader == null) {
            throw new IOException("Failed to open Parquet file after retries: " + filePath, 
                lastException != null ? lastException : new RuntimeException("Unknown error"));
        }

        final org.apache.parquet.hadoop.ParquetReader<Group> finalReader = reader;
        try (finalReader) {
            
            Group group;
            int recordCount = 0;
            
            while ((group = finalReader.read()) != null) {
                if (maxRecords > 0 && recordCount >= maxRecords) {
                    break;
                }
                
                // Get schema from first group if not already set
                if (schema == null) {
                    org.apache.parquet.schema.GroupType groupType = group.getType();
                    if (groupType instanceof MessageType) {
                        schema = (MessageType) groupType;
                    } else {
                        // Convert GroupType to MessageType
                        schema = new MessageType(groupType.getName(), groupType.getFields());
                    }
                }
                
                Map<String, Object> record = new HashMap<>();
                
                // Extract all fields from the group
                for (Type field : schema.getFields()) {
                    String fieldName = field.getName();
                    Object value = extractValue(group, field, 0);
                    record.put(fieldName, value);
                }
                
                records.add(record);
                recordCount++;
            }
        }

        return records;
    }

    /**
     * Reads all records from a Parquet file.
     *
     * @param filePath Path to the Parquet file
     * @return List of maps, where each map represents a row with column names as keys
     * @throws IOException if the file cannot be read
     */
    public static List<Map<String, Object>> readParquetFile(String filePath) throws IOException {
        return readParquetFile(filePath, -1);
    }

    /**
     * Extracts a value from a Group based on the field type.
     */
    private static Object extractValue(Group group, Type field, int index) {
        if (!group.getType().containsField(field.getName())) {
            return null;
        }

        int fieldIndex = group.getType().getFieldIndex(field.getName());
        int repetitionCount = group.getFieldRepetitionCount(fieldIndex);

        if (repetitionCount == 0) {
            return null;
        }

        // Handle primitive types
        if (field.isPrimitive()) {
            org.apache.parquet.schema.PrimitiveType primitiveType = field.asPrimitiveType();
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();

            try {
                switch (typeName) {
                    case BINARY:
                        return group.getBinary(fieldIndex, index).toStringUsingUTF8();
                    case INT32:
                        return group.getInteger(fieldIndex, index);
                    case INT64:
                        return group.getLong(fieldIndex, index);
                    case FLOAT:
                        return group.getFloat(fieldIndex, index);
                    case DOUBLE:
                        return group.getDouble(fieldIndex, index);
                    case BOOLEAN:
                        return group.getBoolean(fieldIndex, index);
                    default:
                        return group.getValueToString(fieldIndex, index);
                }
            } catch (RuntimeException e) {
                // If extraction fails, return null or string representation
                return group.getValueToString(fieldIndex, index);
            }
        }

        return group.getValueToString(fieldIndex, index);
    }

    /**
     * Prints the schema of a Parquet file to the console.
     *
     * @param filePath Path to the Parquet file
     * @throws IOException if the file cannot be read
     */
    public static void printSchema(String filePath) throws IOException {
        MessageType schema = getSchema(filePath);
        System.out.println("Parquet File Schema:");
        System.out.println("====================");
        for (Type field : schema.getFields()) {
            System.out.println(field.toString());
        }
    }

    /**
     * Gets the total number of records in a Parquet file.
     * Note: This method counts records by reading them, which may be slow for large files.
     *
     * @param filePath Path to the Parquet file
     * @return Total number of records
     * @throws IOException if the file cannot be read
     */
    public static long getRecordCount(String filePath) throws IOException {
        // Count records by reading them (may be slow for large files)
        // Alternative: could use ParquetFileReader to get metadata, but that has serialization issues
        return readParquetFile(filePath).size();
    }
}
