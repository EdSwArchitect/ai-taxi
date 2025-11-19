package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Utility class for reading Parquet files.
 */
public class ParquetReaderEdwin {

    /**
     * Creates a Hadoop Configuration with settings to avoid security manager issues.
     */
    private static Configuration createConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.setBoolean("fs.file.impl.disable.cache", true);
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
        // Set system property to disable metadata serialization before opening file
        System.setProperty("parquet.metadata.serialization", "false");
        
        InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), conf);
        
        // Try to read schema using ParquetFileReader
        // This may throw RuntimeException due to Jackson serialization issues
        try {
            try (org.apache.parquet.hadoop.ParquetFileReader reader = 
                    org.apache.parquet.hadoop.ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = reader.getFooter();
                return metadata.getFileMetaData().getSchema();
            }
        } catch (RuntimeException e) {
            // If Jackson serialization error, fall back to reading schema from first record
            String msg = e.getMessage();
            Throwable cause = e.getCause();
            if ((msg != null && (msg.contains("No serializer found") || msg.contains("LogicalTypeAnnotation"))) ||
                (cause != null && cause.getMessage() != null && 
                 (cause.getMessage().contains("No serializer found") || cause.getMessage().contains("LogicalTypeAnnotation")))) {
                // Fall back to reading schema from first record using ParquetReader
                return getSchemaFromFirstRecord(filePath, conf);
            }
            // Re-throw if it's not a Jackson serialization error
            throw new IOException("Failed to read schema from parquet file: " + filePath, e);
        }
    }
    
    /**
     * Alternative method to get schema by reading the first record.
     * This avoids the Jackson serialization issue by not reading metadata directly.
     */
    private static MessageType getSchemaFromFirstRecord(String filePath, Configuration conf) throws IOException {
        Path path = new Path(filePath);
        try (org.apache.parquet.hadoop.ParquetReader<Group> reader = 
                org.apache.parquet.hadoop.ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            
            Group firstGroup = reader.read();
            if (firstGroup != null) {
                org.apache.parquet.schema.GroupType groupType = firstGroup.getType();
                if (groupType instanceof MessageType) {
                    return (MessageType) groupType;
                }
                // Convert GroupType to MessageType
                return new MessageType(groupType.getName(), groupType.getFields());
            }
            
            throw new IOException("Parquet file is empty or cannot be read: " + filePath);
        } catch (RuntimeException e) {
            // If still fails, wrap as IOException
            throw new IOException("Failed to read schema from first record: " + filePath, e);
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
        
        // Get schema first
        MessageType schema = getSchema(filePath);

        try (org.apache.parquet.hadoop.ParquetReader<Group> reader = 
                org.apache.parquet.hadoop.ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            
            Group group;
            int recordCount = 0;
            
            while ((group = reader.read()) != null) {
                if (maxRecords > 0 && recordCount >= maxRecords) {
                    break;
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
        Configuration conf = createConfiguration();
        InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), conf);
        
        try (org.apache.parquet.hadoop.ParquetFileReader reader = 
                org.apache.parquet.hadoop.ParquetFileReader.open(inputFile)) {
            ParquetMetadata metadata = reader.getFooter();
            String numRowsStr = metadata.getFileMetaData().getKeyValueMetaData()
                    .getOrDefault("num_rows", null);
            if (numRowsStr != null) {
                try {
                    return Long.parseLong(numRowsStr);
                } catch (NumberFormatException e) {
                    // Fall through to manual count
                }
            }
        }
        
        // If metadata doesn't have num_rows or parsing fails, count manually
        return readParquetFile(filePath).size();
    }
}
