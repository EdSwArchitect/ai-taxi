package com.example;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to read Parquet file schema and create PostgreSQL table based on it.
 * Creates tables in the "taxi" schema, creating the schema if necessary.
 */
public class ParquetToPostgresTableUtil {

    private static final String DEFAULT_SCHEMA_NAME = "taxi";
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/ai_taxi";
    private static final String DEFAULT_USER = "postgres";
    private static final String DEFAULT_PASSWORD = "postgres";

    /**
     * Converts Parquet type to PostgreSQL data type.
     *
     * @param parquetType The Parquet type
     * @return PostgreSQL data type string
     */
    private static String convertParquetTypeToPostgresType(Type parquetType) {
        if (!parquetType.isPrimitive()) {
            // For nested types, use JSONB for flexibility
            return "JSONB";
        }

        PrimitiveType primitiveType = parquetType.asPrimitiveType();
        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();

        switch (primitiveTypeName) {
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                // Check if it's likely a string type
                String originalType = primitiveType.getOriginalType() != null 
                    ? primitiveType.getOriginalType().toString() 
                    : "";
                if (originalType.contains("UTF8") || originalType.contains("STRING")) {
                    return "TEXT";
                }
                // For other binary types, use TEXT with appropriate length if available
                return "TEXT";
            case INT96:
                // INT96 is often used for timestamps
                return "TIMESTAMP";
            default:
                // Default to TEXT for unknown types
                return "TEXT";
        }
    }

    /**
     * Sanitizes a field name for use as a PostgreSQL column name.
     * PostgreSQL identifiers are case-insensitive unless quoted, so we'll use lowercase.
     *
     * @param fieldName The field name to sanitize
     * @return Sanitized column name
     */
    private static String sanitizeColumnName(String fieldName) {
        // Convert to lowercase and replace invalid characters with underscore
        return fieldName.toLowerCase()
            .replaceAll("[^a-z0-9_]", "_")
            .replaceAll("^_+|_+$", ""); // Remove leading/trailing underscores
    }

    /**
     * Creates the database schema if it doesn't exist.
     *
     * @param connection The database connection
     * @param schemaName The schema name to create
     * @throws SQLException if schema creation fails
     */
    private static void createSchemaIfNotExists(Connection connection, String schemaName) throws SQLException {
        String sql = String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("Schema '" + schemaName + "' created or already exists");
        }
    }

    /**
     * Creates a PostgreSQL table based on Parquet schema.
     *
     * @param connection The database connection
     * @param schemaName The PostgreSQL schema name
     * @param tableName The table name to create
     * @param parquetSchema The Parquet schema
     * @param dropIfExists Whether to drop the table if it already exists
     * @throws SQLException if table creation fails
     */
    public static void createTableFromParquetSchema(
            Connection connection,
            String schemaName,
            String tableName,
            MessageType parquetSchema,
            boolean dropIfExists) throws SQLException {

        // Create schema if it doesn't exist
        createSchemaIfNotExists(connection, schemaName);

        // Drop table if exists
        if (dropIfExists) {
            String dropTableSql = String.format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName);
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(dropTableSql);
                System.out.println("Dropped existing table: " + schemaName + "." + tableName);
            }
        }

        // Build CREATE TABLE SQL
        List<String> columnDefinitions = new ArrayList<>();
        
        for (Type field : parquetSchema.getFields()) {
            String columnName = sanitizeColumnName(field.getName());
            String postgresType = convertParquetTypeToPostgresType(field);
            // Handle nullable fields - in Parquet, optional repetition means nullable
            boolean isOptional = field.getRepetition().name().equals("OPTIONAL");
            String nullable = isOptional ? "" : " NOT NULL";
            columnDefinitions.add(columnName + " " + postgresType + nullable);
        }

        // Build the CREATE TABLE statement
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(schemaName).append(".").append(tableName).append(" (");
        createTableSql.append(String.join(", ", columnDefinitions));
        createTableSql.append(")");

        // Execute CREATE TABLE
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableSql.toString());
            System.out.println("Created table: " + schemaName + "." + tableName);
            System.out.println("Columns: " + columnDefinitions.size());
        }
    }

    /**
     * Creates a PostgreSQL table from a Parquet file schema.
     * Uses default database connection parameters and "taxi" schema.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param tableName The table name to create
     * @param dropIfExists Whether to drop the table if it already exists
     * @throws IOException if Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static void createTableFromParquetFile(
            String parquetFilePath,
            String tableName,
            boolean dropIfExists) throws IOException, SQLException {
        
        createTableFromParquetFile(
            parquetFilePath,
            DEFAULT_SCHEMA_NAME,
            tableName,
            DEFAULT_DB_URL,
            DEFAULT_USER,
            DEFAULT_PASSWORD,
            dropIfExists
        );
    }

    /**
     * Creates a PostgreSQL table from a Parquet file schema with custom connection parameters.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param schemaName The PostgreSQL schema name (will be created if it doesn't exist)
     * @param tableName The table name to create
     * @param dbUrl The database URL
     * @param user The database user
     * @param password The database password
     * @param dropIfExists Whether to drop the table if it already exists
     * @throws IOException if Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static void createTableFromParquetFile(
            String parquetFilePath,
            String schemaName,
            String tableName,
            String dbUrl,
            String user,
            String password,
            boolean dropIfExists) throws IOException, SQLException {

        // Read Parquet schema
        System.out.println("Reading Parquet schema from: " + parquetFilePath);
        MessageType schema = ParquetFileReaderUtil.getSchema(parquetFilePath);
        System.out.println("Schema read successfully. Fields: " + schema.getFieldCount());

        // Connect to database
        System.out.println("Connecting to database: " + dbUrl);
        try (Connection connection = DriverManager.getConnection(dbUrl, user, password)) {
            connection.setAutoCommit(true);
            
            // Create table
            createTableFromParquetSchema(connection, schemaName, tableName, schema, dropIfExists);
            
            System.out.println("Table created successfully: " + schemaName + "." + tableName);
        }
    }

    /**
     * Prints the Parquet schema information for a file.
     *
     * @param parquetFilePath Path to the Parquet file
     * @throws IOException if Parquet file cannot be read
     */
    public static void printParquetSchema(String parquetFilePath) throws IOException {
        MessageType schema = ParquetFileReaderUtil.getSchema(parquetFilePath);
        System.out.println("\n=== Parquet Schema: " + parquetFilePath + " ===");
        System.out.println("Schema name: " + schema.getName());
        System.out.println("Number of fields: " + schema.getFieldCount());
        System.out.println("\nFields:");
        for (Type field : schema.getFields()) {
            String typeName = field.isPrimitive() 
                ? field.asPrimitiveType().getPrimitiveTypeName().toString()
                : "nested";
            String pgType = convertParquetTypeToPostgresType(field);
            String columnName = sanitizeColumnName(field.getName());
            System.out.printf("  - %s (%s) -> %s.%s (%s)%n",
                field.getName(),
                typeName,
                DEFAULT_SCHEMA_NAME,
                columnName,
                pgType
            );
        }
    }
}

