package com.bscllc.ai.taxi.utils;

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
 * Utility class to create a database table from a Parquet file schema.
 * 
 * This class takes JDBC connection parameters and creates a table based on
 * the schema found in a Parquet file. The database schema will be created
 * if it doesn't exist.
 * 
 * Usage example:
 * <pre>
 * DatabaseTableFromParquetUtil.createTable(
 *     "jdbc:postgresql://localhost:5432/ai_taxi",
 *     "taxi",
 *     "postgres",
 *     "postgres",
 *     "src/main/resources/green_tripdata_2025-01.parquet",
 *     "green_tripdata_2025_01",
 *     true  // drop if exists
 * );
 * </pre>
 */
public class DatabaseTableFromParquetUtil {

    /**
     * Creates a database table from a Parquet file schema.
     * 
     * @param jdbcUrl The JDBC connection URL (e.g., "jdbc:postgresql://localhost:5432/dbname")
     * @param dbSchema The database schema name (will be created if it doesn't exist)
     * @param user The database user name
     * @param password The database password
     * @param parquetFilePath Path to the Parquet file
     * @param tableName The name of the table to create
     * @param dropIfExists Whether to drop the table if it already exists
     * @throws IOException if the Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static void createTable(
            String jdbcUrl,
            String dbSchema,
            String user,
            String password,
            String parquetFilePath,
            String tableName,
            boolean dropIfExists) throws IOException, SQLException {
        
        System.out.println("Reading Parquet schema from: " + parquetFilePath);
        MessageType schema = ParquetFileReaderUtil.getSchema(parquetFilePath);
        System.out.println("Schema read successfully. Fields: " + schema.getFieldCount());

        System.out.println("Connecting to database: " + jdbcUrl);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
            connection.setAutoCommit(true);
            
            // Create schema if it doesn't exist
            createSchemaIfNotExists(connection, dbSchema);
            
            // Check if table exists
            boolean tableAlreadyExists = tableExists(connection, dbSchema, tableName);
            
            // Drop table if exists and dropIfExists is true
            if (dropIfExists && tableAlreadyExists) {
                dropTableIfExists(connection, dbSchema, tableName);
                tableAlreadyExists = false; // Reset flag after dropping
            }
            
            // Create table only if it doesn't exist
            if (!tableAlreadyExists) {
                createTableFromSchema(connection, dbSchema, tableName, schema);
                System.out.println("Table created successfully: " + dbSchema + "." + tableName);
            } else {
                System.out.println("Table '" + dbSchema + "." + tableName + "' already exists. Skipping creation.");
            }
        }
    }

    /**
     * Creates a database table from a Parquet file schema.
     * Does not drop existing table (safe operation).
     * 
     * @param jdbcUrl The JDBC connection URL
     * @param dbSchema The database schema name
     * @param user The database user name
     * @param password The database password
     * @param parquetFilePath Path to the Parquet file
     * @param tableName The name of the table to create
     * @throws IOException if the Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static void createTable(
            String jdbcUrl,
            String dbSchema,
            String user,
            String password,
            String parquetFilePath,
            String tableName) throws IOException, SQLException {
        createTable(jdbcUrl, dbSchema, user, password, parquetFilePath, tableName, false);
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
     * Checks if a table exists in the specified schema.
     *
     * @param connection The database connection
     * @param schemaName The database schema name
     * @param tableName The table name to check
     * @return true if the table exists, false otherwise
     * @throws SQLException if the check fails
     */
    private static boolean tableExists(Connection connection, String schemaName, String tableName) throws SQLException {
        String sql = "SELECT EXISTS (" +
                     "SELECT FROM information_schema.tables " +
                     "WHERE table_schema = ? AND table_name = ?" +
                     ")";
        try (java.sql.PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, schemaName);
            pstmt.setString(2, tableName);
            try (java.sql.ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
                return false;
            }
        }
    }

    /**
     * Drops the table if it exists.
     * 
     * @param connection The database connection
     * @param schemaName The database schema name
     * @param tableName The table name to drop
     * @throws SQLException if dropping fails
     */
    private static void dropTableIfExists(Connection connection, String schemaName, String tableName) throws SQLException {
        String dropTableSql = String.format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(dropTableSql);
            System.out.println("Dropped existing table: " + schemaName + "." + tableName);
        }
    }

    /**
     * Creates a database table based on the Parquet schema.
     * 
     * @param connection The database connection
     * @param schemaName The database schema name
     * @param tableName The table name to create
     * @param parquetSchema The Parquet schema
     * @throws SQLException if table creation fails
     */
    private static void createTableFromSchema(
            Connection connection,
            String schemaName,
            String tableName,
            MessageType parquetSchema) throws SQLException {
        
        // Build column definitions from Parquet schema
        List<String> columnDefinitions = new ArrayList<>();
        
        for (Type field : parquetSchema.getFields()) {
            String columnName = sanitizeColumnName(field.getName());
            String dbType = convertParquetTypeToDatabaseType(field);
            // Handle nullable fields - in Parquet, optional repetition means nullable
            boolean isOptional = field.getRepetition().name().equals("OPTIONAL");
            String nullable = isOptional ? "" : " NOT NULL";
            columnDefinitions.add(columnName + " " + dbType + nullable);
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
     * Converts Parquet type to database data type.
     * Currently optimized for PostgreSQL, but can be extended for other databases.
     * 
     * @param parquetType The Parquet type
     * @return Database data type string
     */
    private static String convertParquetTypeToDatabaseType(Type parquetType) {
        if (!parquetType.isPrimitive()) {
            // For nested types, use JSONB for flexibility (PostgreSQL-specific)
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
                // For other binary types, use TEXT
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
     * Sanitizes a field name for use as a database column name.
     * Converts to lowercase and replaces invalid characters with underscore.
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
     * Prints the Parquet schema information for a file.
     * 
     * @param parquetFilePath Path to the Parquet file
     * @throws IOException if Parquet file cannot be read
     */
    public static void printSchema(String parquetFilePath) throws IOException {
        MessageType schema = ParquetFileReaderUtil.getSchema(parquetFilePath);
        System.out.println("\n=== Parquet Schema: " + parquetFilePath + " ===");
        System.out.println("Schema name: " + schema.getName());
        System.out.println("Number of fields: " + schema.getFieldCount());
        System.out.println("\nFields:");
        for (Type field : schema.getFields()) {
            String typeName = field.isPrimitive() 
                ? field.asPrimitiveType().getPrimitiveTypeName().toString()
                : "nested";
            String dbType = convertParquetTypeToDatabaseType(field);
            String columnName = sanitizeColumnName(field.getName());
            System.out.printf("  - %s (%s) -> %s (%s)%n",
                field.getName(),
                typeName,
                columnName,
                dbType
            );
        }
    }
}

