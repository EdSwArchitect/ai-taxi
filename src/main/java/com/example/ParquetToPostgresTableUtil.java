package com.example;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class to read Parquet file schema and create PostgreSQL table based on it.
 * Creates tables in the "taxi" schema, creating the schema if necessary.
 * Includes metrics tracking for files loaded and table entries loaded using Micrometer.
 */
public class ParquetToPostgresTableUtil {

    private static final String DEFAULT_SCHEMA_NAME = "taxi";
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/ai_taxi";
    private static final String DEFAULT_USER = "postgres";
    private static final String DEFAULT_PASSWORD = "postgres";

    // Micrometer Prometheus registry - initialized on first use
    private static volatile PrometheusMeterRegistry prometheusRegistry;
    private static final Object registryLock = new Object();

    // Micrometer counters for metrics
    private static Counter filesLoadedCounter;
    private static Counter tableEntriesLoadedCounter;

    /**
     * Gets or creates the Prometheus MeterRegistry instance.
     * This registry is thread-safe and can be used to expose metrics to Prometheus.
     *
     * @return PrometheusMeterRegistry instance
     */
    public static PrometheusMeterRegistry getPrometheusRegistry() {
        if (prometheusRegistry == null) {
            synchronized (registryLock) {
                if (prometheusRegistry == null) {
                    prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
                    // Add registry to global Metrics registry
                    io.micrometer.core.instrument.Metrics.addRegistry(prometheusRegistry);
                    initializeCounters();
                }
            }
        }
        return prometheusRegistry;
    }

    /**
     * Initializes Micrometer counters for tracking metrics.
     */
    private static void initializeCounters() {
        MeterRegistry registry = getPrometheusRegistry();
        
        filesLoadedCounter = Counter.builder("parquet.files.loaded")
            .description("Total number of Parquet files processed/loaded")
            .tag("component", "parquet_to_postgres")
            .register(registry);

        tableEntriesLoadedCounter = Counter.builder("parquet.table.entries.loaded")
            .description("Total number of table entries (rows) loaded from Parquet files")
            .tag("component", "parquet_to_postgres")
            .register(registry);
    }

    /**
     * Ensures counters are initialized (lazy initialization).
     */
    private static void ensureCountersInitialized() {
        if (filesLoadedCounter == null || tableEntriesLoadedCounter == null) {
            getPrometheusRegistry(); // This will initialize counters
        }
    }

    /**
     * Metrics class to access Micrometer metrics for monitoring.
     */
    public static class Metrics {
        /**
         * Gets the number of files that have been processed/loaded.
         *
         * @return number of files loaded (count from counter)
         */
        public static double getFilesLoaded() {
            ensureCountersInitialized();
            return filesLoadedCounter.count();
        }

        /**
         * Gets the total number of table entries (rows) that have been loaded.
         *
         * @return number of table entries loaded (count from counter)
         */
        public static double getTableEntriesLoaded() {
            ensureCountersInitialized();
            return tableEntriesLoadedCounter.count();
        }

        /**
         * Returns a string representation of current metrics.
         *
         * @return metrics summary
         */
        public static String getSummary() {
            ensureCountersInitialized();
            return String.format("Metrics - Files loaded: %.0f, Table entries loaded: %.0f",
                filesLoadedCounter.count(), tableEntriesLoadedCounter.count());
        }

        /**
         * Gets the Prometheus registry for exposing metrics endpoint.
         *
         * @return PrometheusMeterRegistry
         */
        public static PrometheusMeterRegistry getRegistry() {
            return getPrometheusRegistry();
        }

        /**
         * Gets the Prometheus metrics in scrape format.
         * This can be exposed via HTTP endpoint for Prometheus to scrape.
         *
         * @return Prometheus metrics in scrape format
         */
        public static String scrape() {
            return getPrometheusRegistry().scrape();
        }

        // Package-private methods for internal use
        static void incrementFilesLoaded() {
            ensureCountersInitialized();
            filesLoadedCounter.increment();
        }

        static void incrementTableEntriesLoaded(double count) {
            ensureCountersInitialized();
            tableEntriesLoadedCounter.increment(count);
        }
    }

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
            
            // Increment files loaded metric
            Metrics.incrementFilesLoaded();
            
            System.out.println("Table created successfully: " + schemaName + "." + tableName);
        }
    }

    /**
     * Loads data from a Parquet file into a PostgreSQL table.
     * The table must already exist.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param schemaName The PostgreSQL schema name
     * @param tableName The table name
     * @param dbUrl The database URL
     * @param user The database user
     * @param password The database password
     * @param maxRows Maximum number of rows to load (use -1 for all)
     * @return number of rows loaded
     * @throws IOException if Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static long loadDataFromParquetFile(
            String parquetFilePath,
            String schemaName,
            String tableName,
            String dbUrl,
            String user,
            String password,
            int maxRows) throws IOException, SQLException {

        // Read Parquet data
        System.out.println("Reading data from Parquet file: " + parquetFilePath);
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(parquetFilePath, maxRows);
        System.out.println("Read " + records.size() + " records from Parquet file");

        if (records.isEmpty()) {
            System.out.println("No records to load");
            return 0;
        }

        // Get schema for column order
        MessageType schema = ParquetFileReaderUtil.getSchema(parquetFilePath);
        List<String> columnNames = new ArrayList<>();
        for (Type field : schema.getFields()) {
            columnNames.add(sanitizeColumnName(field.getName()));
        }

        // Connect to database and load data
        System.out.println("Connecting to database: " + dbUrl);
        try (Connection connection = DriverManager.getConnection(dbUrl, user, password)) {
            connection.setAutoCommit(false); // Use batch insert for better performance

            // Build INSERT statement
            String columnList = String.join(", ", columnNames);
            List<String> placeholderList = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                placeholderList.add("?");
            }
            String placeholders = String.join(", ", placeholderList);
            String insertSql = String.format(
                "INSERT INTO %s.%s (%s) VALUES (%s)",
                schemaName, tableName, columnList, placeholders
            );

            int batchSize = 1000;
            long rowsLoaded = 0;

            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                for (Map<String, Object> record : records) {
                    int paramIndex = 1;
                    for (String columnName : columnNames) {
                        Object value = record.get(columnName);
                        pstmt.setObject(paramIndex++, value);
                    }
                    pstmt.addBatch();
                    rowsLoaded++;

                    // Execute batch periodically
                    if (rowsLoaded % batchSize == 0) {
                        pstmt.executeBatch();
                        connection.commit();
                        System.out.println("Loaded " + rowsLoaded + " rows...");
                    }
                }

                // Execute remaining batch
                if (rowsLoaded % batchSize != 0) {
                    pstmt.executeBatch();
                    connection.commit();
                }
            }

            // Update metrics
            Metrics.incrementTableEntriesLoaded(rowsLoaded);

            System.out.println("Successfully loaded " + rowsLoaded + " rows into " + schemaName + "." + tableName);
            return rowsLoaded;
        }
    }

    /**
     * Loads data from a Parquet file into a PostgreSQL table using default connection parameters.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param tableName The table name
     * @param maxRows Maximum number of rows to load (use -1 for all)
     * @return number of rows loaded
     * @throws IOException if Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static long loadDataFromParquetFile(
            String parquetFilePath,
            String tableName,
            int maxRows) throws IOException, SQLException {

        return loadDataFromParquetFile(
            parquetFilePath,
            DEFAULT_SCHEMA_NAME,
            tableName,
            DEFAULT_DB_URL,
            DEFAULT_USER,
            DEFAULT_PASSWORD,
            maxRows
        );
    }

    /**
     * Creates a table and loads data from a Parquet file in one operation.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param schemaName The PostgreSQL schema name
     * @param tableName The table name to create
     * @param dbUrl The database URL
     * @param user The database user
     * @param password The database password
     * @param dropIfExists Whether to drop the table if it already exists
     * @param maxRows Maximum number of rows to load (use -1 for all)
     * @return number of rows loaded
     * @throws IOException if Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static long createTableAndLoadDataFromParquetFile(
            String parquetFilePath,
            String schemaName,
            String tableName,
            String dbUrl,
            String user,
            String password,
            boolean dropIfExists,
            int maxRows) throws IOException, SQLException {

        // Create table first
        createTableFromParquetFile(parquetFilePath, schemaName, tableName, dbUrl, user, password, dropIfExists);

        // Load data
        return loadDataFromParquetFile(parquetFilePath, schemaName, tableName, dbUrl, user, password, maxRows);
    }

    /**
     * Creates a table and loads data from a Parquet file using default connection parameters.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param tableName The table name to create
     * @param dropIfExists Whether to drop the table if it already exists
     * @param maxRows Maximum number of rows to load (use -1 for all)
     * @return number of rows loaded
     * @throws IOException if Parquet file cannot be read
     * @throws SQLException if database operations fail
     */
    public static long createTableAndLoadDataFromParquetFile(
            String parquetFilePath,
            String tableName,
            boolean dropIfExists,
            int maxRows) throws IOException, SQLException {

        return createTableAndLoadDataFromParquetFile(
            parquetFilePath,
            DEFAULT_SCHEMA_NAME,
            tableName,
            DEFAULT_DB_URL,
            DEFAULT_USER,
            DEFAULT_PASSWORD,
            dropIfExists,
            maxRows
        );
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

