package com.bscllc.ai.taxi.utils;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to read Parquet file schema and data, then load it into OpenSearch.
 * Creates indexes based on Parquet schema and indexes all documents.
 * Includes metrics tracking using Micrometer for Prometheus integration.
 */
public class ParquetToOpenSearchUtil {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9200;
    private static final String DEFAULT_SCHEME = "https";
    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "admin";

    // Micrometer Prometheus registry - initialized on first use
    private static volatile PrometheusMeterRegistry prometheusRegistry;
    private static final Object registryLock = new Object();

    // Micrometer counters for metrics
    private static Counter filesLoadedCounter;
    private static Counter entriesLoadedCounter;

    /**
     * Gets or creates the Prometheus MeterRegistry instance.
     *
     * @return PrometheusMeterRegistry instance
     */
    private static PrometheusMeterRegistry getPrometheusRegistry() {
        if (prometheusRegistry == null) {
            synchronized (registryLock) {
                if (prometheusRegistry == null) {
                    prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
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
        
        filesLoadedCounter = Counter.builder("opensearch.parquet.files.loaded")
            .description("Total number of Parquet files loaded into OpenSearch")
            .tag("component", "parquet_to_opensearch")
            .register(registry);

        entriesLoadedCounter = Counter.builder("opensearch.parquet.entries.loaded")
            .description("Total number of entries (documents) loaded into OpenSearch from Parquet files")
            .tag("component", "parquet_to_opensearch")
            .register(registry);
    }

    /**
     * Ensures counters are initialized (lazy initialization).
     */
    private static void ensureCountersInitialized() {
        if (filesLoadedCounter == null || entriesLoadedCounter == null) {
            getPrometheusRegistry();
        }
    }

    /**
     * Metrics class to access Micrometer metrics for monitoring.
     */
    public static class Metrics {
        /**
         * Gets the number of files that have been loaded into OpenSearch.
         *
         * @return number of files loaded (count from counter)
         */
        public static double getFilesLoaded() {
            ensureCountersInitialized();
            return filesLoadedCounter.count();
        }

        /**
         * Gets the total number of entries (documents) that have been loaded.
         *
         * @return number of entries loaded (count from counter)
         */
        public static double getEntriesLoaded() {
            ensureCountersInitialized();
            return entriesLoadedCounter.count();
        }

        /**
         * Returns a string representation of current metrics.
         *
         * @return metrics summary
         */
        public static String getSummary() {
            ensureCountersInitialized();
            return String.format("Metrics - Files loaded: %.0f, Entries loaded: %.0f",
                filesLoadedCounter.count(), entriesLoadedCounter.count());
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
         *
         * @return Prometheus metrics in scrape format
         */
        public static String scrape() {
            return getPrometheusRegistry().scrape();
        }
    }

    /**
     * Creates an OpenSearch client with SSL/TLS configuration.
     */
    private static OpenSearchClient createOpenSearchClient(
            String host,
            int port,
            String scheme,
            String username,
            String password,
            String truststorePath,
            String truststorePassword) throws Exception {

        // Set SSL truststore properties if provided
        if (truststorePath != null && !truststorePath.isEmpty()) {
            System.setProperty("javax.net.ssl.trustStore", truststorePath);
            System.setProperty("javax.net.ssl.trustStorePassword", truststorePassword);
        }

        final HttpHost httpHost = new HttpHost(scheme, host, port);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            new AuthScope(httpHost),
            new UsernamePasswordCredentials(username, password.toCharArray())
        );

        final SSLContext sslcontext = SSLContextBuilder
            .create()
            .loadTrustMaterial(null, (chains, authType) -> true)
            .build();

        final ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(httpHost);

        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                .setSslContext(sslcontext)
                .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                    @Override
                    public TlsDetails create(final SSLEngine sslEngine) {
                        return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                    }
                })
                .build();

            final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder
                .create()
                .setTlsStrategy(tlsStrategy)
                .build();

            return httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)
                .setConnectionManager(connectionManager);
        });

        final OpenSearchTransport transport = builder.build();
        return new OpenSearchClient(transport);
    }

    /**
     * Converts Parquet type to OpenSearch property type.
     */
    private static Property convertParquetTypeToOpenSearchProperty(Type parquetType) {
        if (!parquetType.isPrimitive()) {
            return Property.of(p -> p.object(o -> o));
        }

        PrimitiveType primitiveType = parquetType.asPrimitiveType();
        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();

        switch (primitiveTypeName) {
            case INT32:
                return Property.of(p -> p.integer(i -> i));
            case INT64:
                return Property.of(p -> p.long_(l -> l));
            case FLOAT:
                return Property.of(p -> p.float_(f -> f));
            case DOUBLE:
                return Property.of(p -> p.double_(d -> d));
            case BOOLEAN:
                return Property.of(p -> p.boolean_(b -> b));
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                String originalType = primitiveType.getOriginalType() != null 
                    ? primitiveType.getOriginalType().toString() 
                    : "";
                if (originalType.contains("UTF8") || originalType.contains("STRING")) {
                    return Property.of(p -> p.keyword(k -> k));
                }
                return Property.of(p -> p.keyword(k -> k));
            case INT96:
                return Property.of(p -> p.date(d -> d));
            default:
                return Property.of(p -> p.keyword(k -> k));
        }
    }

    /**
     * Converts Parquet schema to OpenSearch type mapping.
     */
    private static TypeMapping createMappingFromParquetSchema(MessageType schema) {
        Map<String, Property> properties = new HashMap<>();

        for (Type field : schema.getFields()) {
            String fieldName = field.getName();
            Property property = convertParquetTypeToOpenSearchProperty(field);
            properties.put(fieldName, property);
        }

        return TypeMapping.of(tm -> tm.properties(properties));
    }

    /**
     * Creates an OpenSearch index based on Parquet schema.
     */
    private static void createIndex(OpenSearchClient client, String indexName, MessageType schema, boolean dropIfExists) throws IOException {
        // Drop index if exists
        if (dropIfExists) {
            try {
                client.indices().delete(d -> d.index(indexName));
                System.out.println("Dropped existing index: " + indexName);
            } catch (Exception e) {
                // Index might not exist, ignore
            }
        }

        // Create mapping from schema
        TypeMapping mapping = createMappingFromParquetSchema(schema);

        // Create index with mappings
        CreateIndexRequest createRequest = CreateIndexRequest.of(c -> c
            .index(indexName)
            .mappings(mapping)
            .settings(IndexSettings.of(s -> s
                .numberOfShards(1)
                .numberOfReplicas(0)
            ))
        );

        CreateIndexResponse response = client.indices().create(createRequest);

        if (response.acknowledged()) {
            System.out.println("Created index: " + indexName);
        } else {
            throw new IOException("Failed to create index: " + indexName);
        }
    }

    /**
     * Bulk indexes documents into OpenSearch.
     */
    private static long bulkIndexDocuments(OpenSearchClient client, String indexName, List<Map<String, Object>> documents) throws IOException {
        if (documents.isEmpty()) {
            return 0;
        }

        int batchSize = 1000;
        long totalIndexed = 0;

        for (int i = 0; i < documents.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, documents.size());
            List<Map<String, Object>> batch = documents.subList(i, endIndex);

            List<BulkOperation> bulkOperations = new ArrayList<>();
            for (Map<String, Object> doc : batch) {
                IndexOperation<Map<String, Object>> indexOp = IndexOperation.of(op -> op
                    .index(indexName)
                    .document(doc)
                );
                bulkOperations.add(BulkOperation.of(b -> b.index(indexOp)));
            }

            BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(bulkOperations));
            var bulkResponse = client.bulk(bulkRequest);

            if (bulkResponse.errors()) {
                System.err.println("Some errors occurred during bulk indexing");
                bulkResponse.items().forEach(item -> {
                    var error = item.error();
                    if (error != null && error.reason() != null) {
                        System.err.println("Error: " + error.reason());
                    }
                });
            }

            totalIndexed += batch.size();
            System.out.println("Indexed " + totalIndexed + " / " + documents.size() + " documents...");
        }

        return totalIndexed;
    }

    /**
     * Loads a Parquet file into OpenSearch.
     * Creates an index based on the Parquet schema and indexes all documents.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param indexName The OpenSearch index name
     * @param host OpenSearch host (default: localhost)
     * @param port OpenSearch port (default: 9200)
     * @param scheme HTTP scheme (default: https)
     * @param username OpenSearch username (default: admin)
     * @param password OpenSearch password (default: admin)
     * @param truststorePath Path to truststore (optional)
     * @param truststorePassword Truststore password (optional)
     * @param dropIfExists Whether to drop the index if it already exists
     * @param maxRecords Maximum number of records to load (use -1 for all)
     * @return number of documents indexed
     * @throws Exception if operation fails
     */
    public static long loadParquetFileToOpenSearch(
            String parquetFilePath,
            String indexName,
            String host,
            int port,
            String scheme,
            String username,
            String password,
            String truststorePath,
            String truststorePassword,
            boolean dropIfExists,
            int maxRecords) throws Exception {

        System.out.println("Loading Parquet file to OpenSearch: " + parquetFilePath);

        // Read Parquet schema
        System.out.println("Reading Parquet schema...");
        MessageType schema = ParquetFileReaderUtil.getSchema(parquetFilePath);
        System.out.println("Schema read successfully. Fields: " + schema.getFieldCount());

        // Read Parquet data
        System.out.println("Reading Parquet data...");
        List<Map<String, Object>> records = ParquetFileReaderUtil.readParquetFile(parquetFilePath, maxRecords);
        System.out.println("Read " + records.size() + " records from Parquet file");

        if (records.isEmpty()) {
            System.out.println("No records to index");
            return 0;
        }

        // Create OpenSearch client
        System.out.println("Connecting to OpenSearch at " + scheme + "://" + host + ":" + port);
        OpenSearchClient client = createOpenSearchClient(host, port, scheme, username, password, truststorePath, truststorePassword);

        // Create index
        System.out.println("Creating index: " + indexName);
        createIndex(client, indexName, schema, dropIfExists);

        // Bulk index documents
        System.out.println("Indexing " + records.size() + " documents...");
        long documentsIndexed = bulkIndexDocuments(client, indexName, records);

        // Update metrics
        ensureCountersInitialized();
        filesLoadedCounter.increment();
        entriesLoadedCounter.increment(documentsIndexed);

        System.out.println("Successfully indexed " + documentsIndexed + " documents into index: " + indexName);
        return documentsIndexed;
    }

    /**
     * Loads a Parquet file into OpenSearch using default connection parameters.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param indexName The OpenSearch index name
     * @param truststorePath Path to truststore
     * @param dropIfExists Whether to drop the index if it already exists
     * @param maxRecords Maximum number of records to load (use -1 for all)
     * @return number of documents indexed
     * @throws Exception if operation fails
     */
    public static long loadParquetFileToOpenSearch(
            String parquetFilePath,
            String indexName,
            String truststorePath,
            boolean dropIfExists,
            int maxRecords) throws Exception {

        return loadParquetFileToOpenSearch(
            parquetFilePath,
            indexName,
            DEFAULT_HOST,
            DEFAULT_PORT,
            DEFAULT_SCHEME,
            DEFAULT_USER,
            DEFAULT_PASSWORD,
            truststorePath,
            "changeit",
            dropIfExists,
            maxRecords
        );
    }

    /**
     * Loads a Parquet file into OpenSearch using default connection parameters and loads all records.
     *
     * @param parquetFilePath Path to the Parquet file
     * @param indexName The OpenSearch index name
     * @param truststorePath Path to truststore
     * @param dropIfExists Whether to drop the index if it already exists
     * @return number of documents indexed
     * @throws Exception if operation fails
     */
    public static long loadParquetFileToOpenSearch(
            String parquetFilePath,
            String indexName,
            String truststorePath,
            boolean dropIfExists) throws Exception {

        return loadParquetFileToOpenSearch(parquetFilePath, indexName, truststorePath, dropIfExists, -1);
    }

    public static void main(String[] args) {
        try {
                    // Start metrics server (will be available at http://localhost:8080/metrics)
            com.bscllc.ai.taxi.metrics.MetricsServer metricsServer = com.bscllc.ai.taxi.metrics.MetricsServer.start(8080);

            System.out.println("Metrics server is running. Press Ctrl+C to stop.");
            
            // Keep the server running
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down metrics server...");
                metricsServer.stop();
            }));


            long documentsIndexed = loadParquetFileToOpenSearch("src/main/resources/green_tripdata_2025-01.parquet", "green_tripdata_2025-01", "src/test/resources/truststore.jks", false, -1);
            System.out.println("Documents indexed: " + documentsIndexed);

            // double filesLoaded = ParquetToOpenSearchUtil.Metrics.getFilesLoaded();
            // double entriesLoaded = ParquetToOpenSearchUtil.Metrics.getEntriesLoaded();
            // String summary = ParquetToOpenSearchUtil.Metrics.getSummary();
            // System.out.println("Summary: " + summary);
            // System.out.println("Files loaded: " + filesLoaded);
            // System.out.println("Entries loaded: " + entriesLoaded);
            // // Get Prometheus scrape format (for HTTP endpoint)
            // String prometheusMetrics = ParquetToOpenSearchUtil.Metrics.scrape();

            // System.out.println("Prometheus metrics: " + prometheusMetrics);
            // Keep server running for Prometheus to scrape

            // Thread.currentThread().join();

            // metricsServer.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

