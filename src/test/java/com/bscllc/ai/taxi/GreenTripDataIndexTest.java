package com.bscllc.ai.taxi;

import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

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
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import com.bscllc.ai.taxi.utils.ParquetFileReaderUtil;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class that reads the schema from green_tripdata_2025-02.parquet
 * and creates an OpenSearch index based on that schema.
 */
public class GreenTripDataIndexTest {

    private static final String PARQUET_FILE = "src/main/resources/green_tripdata_2025-01.parquet";
    private static final String INDEX_NAME = "green_tripdata_2025_02";

    /**
     * Creates an OpenSearch client with SSL/TLS configuration.
     */
    private OpenSearchClient createOpenSearchClient() throws Exception {
        String dir = System.getProperty("user.dir");
        System.setProperty("javax.net.ssl.trustStore", dir + "/src/test/resources/truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

        final HttpHost host = new HttpHost("https", "localhost", 9200);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            new AuthScope(host),
            new UsernamePasswordCredentials("admin", "admin".toCharArray())
        );

        final SSLContext sslcontext = SSLContextBuilder
            .create()
            .loadTrustMaterial(null, (chains, authType) -> true)
            .build();

        final ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(host);

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
    private Property convertParquetTypeToOpenSearchProperty(Type parquetType) {
        if (parquetType.isPrimitive()) {
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
                    // For binary types, check if it's likely a string
                    String originalType = primitiveType.getOriginalType() != null 
                        ? primitiveType.getOriginalType().toString() 
                        : "";
                    if (originalType.contains("UTF8") || originalType.contains("STRING")) {
                        // Use keyword for exact matches, text for full-text search
                        return Property.of(p -> p.keyword(k -> k));
                    }
                    return Property.of(p -> p.keyword(k -> k));
                case INT96:
                    // INT96 is often used for timestamps
                    return Property.of(p -> p.date(d -> d));
                default:
                    // Default to keyword for unknown types
                    return Property.of(p -> p.keyword(k -> k));
            }
        } else {
            // For nested/repeated types, use object
            return Property.of(p -> p.object(o -> o));
        }
    }

    /**
     * Converts Parquet schema to OpenSearch type mapping.
     */
    private TypeMapping createMappingFromParquetSchema(MessageType schema) {
        Map<String, Property> properties = new HashMap<>();

        for (Type field : schema.getFields()) {
            String fieldName = field.getName();
            Property property = convertParquetTypeToOpenSearchProperty(field);
            properties.put(fieldName, property);
        }

        return TypeMapping.of(tm -> tm.properties(properties));
    }

    @Test
    public void testCreateIndexFromParquetSchema() throws Exception {
        System.out.println("Reading Parquet schema from: " + PARQUET_FILE);

        // Check if file exists
        java.io.File parquetFile = new java.io.File(PARQUET_FILE);
        if (!parquetFile.exists()) {
            System.out.println("WARNING: Parquet file not found at: " + parquetFile.getAbsolutePath());
            System.out.println("Please ensure green_tripdata_2025-02.parquet exists in src/main/resources/");
            return;
        }

        // Read Parquet schema
        MessageType schema = ParquetFileReaderUtil.getSchema(PARQUET_FILE);
        System.out.println("\n=== Parquet Schema ===");
        System.out.println("Schema name: " + schema.getName());
        System.out.println("Number of fields: " + schema.getFieldCount());
        System.out.println("\nFields:");
        for (Type field : schema.getFields()) {
            System.out.println("  - " + field.getName() + " (" + 
                (field.isPrimitive() ? field.asPrimitiveType().getPrimitiveTypeName() : "nested") + ")");
        }

        // Create OpenSearch client
        System.out.println("\n=== Connecting to OpenSearch ===");
        OpenSearchClient client = createOpenSearchClient();

        // Convert Parquet schema to OpenSearch mapping
        TypeMapping mapping = createMappingFromParquetSchema(schema);
        System.out.println("\n=== Creating OpenSearch Index ===");
        System.out.println("Index name: " + INDEX_NAME);

        // Create index with mappings
        CreateIndexRequest createRequest = CreateIndexRequest.of(c -> c
            .index(INDEX_NAME)
            .mappings(mapping)
            .settings(IndexSettings.of(s -> s
                .numberOfShards(1)
                .numberOfReplicas(0)
            ))
        );

        CreateIndexResponse response = client.indices().create(createRequest);

        assertNotNull(response);
        assertTrue(response.acknowledged());
        System.out.println("\n✓ Index created successfully!");
        System.out.println("Index: " + response.index());
        System.out.println("Acknowledged: " + response.acknowledged());

        // Verify index exists
        boolean exists = client.indices().exists(e -> e.index(INDEX_NAME)).value();
        assertTrue(exists, "Index should exist after creation");
        System.out.println("✓ Index verified to exist");

        // Print index mapping for verification
        var getMappingResponse = client.indices().getMapping(g -> g.index(INDEX_NAME));
        System.out.println("\n=== Index Mapping ===");
        System.out.println(getMappingResponse.toString());
    }
}

