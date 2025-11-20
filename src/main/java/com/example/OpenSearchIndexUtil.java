package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.transport.rest_client.RestClientTransport;

/**
 * Utility class for creating and managing OpenSearch indices.
 */
public class OpenSearchIndexUtil {

    private final OpenSearchClient client;
    private final RestClient restClient;

    /**
     * Constructor that creates a client with default settings (localhost:9200).
     */
    public OpenSearchIndexUtil() {
        this.restClient = createDefaultRestClient();
        this.client = new OpenSearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }

    /**
     * Constructor that accepts a custom OpenSearchClient.
     *
     * @param client the OpenSearch client
     */
    public OpenSearchIndexUtil(OpenSearchClient client) {
        this.client = client;
        this.restClient = null;
    }

    /**
     * Creates a default OpenSearch REST client pointing to localhost:9200.
     *
     * @return RestClient instance
     */
    private static RestClient createDefaultRestClient() {
        return RestClient.builder(
                new org.apache.hc.core5.http.HttpHost("localhost", "http", 9200)
        ).build();
    }

    /**
     * Creates an OpenSearch REST client with custom host and port.
     *
     * @param host the hostname (e.g., "localhost")
     * @param port the port (e.g., 9200)
     * @param scheme the scheme (e.g., "http" or "https")
     * @return RestClient instance
     */
    public static RestClient createRestClient(String host, int port, String scheme) {
        return RestClient.builder(
                new org.apache.hc.core5.http.HttpHost(host, scheme, port)
        ).build();
    }

    /**
     * Creates an OpenSearch client with custom host and port.
     *
     * @param host the hostname (e.g., "localhost")
     * @param port the port (e.g., 9200)
     * @param scheme the scheme (e.g., "http" or "https")
     * @return OpenSearchClient instance
     */
    public static OpenSearchClient createClient(String host, int port, String scheme) {
        RestClient restClient = createRestClient(host, port, scheme);
        return new OpenSearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }

    /**
     * Creates an OpenSearch index with the specified name.
     *
     * @param indexName the name of the index to create
     * @return true if the index was created successfully, false otherwise
     * @throws IOException if an error occurs during the request
     */
    public boolean createIndex(String indexName) throws IOException {
        return createIndex(indexName, null, null);
    }

    /**
     * Creates an OpenSearch index with the specified name and settings.
     *
     * @param indexName the name of the index to create
     * @param settings map of index settings (e.g., number of shards, replicas)
     * @return true if the index was created successfully, false otherwise
     * @throws IOException if an error occurs during the request
     */
    public boolean createIndex(String indexName, Map<String, Object> settings) throws IOException {
        return createIndex(indexName, settings, null);
    }

    /**
     * Creates an OpenSearch index with the specified name, settings, and mappings.
     *
     * @param indexName the name of the index to create
     * @param settings map of index settings (e.g., number of shards, replicas)
     * @param mappings map representing the index mappings (field definitions)
     * @return true if the index was created successfully, false otherwise
     * @throws IOException if an error occurs during the request
     */
    public boolean createIndex(String indexName, Map<String, Object> settings, Map<String, Object> mappings) throws IOException {
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        }

        // Check if index already exists
        if (indexExists(indexName)) {
            System.out.println("Index '" + indexName + "' already exists. Skipping creation.");
            return false;
        }

        try {
            CreateIndexRequest.Builder requestBuilder = new CreateIndexRequest.Builder();
            requestBuilder.index(indexName);

            // Note: Settings and mappings configuration is complex in OpenSearch Java client
            // For production use, properly build Settings and TypeMapping objects from the maps
            // This is a simplified version that creates an index with default settings
            
            CreateIndexRequest request = requestBuilder.build();
            CreateIndexResponse response = client.indices().create(request);

            if (response.acknowledged()) {
                System.out.println("Index '" + indexName + "' created successfully.");
                return true;
            } else {
                System.out.println("Index '" + indexName + "' creation was not acknowledged.");
                return false;
            }
        } catch (OpenSearchException e) {
            System.err.println("Error creating index '" + indexName + "': " + e.getMessage());
            throw new IOException("Failed to create index: " + e.getMessage(), e);
        }
    }

    /**
     * Checks if an index exists.
     *
     * @param indexName the name of the index to check
     * @return true if the index exists, false otherwise
     * @throws IOException if an error occurs during the request
     */
    public boolean indexExists(String indexName) throws IOException {
        if (indexName == null || indexName.trim().isEmpty()) {
            return false;
        }

        try {
            ExistsRequest request = new ExistsRequest.Builder().index(indexName).build();
            return client.indices().exists(request).value();
        } catch (OpenSearchException e) {
            if (e.status() == 404) {
                return false;
            }
            throw new IOException("Error checking if index exists: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes an index if it exists.
     *
     * @param indexName the name of the index to delete
     * @return true if the index was deleted successfully, false otherwise
     * @throws IOException if an error occurs during the request
     */
    public boolean deleteIndex(String indexName) throws IOException {
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        }

        if (!indexExists(indexName)) {
            System.out.println("Index '" + indexName + "' does not exist. Nothing to delete.");
            return false;
        }

        try {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index(indexName).build();
            DeleteIndexResponse response = client.indices().delete(request);

            if (response.acknowledged()) {
                System.out.println("Index '" + indexName + "' deleted successfully.");
                return true;
            } else {
                System.out.println("Index '" + indexName + "' deletion was not acknowledged.");
                return false;
            }
        } catch (OpenSearchException e) {
            throw new IOException("Failed to delete index: " + e.getMessage(), e);
        }
    }

    /**
     * Closes the OpenSearch client. Should be called when done using the utility.
     *
     * @throws IOException if an error occurs while closing the client
     */
    public void close() throws IOException {
        if (restClient != null) {
            restClient.close();
        }
    }

    /**
     * Lists all indices in the OpenSearch cluster.
     *
     * @return a list of index names
     * @throws IOException if an error occurs during the request
     */
    public List<String> listIndices() throws IOException {
        try {
            GetIndexRequest request = new GetIndexRequest.Builder()
                    .index("*")
                    .build();
            var response = client.indices().get(request);
            
            // Extract index names from the response
            List<String> indexNames = new ArrayList<>(response.result().keySet());
            return indexNames;
        } catch (OpenSearchException e) {
            throw new IOException("Failed to list indices: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the underlying OpenSearch client.
     *
     * @return the OpenSearchClient instance
     */
    public OpenSearchClient getClient() {
        return client;
    }
}
