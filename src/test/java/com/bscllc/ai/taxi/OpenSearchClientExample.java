package com.bscllc.ai.taxi;

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
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OpenSearchClientExample {

    @Test
    public void testConnection() throws Exception {
        System.out.println("Testing connection to OpenSearch...");

        String dir = System.getProperty("user.dir");
        System.out.println("CWD: " + dir);

        String truststorePath = dir + "/src/test/resources/truststore.jks";
        System.out.println("Truststore path: " + truststorePath);

        // Note: Use "localhost" when running from host machine
        // Use "opensearch-node1" when running inside Docker network
        System.setProperty("javax.net.ssl.trustStore", dir + "/src/test/resources/truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

        // When running from host: use "localhost"
        // When running inside Docker: use "opensearch-node1"
        final HttpHost host = new HttpHost("https", "localhost", 9200);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        // Only for demo purposes. Don't specify your credentials in code.
        credentialsProvider.setCredentials(
            new AuthScope(host),
            // new UsernamePasswordCredentials("admin", "SuperSecret123!".toCharArray())
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
                // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
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
        OpenSearchClient client = new OpenSearchClient(transport);

        final InfoResponse response = client.info();
        System.out.println("Cluster name: " + response.clusterName());
        System.out.println("Cluster UUID: " + response.clusterUuid());
        System.out.println("Version:      " + response.version().number());

        assertNotNull(response);
        assertEquals(response.version().distribution(), "opensearch");
        // assertEquals(response.version().number(), "2.15.0");
        assertEquals(response.version().number(), "3.3.2");
    }
}
