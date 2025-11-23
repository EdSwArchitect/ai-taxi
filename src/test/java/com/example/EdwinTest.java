package com.example;

import java.io.IOException;

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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

public class EdwinTest {

  private static OpenSearchClient client;
  private static OpenSearchTransport transportClient;
  
    @BeforeEach
    void setUp() throws Exception{
    //Point to keystore with appropriate certificates for security.
    System.setProperty("javax.net.ssl.trustStore", "/src/test/resources/truststore.jks");
    System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

    //Establish credentials to use basic authentication.
    //Only for demo purposes. Don't specify your credentials in code.
    final HttpHost host = new HttpHost("https", "localhost", 9200);
    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      // Only for demo purposes. Don't specify your credentials in code.
      credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials("admin", "SuperSecret123!".toCharArray()));
  
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
    transportClient = transport;
    client = new OpenSearchClient(transport);
  
    }

    @AfterEach
    void tearDown() throws Exception {
      if (transportClient != null) {
        transportClient.close();
      }
    }

    @Test
    void testInfo() throws IOException {
      final InfoResponse response = client.info();
      System.out.println("Cluster name: " + response.clusterName());
      System.out.println("Cluster UUID: " + response.clusterUuid());
      System.out.println("Version:      " + response.version().number());

      assertNotNull(response);
      assertEquals(response.version().distribution(), "opensearch");
      assertEquals(response.version().number(), "3.3.0");
    }

    @Test
    void testCreateIndex() throws IOException {
      final CreateIndexResponse response = client.indices().create(c -> c
        .index("test-index"));

        assertTrue(response.acknowledged());
        assertEquals(response.index(), "test-index");

        System.out.println("Index created: " + response.index());
    }

  // @Test
  // void testSearch() {
  //   final SearchResponse response = client.search(s -> s
  //     .index("test-index")
  //     .query(q -> q.matchAll()));
  // }
}
