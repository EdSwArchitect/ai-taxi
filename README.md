# ai-taxi

AI Taxi Project for reading and processing Parquet files, with OpenSearch integration for indexing and searching.

## Overview

This project provides utilities for:
- Reading and processing Parquet files (taxi trip data)
- Creating and managing OpenSearch indices
- Integration testing with Testcontainers

## Getting Started

### Prerequisites

- **Java 21** or higher
- **Maven 3.6+**
- **Docker** (for running OpenSearch tests and docker-compose services)

### Building the Project

```bash
# Compile the project
mvn compile

# Run tests
mvn test

# Package the project
mvn package
```

## Requirements

- **Parquet Version**: Apache Parquet version **1.15.0 or higher** is required.
  - This project currently uses Parquet version 1.15.2
  - Earlier versions may have compatibility issues with Jackson serialization when reading metadata
- **OpenSearch Version**: OpenSearch 3.3.0
  - The project uses OpenSearch 3.3.0 for both the Java client and Docker containers

## Dependencies

- Java 21
- Apache Parquet 1.15.0+
- Apache Hadoop 3.3.6
- JUnit 5.10.0
- OpenSearch Java Client 3.3.0
- Testcontainers (for integration testing with OpenSearch)

## OpenSearch Index Utility

The project includes `OpenSearchIndexUtil`, a utility class for creating and managing OpenSearch indices.

### Features

- Create OpenSearch indices with custom settings and mappings
- Check if an index exists
- Delete indices
- Support for custom OpenSearch client configuration

### Basic Usage

```java
// Create utility with default connection (localhost:9200)
OpenSearchIndexUtil util = new OpenSearchIndexUtil();

try {
    // Create an index
    boolean created = util.createIndex("my-index");
    
    // Check if index exists
    boolean exists = util.indexExists("my-index");
    
    // Delete an index
    boolean deleted = util.deleteIndex("my-index");
} finally {
    // Always close the client when done
    util.close();
}
```

### Custom Connection

```java
// Create client with custom host and port
OpenSearchClient client = OpenSearchIndexUtil.createClient("localhost", 9200, "http");
OpenSearchIndexUtil util = new OpenSearchIndexUtil(client);
```

### Methods

- `createIndex(String indexName)` - Create an index with default settings
- `createIndex(String indexName, Map<String, Object> settings)` - Create with custom settings
- `createIndex(String indexName, Map<String, Object> settings, Map<String, Object> mappings)` - Create with settings and mappings
- `indexExists(String indexName)` - Check if an index exists
- `deleteIndex(String indexName)` - Delete an index
- `close()` - Close the OpenSearch client connection

## Docker Compose

The project includes two docker-compose files for running a complete development environment:

### Standard Configuration (`docker-compose.yml`)

The standard configuration runs all services with security disabled for easy development:

- **PostgreSQL** (port 5432) - Database service
- **Kafka** (ports 9092, 9093, 9094) - Message broker
- **OpenSearch** (port 9200) - Search and analytics engine (version 3.3.0) with security disabled
- **OpenSearch Dashboards** (port 5601) - Visualization and management UI
- **Grafana** (port 3000) - Monitoring and observability

### SSL/TLS Configuration (`docker-compose-ssl.yml`)

The SSL-enabled configuration runs OpenSearch with SSL/TLS using a Java keystore:

- All services from the standard configuration
- **OpenSearch** with SSL/TLS enabled using `self-signed.jks` keystore
- Security plugin enabled with basic authentication
- HTTPS connections required

**Prerequisites for SSL configuration:**
- A Java keystore file (`self-signed.jks`) must be present in `src/test/resources/`
- Default keystore password: `edwinpass` (configurable in the compose file)

### Starting Services

#### Standard Configuration

```bash
# Start all services
docker-compose up -d

# Start only OpenSearch
docker-compose up -d opensearch

# Start OpenSearch and Dashboards
docker-compose up -d opensearch opensearch-dashboards

# View logs
docker-compose logs -f opensearch

# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

#### SSL/TLS Configuration

```bash
# Start all services with SSL/TLS
docker-compose -f docker-compose-ssl.yml up -d

# Start only OpenSearch with SSL/TLS
docker-compose -f docker-compose-ssl.yml up -d opensearch

# View logs
docker-compose -f docker-compose-ssl.yml logs -f opensearch

# Stop all services
docker-compose -f docker-compose-ssl.yml down

# Stop and remove volumes (clean slate)
docker-compose -f docker-compose-ssl.yml down -v
```

### Service URLs

#### Standard Configuration

Once services are running:
- OpenSearch: http://localhost:9200
- OpenSearch Dashboards: http://localhost:5601
- Grafana: http://localhost:3000 (admin/admin)
- PostgreSQL: localhost:5432
- Kafka: localhost:9094 (external listener)

#### SSL/TLS Configuration

Once services are running:
- OpenSearch: https://localhost:9200 (admin/admin)
- OpenSearch Dashboards: http://localhost:5601 (admin/admin)
- Grafana: http://localhost:3000 (admin/admin)
- PostgreSQL: localhost:5432
- Kafka: localhost:9094 (external listener)

**Note**: When using the SSL configuration, you'll need to configure your Java client to trust the self-signed certificate. The keystore file at `src/test/resources/self-signed.jks` is used by both the OpenSearch container and your Java application.

## Project Structure

```
ai-taxi/
├── src/
│   ├── main/
│   │   ├── java/com/example/
│   │   │   ├── App.java                    # Main application class
│   │   │   ├── OpenSearchIndexUtil.java    # OpenSearch index management utility
│   │   │   ├── ParquetFileReaderUtil.java  # Parquet file reading utility
│   │   │   └── ParquetReaderEdwin.java    # Alternative Parquet reader
│   │   └── resources/
│   │       ├── application.properties
│   │       └── *.parquet                   # Parquet data files
│   └── test/
│       ├── java/com/example/
│       │   ├── OpenSearchIndexUtilTest.java # OpenSearch integration tests
│       │   ├── FhvTripDataSchemaTest.java   # Schema validation tests
│       │   ├── FhvTripDataTest.java         # FHV trip data tests
│       │   ├── ParquetReaderTest.java       # Parquet reader tests
│       │   └── AppTest.java                # Application tests
│       └── resources/
│           └── test.properties
├── docker-compose.yml                       # Docker services configuration (standard)
├── docker-compose-ssl.yml                   # Docker services configuration (SSL/TLS enabled)
├── pom.xml                                  # Maven project configuration
└── README.md                                # This file
```

## Running Tests

### Run All Tests

```bash
mvn test
```

### Run a Specific Test Class

```bash
# Run FhvTripDataSchemaTest
mvn test -Dtest=FhvTripDataSchemaTest

# Run ParquetReaderTest
mvn test -Dtest=ParquetReaderTest

# Run FhvTripDataTest
mvn test -Dtest=FhvTripDataTest

# Run AppTest
mvn test -Dtest=AppTest
```

### Run a Specific Test Method

```bash
# Run a specific test method in a class
mvn test -Dtest=FhvTripDataSchemaTest#testParquetFileHasSchema
mvn test -Dtest=FhvTripDataSchemaTest#displaySchema
```

### Test Classes

- **FhvTripDataSchemaTest**: Tests for displaying and validating schema of `fhv_tripdata_2025-01.parquet`
- **FhvTripDataTest**: Comprehensive tests for reading `fhv_tripdata_2025-01.parquet` file
- **ParquetReaderTest**: Tests for reading `fhvhv_tripdata_2025-01.parquet` file
- **AppTest**: Basic unit test for the App class
- **OpenSearchIndexUtilTest**: Integration tests for OpenSearchIndexUtil using Testcontainers

### Test Requirements

- **Docker**: Required for running OpenSearch tests (Testcontainers uses Docker to spin up OpenSearch containers)
  - Ensure Docker is installed and running on your machine
  - The OpenSearch tests will automatically start an OpenSearch container using Testcontainers

- **Parquet files** must be present in `src/main/resources/` for the Parquet tests to run successfully:
  - `fhv_tripdata_2025-01.parquet`
  - `fhvhv_tripdata_2025-01.parquet`
  - `green_tripdata_2025-01.parquet`
  - `yellow_tripdata_2025-01.parquet`

### Running OpenSearch Tests

The `OpenSearchIndexUtilTest` uses Testcontainers to automatically create and manage an OpenSearch container for integration testing:

```bash
# Run only OpenSearch tests
mvn test -Dtest=OpenSearchIndexUtilTest

# Run all tests (including OpenSearch tests)
mvn test
```

**Note**: The first time you run OpenSearch tests, Docker will download the OpenSearch image (opensearchproject/opensearch:3.3.0), which may take a few minutes. Subsequent runs will be faster.

## Utility Classes

### ParquetFileReaderUtil

Utility class for reading Parquet files with methods to:
- Get schema information from Parquet files
- Read records from Parquet files
- Print schema information
- Handle Jackson serialization issues

### ParquetReaderEdwin

Alternative Parquet reader implementation with:
- Direct metadata access for schema reading
- Record reading capabilities
- Fallback mechanisms for error handling

## License

This project is part of the AI Taxi playground.