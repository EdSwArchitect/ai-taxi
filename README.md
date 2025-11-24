# ai-taxi

AI Taxi Project for reading and processing Parquet files, with OpenSearch and PostgreSQL integration for indexing, searching, and data storage.

## Overview

This project provides utilities for:
- Reading and processing Parquet files (taxi trip data)
- Creating and managing OpenSearch indices from Parquet schemas
- Creating PostgreSQL tables from Parquet schemas
- Loading Parquet data into OpenSearch
- Loading Parquet data into PostgreSQL
- Metrics tracking with Micrometer for Prometheus integration
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
- **OpenSearch Version**: OpenSearch 3.3.0 / 3.3.2
  - The project uses OpenSearch 3.3.0+ for the Java client
  - Docker containers use OpenSearch 3.3.0 (standard) or 3.3.2 (SSL configuration)

## Dependencies

- Java 21
- Apache Parquet 1.15.0+
- Apache Hadoop 3.3.6
- JUnit 5.10.0
- OpenSearch Java Client 3.3.0
- PostgreSQL JDBC Driver 42.7.1
- Micrometer Core 1.13.0 (for metrics)
- Micrometer Registry Prometheus 1.13.0 (for Prometheus integration)
- Testcontainers (for integration testing with OpenSearch)

## Utility Classes

### ParquetToOpenSearchUtil

Utility class that reads Parquet file schema and data, then loads it into OpenSearch. Automatically creates indexes based on Parquet schema and indexes all documents. Includes Micrometer metrics for monitoring.

#### Features

- Reads Parquet file schema and automatically converts to OpenSearch field mappings
- Creates OpenSearch index with appropriate field types
- Bulk indexes documents from Parquet files (batch size: 1000)
- SSL/TLS support for secure connections
- Micrometer metrics tracking:
  - `opensearch.parquet.files.loaded` - Number of files processed
  - `opensearch.parquet.entries.loaded` - Total number of documents indexed

#### Basic Usage

```java
// Load Parquet file to OpenSearch with default settings
String truststorePath = System.getProperty("user.dir") + "/src/test/resources/truststore.jks";
long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
    "src/main/resources/green_tripdata_2025-01.parquet",
    "green_tripdata_index",
    truststorePath,
    true,  // drop index if exists
    -1     // load all records (-1 = all)
);

// Check metrics
System.out.println(ParquetToOpenSearchUtil.Metrics.getSummary());
```

#### Advanced Usage

```java
// Custom connection parameters
long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
    "src/main/resources/green_tripdata_2025-01.parquet",
    "green_tripdata_index",
    "localhost",      // host
    9200,             // port
    "https",          // scheme
    "admin",          // username
    "admin",          // password
    truststorePath,   // truststore path
    "changeit",       // truststore password
    true,             // drop if exists
    1000              // max records to load
);
```

#### Type Mapping

The utility automatically converts Parquet types to OpenSearch field types:
- `INT32` → `integer`
- `INT64` → `long`
- `FLOAT` → `float`
- `DOUBLE` → `double`
- `BOOLEAN` → `boolean`
- `BINARY`/`STRING` → `keyword`
- `INT96` → `date` (for timestamps)
- Nested types → `object`

#### Metrics Access

```java
// Get metrics
double filesLoaded = ParquetToOpenSearchUtil.Metrics.getFilesLoaded();
double entriesLoaded = ParquetToOpenSearchUtil.Metrics.getEntriesLoaded();
String summary = ParquetToOpenSearchUtil.Metrics.getSummary();

// Get Prometheus scrape format (for HTTP endpoint)
String prometheusMetrics = ParquetToOpenSearchUtil.Metrics.scrape();
```

### ParquetToPostgresTableUtil

Utility class that reads Parquet file schema and creates PostgreSQL tables based on it. Creates tables in the "taxi" schema (creating the schema if necessary). Includes Micrometer metrics for monitoring.

#### Features

- Reads Parquet file schema and automatically converts to PostgreSQL column types
- Creates PostgreSQL schema ("taxi") if it doesn't exist
- Creates tables with appropriate column types and nullability
- Bulk loads data from Parquet files into PostgreSQL tables
- Micrometer metrics tracking:
  - `parquet.files.loaded` - Number of files processed
  - `parquet.table.entries.loaded` - Total number of rows loaded

#### Basic Usage

```java
// Create table from Parquet schema
ParquetToPostgresTableUtil.createTableFromParquetFile(
    "src/main/resources/green_tripdata_2025-01.parquet",
    "green_tripdata_2025_01",
    true  // drop if exists
);

// Create table and load data in one operation
long rowsLoaded = ParquetToPostgresTableUtil.createTableAndLoadDataFromParquetFile(
    "src/main/resources/green_tripdata_2025-01.parquet",
    "green_tripdata_2025_01",
    true,  // drop if exists
    -1     // load all rows
);
```

#### Advanced Usage

```java
// Custom connection parameters
ParquetToPostgresTableUtil.createTableFromParquetFile(
    "src/main/resources/green_tripdata_2025-01.parquet",
    "taxi",              // schema name
    "green_tripdata",    // table name
    "jdbc:postgresql://localhost:5432/ai_taxi",
    "postgres",
    "postgres",
    true  // drop if exists
);

// Load data into existing table
long rowsLoaded = ParquetToPostgresTableUtil.loadDataFromParquetFile(
    "src/main/resources/green_tripdata_2025-01.parquet",
    "taxi",
    "green_tripdata",
    "jdbc:postgresql://localhost:5432/ai_taxi",
    "postgres",
    "postgres",
    10000  // max rows to load
);
```

#### Type Mapping

The utility automatically converts Parquet types to PostgreSQL column types:
- `INT32` → `INTEGER`
- `INT64` → `BIGINT`
- `FLOAT` → `REAL`
- `DOUBLE` → `DOUBLE PRECISION`
- `BOOLEAN` → `BOOLEAN`
- `BINARY`/`STRING` → `TEXT`
- `INT96` → `TIMESTAMP`
- Nested types → `JSONB`

#### Metrics Access

```java
// Get metrics
double filesLoaded = ParquetToPostgresTableUtil.Metrics.getFilesLoaded();
double entriesLoaded = ParquetToPostgresTableUtil.Metrics.getTableEntriesLoaded();
String summary = ParquetToPostgresTableUtil.Metrics.getSummary();

// Get Prometheus scrape format
String prometheusMetrics = ParquetToPostgresTableUtil.Metrics.scrape();
```

## Docker Compose

The project includes two docker-compose files for running a complete development environment:

### Standard Configuration (`docker-compose.yml`)

The standard configuration runs all services with security disabled for easy development:

- **PostgreSQL** (port 5432) - Database service
- **Kafka** (ports 9092, 9093, 9094) - Message broker
- **OpenSearch** (port 9200) - Search and analytics engine (version 3.3.0) with security disabled
- **OpenSearch Dashboards** (port 5601) - Visualization and management UI
- **Grafana** (port 3000) - Monitoring and observability

### SSL/TLS Configuration (`docker-compose-ssl-2.yaml`)

The SSL-enabled configuration runs OpenSearch with SSL/TLS using a Java keystore:

- All services from the standard configuration
- **OpenSearch** (version 3.3.2) with SSL/TLS enabled using `opensearch-keystore.jks` and `truststore.jks`
- Security plugin enabled with basic authentication
- HTTPS connections required
- Admin credentials: `admin/admin` (default password)

**Prerequisites for SSL configuration:**
- SSL certificates must be available in the `./certs` directory:
  - `certs/opensearch-keystore.jks`
  - `certs/truststore.jks`
- Certificates should have password: `changeit`
- Truststore should also be available at `src/test/resources/truststore.jks` for Java clients

**See `docker-compose-ssl-2.yaml` file for detailed setup instructions in the header comments.**

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

#### SSL/TLS Configuration (`docker-compose-ssl-2.yaml`)

```bash
# Start all services with SSL/TLS
docker-compose -f docker-compose-ssl-2.yaml up -d

# Check OpenSearch is running (wait 30-60 seconds for startup)
docker-compose -f docker-compose-ssl-2.yaml ps

# Verify OpenSearch health
curl -k -u admin:admin https://localhost:9200/_cluster/health

# View logs
docker-compose -f docker-compose-ssl-2.yaml logs -f opensearch-node1

# Stop all services
docker-compose -f docker-compose-ssl-2.yaml down

# Stop and remove volumes (clean slate - WARNING: deletes all data)
docker-compose -f docker-compose-ssl-2.yaml down -v
```

**Note**: See the `docker-compose-ssl-2.yaml` file header for comprehensive setup and troubleshooting instructions.

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

**Note**: When using the SSL configuration, you'll need to configure your Java client to trust the self-signed certificate. The truststore file at `src/test/resources/truststore.jks` is used by Java clients to connect securely.

## Project Structure

```
ai-taxi/
├── src/
│   ├── main/
│   │   ├── java/com/example/
│   │   │   ├── App.java                      # Main application class
│   │   │   ├── ParquetFileReaderUtil.java    # Parquet file reading utility
│   │   │   ├── ParquetReaderEdwin.java       # Alternative Parquet reader
│   │   │   ├── ParquetToOpenSearchUtil.java  # Load Parquet files to OpenSearch
│   │   │   └── ParquetToPostgresTableUtil.java # Create PostgreSQL tables from Parquet
│   │   └── resources/
│   │       ├── application.properties
│   │       └── *.parquet                     # Parquet data files
│   └── test/
│       ├── java/com/example/
│       │   ├── OpenSearchClientExample.java  # OpenSearch connection example
│       │   ├── EdwinTest.java                # OpenSearch integration tests
│       │   ├── GreenTripDataIndexTest.java   # Create OpenSearch index from Parquet
│       │   ├── ParquetToPostgresTableTest.java # PostgreSQL table creation tests
│       │   ├── FhvTripDataSchemaTest.java    # Schema validation tests
│       │   ├── FhvTripDataTest.java          # FHV trip data tests
│       │   ├── ParquetReaderTest.java        # Parquet reader tests
│       │   └── AppTest.java                  # Application tests
│       └── resources/
│           ├── test.properties
│           ├── truststore.jks                # SSL truststore for OpenSearch
│           └── docker-compose.yml            # Test docker-compose configuration
├── certs/                                    # SSL certificates directory
│   ├── opensearch-keystore.jks              # OpenSearch SSL keystore
│   └── truststore.jks                        # SSL truststore
├── docker-compose.yml                        # Docker services configuration (standard)
├── docker-compose-ssl-2.yaml                 # Docker services with SSL/TLS enabled
├── pom.xml                                   # Maven project configuration
└── README.md                                 # This file
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
mvn test -Dtest=OpenSearchClientExample#testConnection
mvn test -Dtest=GreenTripDataIndexTest#testCreateIndexFromParquetSchema
mvn test -Dtest=ParquetToPostgresTableTest#testCreateTableFromParquetFile
```

### Test Classes

- **OpenSearchClientExample**: Example of connecting to OpenSearch with SSL/TLS
- **EdwinTest**: OpenSearch integration tests with SSL/TLS
- **GreenTripDataIndexTest**: Creates OpenSearch index from Parquet schema
- **ParquetToPostgresTableTest**: Creates PostgreSQL tables from Parquet schemas
- **FhvTripDataSchemaTest**: Tests for displaying and validating schema of `fhv_tripdata_2025-01.parquet`
- **FhvTripDataTest**: Comprehensive tests for reading `fhv_tripdata_2025-01.parquet` file
- **ParquetReaderTest**: Tests for reading `fhvhv_tripdata_2025-01.parquet` file
- **AppTest**: Basic unit test for the App class

### Running OpenSearch Tests with SSL

To run tests that connect to OpenSearch with SSL/TLS:

```bash
# 1. Start OpenSearch with SSL (if not already running)
docker-compose -f docker-compose-ssl-2.yaml up -d opensearch-node1

# 2. Wait for OpenSearch to be ready (check logs)
docker-compose -f docker-compose-ssl-2.yaml logs -f opensearch-node1

# 3. Run the test
mvn test -Dtest=OpenSearchClientExample#testConnection
mvn test -Dtest=GreenTripDataIndexTest#testCreateIndexFromParquetSchema
```

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

## Metrics and Monitoring

Both `ParquetToOpenSearchUtil` and `ParquetToPostgresTableUtil` include Micrometer metrics integration for Prometheus monitoring.

### Metrics Exposed

**ParquetToOpenSearchUtil Metrics:**
- `opensearch.parquet.files.loaded` - Counter for number of files loaded
- `opensearch.parquet.entries.loaded` - Counter for number of documents indexed

**ParquetToPostgresTableUtil Metrics:**
- `parquet.files.loaded` - Counter for number of files processed
- `parquet.table.entries.loaded` - Counter for number of rows loaded

### Accessing Metrics

```java
// Get metrics summary
System.out.println(ParquetToOpenSearchUtil.Metrics.getSummary());
System.out.println(ParquetToPostgresTableUtil.Metrics.getSummary());

// Get Prometheus scrape format (for HTTP endpoint)
String metrics = ParquetToOpenSearchUtil.Metrics.scrape();
// Expose this via HTTP endpoint: GET /metrics
```

### Exposing Metrics for Prometheus

To expose metrics for Prometheus scraping, you can create a simple HTTP endpoint:

```java
// Example: Expose metrics endpoint
String prometheusMetrics = ParquetToOpenSearchUtil.Metrics.scrape();
// Return this in HTTP response at /metrics endpoint
```

Prometheus can then scrape this endpoint and visualize metrics in Grafana.

### Metrics Format

Metrics are exposed in Prometheus format:
```
# HELP opensearch_parquet_files_loaded Total number of Parquet files loaded into OpenSearch
# TYPE opensearch_parquet_files_loaded counter
opensearch_parquet_files_loaded{component="parquet_to_opensearch"} 1.0

# HELP opensearch_parquet_entries_loaded Total number of entries (documents) loaded into OpenSearch from Parquet files
# TYPE opensearch_parquet_entries_loaded counter
opensearch_parquet_entries_loaded{component="parquet_to_opensearch"} 100000.0
```

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

## Examples

### Example 1: Load Parquet File to OpenSearch

```java
import com.example.ParquetToOpenSearchUtil;

public class LoadToOpenSearchExample {
    public static void main(String[] args) throws Exception {
        String truststorePath = System.getProperty("user.dir") + "/src/test/resources/truststore.jks";
        
        long docsIndexed = ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(
            "src/main/resources/green_tripdata_2025-01.parquet",
            "green_tripdata_index",
            truststorePath,
            true,  // drop if exists
            -1     // load all records
        );
        
        System.out.println("Indexed " + docsIndexed + " documents");
        System.out.println(ParquetToOpenSearchUtil.Metrics.getSummary());
    }
}
```

### Example 2: Create PostgreSQL Table from Parquet Schema

```java
import com.example.ParquetToPostgresTableUtil;

public class LoadToPostgresExample {
    public static void main(String[] args) throws Exception {
        // Create table and load data
        long rowsLoaded = ParquetToPostgresTableUtil.createTableAndLoadDataFromParquetFile(
            "src/main/resources/green_tripdata_2025-01.parquet",
            "green_tripdata_2025_01",
            true,  // drop if exists
            -1     // load all rows
        );
        
        System.out.println("Loaded " + rowsLoaded + " rows");
        System.out.println(ParquetToPostgresTableUtil.Metrics.getSummary());
    }
}
```

### Example 3: Running Tests

```bash
# Test OpenSearch connection
mvn test -Dtest=OpenSearchClientExample#testConnection

# Create OpenSearch index from Parquet schema
mvn test -Dtest=GreenTripDataIndexTest#testCreateIndexFromParquetSchema

# Create PostgreSQL table from Parquet schema
mvn test -Dtest=ParquetToPostgresTableTest#testCreateTableFromParquetFile
```

## License

This project is part of the AI Taxi playground.