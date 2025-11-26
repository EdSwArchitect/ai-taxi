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

## Package Structure

The project uses the package structure `com.bscllc.ai.taxi.*`:

- **`com.bscllc.ai.taxi.model.*`** - Data model classes (record classes for taxi trip data)
  - `GreenTrip` - Record class for Green taxi trip data with JSON annotations
  - `YellowTrip` - Record class for Yellow taxi trip data with JSON annotations
  
- **`com.bscllc.ai.taxi.utils.*`** - Utility classes for Parquet file processing and database operations
  - `ParquetFileReaderUtil` - Parquet file reading utility
  - `ParquetToOpenSearchUtil` - Load Parquet files to OpenSearch
  - `ParquetToPostgresTableUtil` - Create PostgreSQL tables from Parquet schemas
  - `DatabaseTableFromParquetUtil` - Generic database table creation utility

- **`com.bscllc.ai.taxi.metrics.*`** - Metrics and monitoring components
  - `MetricsServer` - HTTP server for exposing Prometheus metrics

All test classes follow the same package structure under `src/test/java/com/bscllc/ai/taxi/`.

## Dependencies

- Java 21
- Apache Parquet 1.15.0+
- Apache Hadoop 3.3.6
- JUnit 5.10.0
- OpenSearch Java Client 3.3.0
- PostgreSQL JDBC Driver 42.7.1
- Micrometer Core 1.13.0 (for metrics)
- Micrometer Registry Prometheus 1.13.0 (for Prometheus integration)
- Jackson Databind 2.15.2 (for JSON serialization)
- Jackson Datatype JSR310 2.15.2 (for Java 8 Time API support)
- Testcontainers 1.19.3 (for integration testing)
  - Testcontainers Core
  - Testcontainers JUnit Jupiter integration
  - Testcontainers PostgreSQL module

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
│   │   ├── java/com/bscllc/ai/taxi/
│   │   │   ├── model/
│   │   │   │   ├── App.java                  # Main application class
│   │   │   │   ├── GreenTrip.java            # Record class for Green taxi trip data
│   │   │   │   ├── YellowTrip.java           # Record class for Yellow taxi trip data
│   │   │   │   └── ParquetReaderEdwin.java   # Alternative Parquet reader
│   │   │   ├── utils/
│   │   │   │   ├── ParquetFileReaderUtil.java      # Parquet file reading utility
│   │   │   │   ├── ParquetToOpenSearchUtil.java    # Load Parquet files to OpenSearch
│   │   │   │   ├── ParquetToPostgresTableUtil.java # Create PostgreSQL tables from Parquet
│   │   │   │   └── DatabaseTableFromParquetUtil.java # Generic database table creation utility
│   │   │   └── metrics/
│   │   │       └── MetricsServer.java        # HTTP server for Prometheus metrics
│   │   └── resources/
│   │       ├── application.properties
│   │       └── *.parquet                     # Parquet data files
│   └── test/
│       ├── java/com/bscllc/ai/taxi/
│       │   ├── OpenSearchClientExample.java  # OpenSearch connection example
│       │   ├── EdwinTest.java                # OpenSearch integration tests
│       │   ├── GreenTripDataIndexTest.java   # Create OpenSearch index from Parquet
│       │   ├── ParquetToPostgresTableTest.java # PostgreSQL table creation tests
│       │   ├── FhvTripDataSchemaTest.java    # Schema validation tests
│       │   ├── FhvTripDataTest.java          # FHV trip data tests
│       │   ├── ParquetReaderTest.java        # Parquet reader tests
│       │   ├── model/
│       │   │   ├── GreenTripTest.java        # Tests for GreenTrip record class
│       │   │   └── YellowTripTest.java       # Tests for YellowTrip record class
│       │   ├── metrics/
│       │   │   └── MetricsServerTest.java    # Tests for MetricsServer
│       │   └── utils/
│       │       ├── ParquetToOpenSearchUtilTest.java    # Testcontainers tests for OpenSearch utility
│       │       ├── ParquetToPostgresTableUtilTest.java # Testcontainers tests for PostgreSQL utility
│       │       └── DatabaseTableFromParquetUtilTest.java # Testcontainers tests for database table utility
│       │   └── utils/
│       │       ├── ParquetToOpenSearchUtilTest.java    # Testcontainers tests for OpenSearch utility
│       │       ├── ParquetToPostgresTableUtilTest.java # Testcontainers tests for PostgreSQL utility
│       │       └── DatabaseTableFromParquetUtilTest.java # Testcontainers tests for database table utility
│       └── resources/
│           ├── test.properties
│           ├── truststore.jks                # SSL truststore for OpenSearch
│           └── docker-compose.yml            # Test docker-compose configuration
├── certs/                                    # SSL certificates directory
│   ├── opensearch-keystore.jks              # OpenSearch SSL keystore
│   └── truststore.jks                        # SSL truststore
├── docker-compose.yml                        # Docker services configuration (standard)
├── docker-compose-ssl-2.yaml                 # Docker services with SSL/TLS enabled
├── prometheus/                               # Prometheus configuration
│   └── prometheus.yml                        # Prometheus scrape configuration
├── grafana/                                  # Grafana provisioning
│   └── provisioning/                         # Auto-provisioning configs
│       ├── datasources/
│       │   └── prometheus.yml                # Prometheus datasource config
│       └── dashboards/                       # Dashboard definitions
├── pom.xml                                   # Maven project configuration
└── README.md                                 # This file
```

### Package Organization

The project uses the package structure `com.bscllc.ai.taxi` with the following organization:

- **`model/`** - Data model classes (record classes for taxi trip data)
- **`utils/`** - Utility classes for Parquet file processing and database operations
- **`metrics/`** - Metrics and monitoring components

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

#### Integration Tests
- **OpenSearchClientExample**: Example of connecting to OpenSearch with SSL/TLS
- **EdwinTest**: OpenSearch integration tests with SSL/TLS
- **GreenTripDataIndexTest**: Creates OpenSearch index from Parquet schema
- **ParquetToPostgresTableTest**: Creates PostgreSQL tables from Parquet schemas
- **FhvTripDataSchemaTest**: Tests for displaying and validating schema of `fhv_tripdata_2025-01.parquet`
- **FhvTripDataTest**: Comprehensive tests for reading `fhv_tripdata_2025-01.parquet` file
- **ParquetReaderTest**: Tests for reading `fhvhv_tripdata_2025-01.parquet` file

#### Testcontainers Integration Tests

These test classes use Testcontainers to create isolated, containerized environments for testing utilities:

- **ParquetToOpenSearchUtilTest**: Integration tests for `ParquetToOpenSearchUtil` using isolated OpenSearch containers
  - Tests index creation from Parquet schema
  - Tests metrics tracking
  - Tests zero records handling
  - Tests index structure verification
  - Uses GenericContainer with OpenSearch 3.3.0 image

- **ParquetToPostgresTableUtilTest**: Integration tests for `ParquetToPostgresTableUtil` using isolated PostgreSQL containers
  - Tests table creation from Parquet schema
  - Tests schema creation if it doesn't exist
  - Tests table existence checks
  - Tests metrics tracking
  - Tests data loading functionality
  - Uses PostgreSQLContainer with PostgreSQL 15-alpine image

- **DatabaseTableFromParquetUtilTest**: Integration tests for `DatabaseTableFromParquetUtil` using isolated PostgreSQL containers
  - Tests table creation with JDBC parameters
  - Tests schema creation
  - Tests safe table creation (no dropping)
  - Tests column verification against Parquet schema
  - Uses PostgreSQLContainer with PostgreSQL 15-alpine image

**Note**: All Testcontainers tests automatically spin up isolated containers for each test run, ensuring clean, reproducible test environments without requiring manual Docker setup.

#### Model Tests
- **GreenTripTest**: Comprehensive tests for `GreenTrip` record class including JSON serialization/deserialization, record creation, and edge cases (11 test methods)
- **YellowTripTest**: Comprehensive tests for `YellowTrip` record class including JSON serialization/deserialization, record creation, and edge cases (12 test methods)

#### Metrics Tests
- **MetricsServerTest**: Tests for `MetricsServer` including server lifecycle, HTTP endpoints (`/metrics`, `/health`), error handling, and concurrent request handling (14 test methods)

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

- **Docker**: Required for running Testcontainers integration tests (Testcontainers uses Docker to spin up isolated containers)
  - Ensure Docker is installed and running on your machine
  - OpenSearch tests automatically start an OpenSearch container using Testcontainers
  - PostgreSQL tests automatically start a PostgreSQL container using Testcontainers
  - Containers are isolated per test run, ensuring clean test environments
  - Containers are automatically cleaned up after tests complete

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

### MetricsServer

A simple HTTP server (`MetricsServer`) is provided to expose Prometheus metrics for scraping. It combines metrics from both utility classes and serves them at `/metrics` endpoint.

**Starting the Metrics Server:**

```java
<<<<<<< HEAD
import com.example.MetricsServer;
=======
import com.bscllc.ai.taxi.metrics.MetricsServer;
>>>>>>> 813481c (status)

public class Main {
    public static void main(String[] args) throws Exception {
        // Start metrics server on port 8080 (default)
        MetricsServer metricsServer = MetricsServer.start();
        
        // Or specify a custom port
        // MetricsServer metricsServer = MetricsServer.start(9091);
        
        // Your application code here...
        // Load data, process files, etc.
        
        // Metrics will be available at http://localhost:8080/metrics
        
        // Keep server running or stop it when done
        // metricsServer.stop();
    }
}
```

**Running as Standalone:**

```bash
# Run the metrics server standalone
<<<<<<< HEAD
java -cp target/ai-taxi-1.0-SNAPSHOT.jar com.example.MetricsServer 8080
=======
java -cp target/ai-taxi-1.0-SNAPSHOT.jar com.bscllc.ai.taxi.metrics.MetricsServer 8080
>>>>>>> 813481c (status)
```

**Endpoints:**
- `http://localhost:8080/metrics` - Prometheus metrics endpoint
- `http://localhost:8080/health` - Health check endpoint

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

The `MetricsServer` class provides an HTTP endpoint for Prometheus to scrape metrics automatically. The server combines metrics from both utility classes.

**Using MetricsServer in your application:**

```java
import com.bscllc.ai.taxi.metrics.MetricsServer;
import com.bscllc.ai.taxi.utils.ParquetToOpenSearchUtil;

public class Main {
    public static void main(String[] args) throws Exception {
        // Start metrics server (will be available at http://localhost:8080/metrics)
        MetricsServer metricsServer = MetricsServer.start(8080);
        
        // Your application code - metrics are automatically tracked
        ParquetToOpenSearchUtil.loadParquetFileToOpenSearch(...);
        
        // Metrics are now available at http://localhost:8080/metrics
        // Prometheus can scrape from this endpoint
        
        // Keep server running for Prometheus to scrape
        Thread.currentThread().join();
    }
}
```

**Configure Prometheus to scrape:**

Update `prometheus/prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'ai-taxi'
    static_configs:
      - targets: ['host.docker.internal:8080']  # Your metrics server
```

Prometheus will scrape metrics from `http://host.docker.internal:8080/metrics` and visualize them in Grafana dashboards.

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

## Data Models

### GreenTrip

Java record class representing Green Taxi trip data from NYC green_tripdata Parquet files.

**Location**: `com.bscllc.ai.taxi.model.GreenTrip`

**Features**:
- Annotated for JSON serialization/deserialization using Jackson
- 20 fields including vendor ID, pickup/dropoff times, passenger count, trip distance, fare information, etc.
- Uses `lpep_pickup_datetime` and `lpep_dropoff_datetime` for timestamps
- All fields are nullable to handle missing data

**Usage**:
```java
import com.bscllc.ai.taxi.model.GreenTrip;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new JavaTimeModule());

// Read from JSON
GreenTrip trip = mapper.readValue(jsonString, GreenTrip.class);

// Write to JSON
String json = mapper.writeValueAsString(trip);
```

### YellowTrip

Java record class representing Yellow Taxi trip data from NYC yellow_tripdata Parquet files.

**Location**: `com.bscllc.ai.taxi.model.YellowTrip`

**Features**:
- Annotated for JSON serialization/deserialization using Jackson
- 19 fields including vendor ID, pickup/dropoff times, passenger count, trip distance, fare information, etc.
- Uses `tpep_pickup_datetime` and `tpep_dropoff_datetime` for timestamps (TPEP = Taxi & Limousine Commission Passenger Enhancement Program)
- All fields are nullable to handle missing data

**Usage**:
```java
import com.bscllc.ai.taxi.model.YellowTrip;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new JavaTimeModule());

// Read from JSON
YellowTrip trip = mapper.readValue(jsonString, YellowTrip.class);

// Write to JSON
String json = mapper.writeValueAsString(trip);
```

## Utility Classes

### DatabaseTableFromParquetUtil

Generic utility class to create a database table from a Parquet file schema.

**Location**: `com.bscllc.ai.taxi.utils.DatabaseTableFromParquetUtil`

**Features**:
- Creates database schema if it doesn't exist
- Creates table only if it doesn't exist (safe, idempotent operation)
- Supports any JDBC-compatible database
- Automatic type conversion from Parquet to database types

**Usage**:
```java
import com.bscllc.ai.taxi.utils.DatabaseTableFromParquetUtil;

// Create table from Parquet schema
DatabaseTableFromParquetUtil.createTable(
    "jdbc:postgresql://localhost:5432/ai_taxi",
    "taxi",              // database schema
    "postgres",          // user
    "postgres",          // password
    "src/main/resources/green_tripdata_2025-01.parquet",
    "green_tripdata_2025_01",
    true  // drop if exists
);
```

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
import com.bscllc.ai.taxi.utils.ParquetToOpenSearchUtil;

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
import com.bscllc.ai.taxi.utils.ParquetToPostgresTableUtil;

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

### Example 3: Using Record Models with JSON

```java
import com.bscllc.ai.taxi.model.GreenTrip;
import com.bscllc.ai.taxi.utils.ParquetFileReaderUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ParseGreenTripExample {
    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        
        // Read Parquet data
        var records = ParquetFileReaderUtil.readParquetFile(
            "src/main/resources/green_tripdata_2025-01.parquet", 1
        );
        
        // Convert to JSON and parse as GreenTrip
        String json = mapper.writeValueAsString(records.get(0));
        GreenTrip trip = mapper.readValue(json, GreenTrip.class);
        
        System.out.println("Trip: " + trip);
        System.out.println("Pickup time: " + trip.lpepPickupDatetime());
        System.out.println("Distance: " + trip.tripDistance());
    }
}
```

### Example 4: Running Tests

```bash
# Test OpenSearch connection
mvn test -Dtest=OpenSearchClientExample#testConnection

# Create OpenSearch index from Parquet schema
mvn test -Dtest=GreenTripDataIndexTest#testCreateIndexFromParquetSchema

# Create PostgreSQL table from Parquet schema
mvn test -Dtest=ParquetToPostgresTableTest#testCreateTableFromParquetFile

# Test record model classes
mvn test -Dtest=GreenTripTest
mvn test -Dtest=YellowTripTest

# Test metrics server
mvn test -Dtest=MetricsServerTest

# Test utility classes with Testcontainers
mvn test -Dtest=ParquetToOpenSearchUtilTest
mvn test -Dtest=ParquetToPostgresTableUtilTest
mvn test -Dtest=DatabaseTableFromParquetUtilTest
```

## License

This project is part of the AI Taxi playground.
