# ai-taxi

AI Taxi Project for reading and processing Parquet files.

## Requirements

- **Parquet Version**: Apache Parquet version **1.15.0 or higher** is required.
  - This project currently uses Parquet version 1.15.2
  - Earlier versions may have compatibility issues with Jackson serialization when reading metadata

## Dependencies

- Java 21
- Apache Parquet 1.15.0+
- Apache Hadoop 3.3.6
- JUnit 5.10.0

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

### Test Requirements

- Parquet files must be present in `src/main/resources/` for the tests to run successfully:
  - `fhv_tripdata_2025-01.parquet`
  - `fhvhv_tripdata_2025-01.parquet`
  - `green_tripdata_2025-01.parquet`
  - `yellow_tripdata_2025-01.parquet`