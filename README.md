# Open-Meteo Air Quality Pipeline with MongoDB and PySpark

A personal learning and portfolio project for building an air-quality ingestion and transformation pipeline with Python, MongoDB, and PySpark.

The project retrieves hourly air-quality data from the Open-Meteo Air Quality API, validates the response structure, and stores each complete, unmodified API payload in MongoDB. Each ingestion execution also creates a permanent run-summary document for observability and troubleshooting.

PySpark reads the raw MongoDB documents through the MongoDB Spark Connector and transforms the nested hourly arrays into validated, deduplicated measurement rows. The Silver dataset is stored as Parquet with one row per city and valid measurement timestamp, while records with invalid timestamps are retained for data-quality investigation.

A second PySpark transformation creates a Gold daily summary with one row per city and date. It includes pollutant averages and maxima, daily completeness metrics, valid-measurement counts, data-quality flags, and the timestamp of the maximum European AQI.


## Project goals

This project is designed to practise:

* MongoDB document modelling
* PyMongo database operations
* API ingestion with Python
* Retry and exception handling
* Response validation
* MongoDB indexing
* Configuration management
* Unit testing with pytest
* Continuous integration with GitHub Actions
* Docker-based local development
* PySpark DataFrame transformations
* Integrating MongoDB with PySpark through the Spark Connector
* Array zipping and exploding
* Data-quality flags
* Window-based deduplication
* Silver and Gold data modelling
* Parquet persistence
* Read-back validation

## Pipeline architecture

```text
config/cities.json
        |
        v
Python ingestion script
        |
        +--> Open-Meteo Air Quality API
        |
        +--> Response structure validation
        |
        +--> MongoDB raw_responses
        |
        +--> MongoDB ingestion_runs

MongoDB raw_responses
        |
        v
PySpark Silver transformation
        |
        +--> Validate hourly array lengths
        +--> Zip and explode hourly arrays
        +--> Parse timestamps
        +--> Add measurement-validity flags
        +--> Deduplicate by city and timestamp
        |
        v
data/silver/air_quality_measurements
        |
        v
PySpark Gold transformation
        |
        +--> Aggregate measurements by city and date
        +--> Calculate daily averages and maxima
        +--> Calculate completeness and validity metrics
        +--> Identify the timestamp of maximum European AQI
        |
        v
data/gold/air_quality_summary
```

For every configured city, the ingestion script:

1. Builds an Open-Meteo request URL.
2. Fetches the API response.
3. Validates the hourly response structure.
4. Preserves the complete API payload without flattening it.
5. Inserts one raw document into MongoDB.
6. Tracks the success or failure of the city ingestion.

After all cities have been processed, the script inserts one run-summary document describing the complete execution.

The Silver transformation converts the raw hourly arrays into measurement-level rows, applies data-quality flags, and keeps the most recently ingested record for each valid `city_id + measurement_timestamp` key.

The Gold transformation aggregates the Silver measurements into one daily row per `city_id + measurement_date`.


## Current features

* Local MongoDB environment using Docker Compose
* Secure credentials loaded from `.env`
* External city configuration stored in JSON
* Multi-city ingestion
* Shared UUID `run_id` for each execution
* One raw MongoDB document per city per run
* One permanent run-summary document per execution
* BSON UTC timestamps
* Retry handling for temporary network and server failures
* Immediate failure for non-retryable HTTP errors
* Separate handling for MongoDB errors
* Lightweight Open-Meteo response validation
* Repeatable MongoDB index setup
* Focused pytest unit tests
* GitHub Actions CI for pushes and pull requests
* Reading raw MongoDB documents with the MongoDB Spark Connector
* Silver Parquet output with one row per city and valid measurement timestamp
* Gold Parquet output with one daily row per city
* Parquet row-count, schema, business-key, and duplicate-key validation

## Data source

The project uses the Open-Meteo Air Quality API.

Current hourly variables:

* `european_aqi`
* `pm10`
* `pm2_5`
* `nitrogen_dioxide`

Current request configuration:

* Two past days
* One forecast day
* UTC timezone

Current cities:

* Athens, Greece
* Thessaloniki, Greece

The city configuration can be changed in:

```text
config/cities.json
```

## Project structure

```text
mongodb-openmeteo-air-quality-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml
├── config/
│   └── cities.json
├── src/
│   ├── __init__.py
│   ├── check_connection.py
│   ├── ingest_openmeteo.py
│   ├── transform_air_quality.py
│   ├── create_gold_daily_summary.py
│   └── setup_indexes.py
├── tests/
│   └── test_ingest_openmeteo.py
├── .env.example
├── .gitignore
├── compose.yaml
├── README.md
└── requirements.txt
```

### Generated data

Generated locally and excluded from Git:

```text
data/silver/air_quality_measurements
data/gold/air_quality_summary
```

## Prerequisites

The local setup requires:

* Git
* Python 3.12
* Docker
* Docker Compose
* WSL2, Linux, or another compatible shell environment
* Java 17
* Apache Spark/PySpark
* MongoDB Spark Connector package

## Clone the repository

```bash
git clone https://github.com/ENikopoulos/mongodb-openmeteo-air-quality-pipeline
cd mongodb-openmeteo-air-quality-pipeline
```

## Create the Python environment

Create a virtual environment:

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install the dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Environment variables

MongoDB credentials are loaded from a `.env` file in the project root.

Create the local file from the provided example:

```bash
cp .env.example .env
```

Open `.env` and replace the placeholder values:

```env
MONGO_ROOT_USERNAME=your_local_username
MONGO_ROOT_PASSWORD=your_local_password
```

The `.env` file must remain private and must not be committed to Git.

The repository contains `.env.example` only to document the required variable names:

```env
MONGO_ROOT_USERNAME=replace_with_username
MONGO_ROOT_PASSWORD=replace_with_password
```

Do not put real credentials in `.env.example`.

## Start MongoDB

Start the MongoDB container:

```bash
docker compose up -d
```

Check the container status:

```bash
docker compose ps
```

## Verify the MongoDB connection

Run:

```bash
python src/check_connection.py
```

Expected output:

```text
Successful Connection
```

## Create or confirm MongoDB indexes

Run the repeatable index setup script:

```bash
python src/setup_indexes.py
```

The script creates or confirms the following indexes.

### `ingestion_runs`

```text
run_id ascending, unique
run_completed_at_utc descending
```

### `raw_responses`

```text
run_id ascending
city.city_id ascending + ingestion_metadata.ingested_at_utc descending
```

The setup script can safely be run again when the same index definitions already exist.

## Run the ingestion

Run the ingestion script from the repository root:

```bash
python src/ingest_openmeteo.py
```

A successful run should:

1. Load the cities from `config/cities.json`.
2. Fetch and validate one response per city.
3. Insert one document per city into `raw_responses`.
4. Insert one summary document into `ingestion_runs`.
5. Print the run status and counts.

Example terminal result:

```text
Run id: <uuid>
Fetched and inserted Athens
Fetched and inserted Thessaloniki
Inserted run summary ID = <object-id>

status: completed
successful_ingestions: 2
failed_ingestions: 0
```

## Run the Silver transformation

The Silver transformation reads the raw API documents from the MongoDB `raw_responses` collection through the MongoDB Spark Connector.

Run it from the repository root:

```bash
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 \
  src/transform_air_quality.py
```

The transformation:

1. Reads the raw MongoDB documents.
2. Selects the city, ingestion, and hourly measurement fields.
3. Validates that the timestamp and pollutant arrays contain the expected number of elements.
4. Uses `arrays_zip()` to align each timestamp with its pollutant measurements.
5. Explodes the arrays into individual measurement rows.
6. Parses the measurement timestamp.
7. Creates validity flags for timestamps and pollutant values.
8. Identifies duplicate `city_id + measurement_timestamp` keys.
9. Keeps the most recently ingested valid record for each key.
10. Retains invalid-timestamp records for data-quality investigation.
11. Writes the resulting Silver dataset as Parquet.
12. Reads the Parquet output back and validates its row count and duplicate-key status.

Silver output:

```text
data/silver/air_quality_measurements
```

The Silver grain is:

```text
One row per city_id and valid measurement_timestamp,
plus retained invalid-timestamp records for investigation.
```

The normal validation output includes:

* Hourly array-length validation
* Measurement-validity summary
* Pre-deduplication summary
* Post-deduplication duplicate-key validation
* Parquet read-back row-count validation
* Persisted duplicate-key validation

## Run the Gold transformation

The Gold transformation reads the Silver Parquet dataset and produces one daily summary row per city.

Run it from the repository root:

```bash
spark-submit src/create_gold_daily_summary.py
```

The transformation:

1. Reads the Silver Parquet dataset.
2. Derives `measurement_date` from `measurement_timestamp`.
3. Filters out rows with invalid measurement timestamps.
4. Aggregates measurements by `city_id + measurement_date`.
5. Calculates average and maximum values for:

   * European AQI
   * PM10
   * PM2.5
   * Nitrogen dioxide
6. Counts observed distinct hours.
7. Counts valid daily measurements for each pollutant.
8. Calculates the daily completeness percentage.
9. Creates `is_complete_day` and `is_fully_valid_day` flags.
10. Identifies the earliest timestamp at which the maximum daily European AQI occurred.
11. Writes the Gold result as Parquet.
12. Reads the output back and validates its row count, schema, business keys, and daily grain.

Gold output:

```text
data/gold/air_quality_summary
```

The Gold grain is:

```text
One row per city_id and measurement_date.
```

The Gold read-back checks validate that:

* Written and read-back row counts match.
* Column names and data types match.
* `city_id` and `measurement_date` are non-null.
* No duplicate city-date keys exist.


## Error and retry behaviour

The ingestion script distinguishes between several failure types.

### Retryable HTTP errors

The script retries:

* HTTP `429`
* HTTP `500` to `599`

Each city can be attempted up to three times.

### Retryable network errors

Temporary connection and timeout errors are retried.

### Non-retryable HTTP errors

Most other HTTP client errors, such as HTTP `400`, stop after one attempt.

### Validation errors

Structurally invalid API responses stop after one attempt and are not inserted into `raw_responses`.

### MongoDB errors

MongoDB insertion failures are captured separately from API failures.

A failed run-summary insertion is reported in the terminal along with the associated `run_id`.

## Response validation

Before insertion, the script verifies that:

* The decoded response is a dictionary.
* The API did not return an error indicator.
* The `hourly` section exists and is a dictionary.
* `hourly.time` is a non-empty list.
* Every requested variable exists.
* Every requested variable is represented as a list.
* Every requested variable has the same number of elements as the timestamp list.

The ingestion validator checks structural usability only.

Measurement-level quality rules are applied in the Silver transformation. These include timestamp parsing, null and negative-value checks, hourly array-length validation, validity flags, and deduplication by city and measurement timestamp.

Daily completeness and metric-validity rules are applied in the Gold transformation.

## MongoDB collections

The database name is:

```text
openmeteo_air_quality
```

### `raw_responses`

Contains one document per city per ingestion run.

Simplified structure:

```javascript
{
  "_id": ObjectId("..."),
  "run_id": "<uuid>",

  "city": {
    "city_id": 1,
    "city": "Athens",
    "country": "Greece",
    "requested_latitude": 37.9838,
    "requested_longitude": 23.7275
  },

  "request": {
    "endpoint": "...",
    "hourly_variables": [
      "european_aqi",
      "pm10",
      "pm2_5",
      "nitrogen_dioxide"
    ],
    "timezone": "UTC",
    "forecast_days": 1,
    "past_days": 2
  },

  "ingestion_metadata": {
    "run_started_at_utc": ISODate("..."),
    "ingested_at_utc": ISODate("..."),
    "source": "openmeteo",
    "status": "ingest_complete"
  },

  "raw_payload": {
    "...": "complete Open-Meteo response"
  }
}
```

### `ingestion_runs`

Contains one summary document per ingestion execution.

Simplified structure:

```javascript
{
  "_id": ObjectId("..."),
  "run_id": "<uuid>",
  "run_started_at_utc": ISODate("..."),
  "run_completed_at_utc": ISODate("..."),
  "status": "completed",

  "counts": {
    "cities_intended": 2,
    "successful_ingestions": 2,
    "failed_ingestions": 0
  },

  "successful_cities": [
    {
      "city_id": 1,
      "city": "Athens",
      "attempts_made": 1
    }
  ],

  "failed_cities": [],

  "request_config": {
    "hourly_variables": [
      "european_aqi",
      "pm10",
      "pm2_5",
      "nitrogen_dioxide"
    ],
    "past_days": 2,
    "forecast_days": 1,
    "timezone": "UTC"
  }
}
```

Possible run-status values:

```text
completed
partial_failure
failed
```

## Access MongoDB with mongosh

Open `mongosh` inside the MongoDB container:

```bash
docker compose exec mongodb mongosh \
  --username admin \
  --authenticationDatabase admin \
  --password
```

The command prompts for the password rather than placing it directly in the shell history.

Replace `admin` if a different username is configured in `.env`.

Select the project database:

```javascript
use openmeteo_air_quality
```

List collections:

```javascript
show collections
```

## Inspect the newest ingestion run

```javascript
db.ingestion_runs
  .find()
  .sort({ run_completed_at_utc: -1 })
  .limit(1)
```

## Find raw responses for one run

```javascript
db.raw_responses.find(
  {
    run_id: "<run-id>"
  },
  {
    run_id: 1,
    "city.city_id": 1,
    "city.city": 1,
    "ingestion_metadata.ingested_at_utc": 1
  }
)
```

## Count raw responses for one run

```javascript
db.raw_responses.countDocuments({
  run_id: "<run-id>"
})
```

For a successful run with two configured cities, the expected result is:

```text
2
```

## View indexes

```javascript
db.raw_responses.getIndexes()
```

```javascript
db.ingestion_runs.getIndexes()
```

## Exit mongosh

```javascript
exit
```

## Run the unit tests

Run all tests from the repository root:

```bash
python -m pytest -q
```

The current focused tests cover:

* Open-Meteo URL construction
* Valid API-response validation
* Missing hourly variables
* Mismatched hourly-array lengths
* Valid city configuration loading
* Empty city configuration
* Invalid top-level city configuration
* Ingestion run-status determination
* Run-summary document construction
* Terminal run-summary output
* Raw MongoDB document construction

The unit tests use controlled fake inputs and temporary files. They do not require:

* A live Open-Meteo request
* A running MongoDB container
* Real credentials

## Continuous integration

The GitHub Actions workflow is stored in:

```text
.github/workflows/ci.yml
```

It runs on:

* Pushes
* Pull requests

The workflow:

1. Checks out the repository.
2. Sets up Python 3.12.
3. Installs the pinned dependencies.
4. Runs:

```bash
python -m pytest -q
```

MongoDB and Docker services are not required for the current CI workflow because the tests are unit tests rather than integration tests.

## Stop the local environment

Stop the running MongoDB container:

```bash
docker compose stop
```

Stop and remove the container and network:

```bash
docker compose down
```

The MongoDB named volume remains available unless volumes are explicitly removed.

Avoid using the following command unless the stored local data should also be deleted:

```bash
docker compose down -v
```

## Future work

Possible next improvements include:

* Adding focused unit tests for the Silver and Gold transformation helpers
* Creating a separate rejected-record output for invalid timestamps
* Making the expected ingestion window configurable across ingestion and transformation scripts
* Adding analytical queries or a downstream visualisation
* Adding transformation checks to CI without requiring a live MongoDB instance
* Expanding the city configuration or supported air-quality measurements
