# Open-Meteo Air Quality Ingestion with MongoDB

A personal learning and portfolio project for building a reliable Python ingestion layer with MongoDB.

The project retrieves hourly air-quality data from the Open-Meteo Air Quality API, validates the response structure, and stores the complete unmodified API payload in MongoDB. Each execution also creates a permanent run-summary document for observability and troubleshooting.

The ingestion layer is intentionally kept separate from the future PySpark transformation layer.

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

## Current ingestion flow

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
```

For every configured city, the script:

1. Builds an Open-Meteo request URL.
2. Fetches the API response.
3. Validates the hourly response structure.
4. Preserves the complete API payload without flattening it.
5. Inserts one raw document into MongoDB.
6. Tracks the success or failure of the city ingestion.

After all cities have been processed, the script inserts one run-summary document describing the complete execution.

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
mongodb-learning-project/
├── .github/
│   └── workflows/
│       └── ci.yml
├── config/
│   └── cities.json
├── src/
│   ├── __init__.py
│   ├── check_connection.py
│   ├── ingest_openmeteo.py
│   └── setup_indexes.py
├── tests/
│   └── test_ingest_openmeteo.py
├── .env.example
├── .gitignore
├── compose.yaml
├── README.md
└── requirements.txt
```

## Prerequisites

The local setup requires:

* Git
* Python 3.12
* Docker
* Docker Compose
* WSL2, Linux, or another compatible shell environment

## Clone the repository

```bash
git clone <https://github.com/ENikopoulos/mongodb-openmeteo-air-quality-pipeline>
cd mongodb-learning-project
```

Replace `<repository-url>` with the GitHub repository URL.

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

The validator checks structural usability only.

Measurement-quality rules, such as null handling, negative values, deduplication, and AQI-specific validation, are intentionally reserved for the future transformation layer.

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

The ingestion layer will be followed by a separate transformation stage.

Planned future work includes:

* Reading raw MongoDB documents with PySpark
* Flattening hourly arrays
* Casting and validating measurements
* Handling null and invalid values
* Deduplicating measurements
* Producing transformation-ready datasets
* Adding analytical queries or downstream visualisation

The PySpark transformation layer is intentionally outside the scope of the current ingestion implementation.
