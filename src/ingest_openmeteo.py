import urllib.parse
import urllib.request
import json
from datetime import datetime, timezone
import uuid
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import os
from dotenv import load_dotenv
import time
from urllib.error import HTTPError, URLError
from pathlib import Path

# API configuration
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

HOURLY_VARIABLES = [
    "european_aqi",
    "pm10",
    "pm2_5",
    "nitrogen_dioxide",
]

API_TIMEZONE = "UTC"
PAST_DAYS = 2
FORECAST_DAYS = 1
REQUEST_TIMEOUT_SECONDS = 30

# MongoDB configuration
DATABASE_NAME = "openmeteo_air_quality"
RAW_COLLECTION_NAME = "raw_responses"
INGESTION_RUNS_COLLECTION_NAME = "ingestion_runs"

# Retry configuration
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2

# Cities configuration
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CITIES_CONFIG_PATH = PROJECT_ROOT / "config" / "cities.json"

def load_cities(config_path):
    """
    Loads city configuration from a JSON file.

    Args:
        config_path: Path to the JSON file containing city definitions.

    Returns:
        A non-empty list of city configuration dictionaries.

    Raises:
        ValueError: If the JSON does not contain a non-empty list.
    """
    with open(config_path, "r", encoding="utf-8") as file:
        cities = json.load(file)

    if not isinstance(cities, list):
        raise ValueError("City configuration must be a list")

    if not cities:
        raise ValueError("City configuration list must not be empty")

    return cities

def build_url(city, hourly, base_prefix):
    """
    Builds the request url for the Open-Meteo API for hourly
    variables: european_aqi, pm10, pm2_5, nitrogen_dioxide.
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(hourly),
        "timezone": API_TIMEZONE,
        "forecast_days": FORECAST_DAYS,
        "past_days": PAST_DAYS
    }
    return base_prefix + "?" + urllib.parse.urlencode(params, encoding="utf-8")

def fetch_response(request_url):
    """
    Fetches the Open-Meteo air_quality API response.
    """
    with urllib.request.urlopen(
        request_url, 
        timeout=REQUEST_TIMEOUT_SECONDS
    ) as response:
        response_body = response.read().decode("utf-8")
        api_response = json.loads(response_body)
    return api_response

def validate_api_response(api_response, required_variables):
    """
    Validates the basic structure of an Open-Meteo air-quality API response.

    Confirms that the response contains a non-empty hourly timestamp list and
    that every requested hourly variable exists as a list with the same number
    of values as the timestamp list.

    Args:
        api_response: Parsed JSON response returned by the Open-Meteo API.
        required_variables: Hourly variable names expected in the response.

    Raises:
        ValueError: If the API reports an error or the response structure is
        missing, invalid, empty, or inconsistent.
    """
    if not isinstance(api_response, dict):
        raise ValueError("API response must be a dictionary")

    if api_response.get("error") is True:
        reason = api_response.get("reason", "Unknown API error")
        raise ValueError(f"API returned an error: {reason}")

    hourly = api_response.get("hourly")

    if not isinstance(hourly, dict):
        raise ValueError(
            "API response does not contain a valid hourly section"
        )

    time_values = hourly.get("time")

    if not isinstance(time_values, list) or not time_values:
        raise ValueError("hourly.time must be a non-empty list")

    missing_variables = set(required_variables) - set(hourly.keys())

    if missing_variables:
        raise ValueError(
            f"Missing hourly variables: {sorted(missing_variables)}"
        )

    expected_count = len(time_values)

    for variable in required_variables:
        variable_values = hourly[variable]

        if not isinstance(variable_values, list):
            raise ValueError(
                f"Hourly variable '{variable}' must be a list"
            )

        if len(variable_values) != expected_count:
            raise ValueError(
                f"Hourly variable '{variable}' has "
                f"{len(variable_values)} values; "
                f"expected {expected_count}"
            )
        
def build_raw_document(
        city, api_response, run_id, run_started_at_utc, ingested_at_utc
        ):
    """
    Creates a raw document containing the run, city, request,
    ingestion metadata, and unmodified API payload.

    Returns the document as a dictionary.
    """
    raw_document = {
        "run_id": run_id,

        "city": {
            "city_id": city["city_id"],
            "city": city["city"],
            "country": city["country"],
            "requested_latitude": city["latitude"],
            "requested_longitude": city["longitude"]
        },

        "request": {
            "endpoint": AIR_QUALITY_URL,
            "hourly_variables": HOURLY_VARIABLES,
            "timezone": API_TIMEZONE,
            "forecast_days": FORECAST_DAYS,
            "past_days": PAST_DAYS
        },

        "ingestion_metadata": {
            "run_started_at_utc": run_started_at_utc,
            "ingested_at_utc": ingested_at_utc,
            "source": "openmeteo",
            "status": "ingest_complete"
        },

        "raw_payload": api_response
    }

    return raw_document

def create_mongo_uri():
    """
    Creates the MongoDB connection URI, loads the secrets from .env 
    and returns the URI in str format.
    """
    # Get secret data
    load_dotenv()
    root_username = urllib.parse.quote_plus(os.environ["MONGO_ROOT_USERNAME"])
    root_password = urllib.parse.quote_plus(os.environ["MONGO_ROOT_PASSWORD"])

    # Build URI
    uri = f"mongodb://{root_username}:{root_password}@localhost:27017/?authSource=admin"

    return uri

def ingest_city(
        city,
        collection,
        run_id,
        run_started_at_utc
):
    """
    Fetches air-quality data for one city, builds the raw document,
    and inserts it into MongoDB.

    Args:
        city: Dictionary containing the city configuration.
        collection: MongoDB collection used for raw responses.
        run_id: Identifier shared by all cities in the ingestion run.
        run_started_at_utc: UTC timestamp when the run started.

    Returns:
        A dictionary containing the inserted MongoDB document ID
        and the city ingestion timestamp.
    """
    # Build request url
    request_url = build_url(city, HOURLY_VARIABLES, AIR_QUALITY_URL)

    # Fetch api response
    api_response = fetch_response(request_url)

    # Validate api response
    validate_api_response(
        api_response, 
        HOURLY_VARIABLES
    )
    
    ingested_at_utc = datetime.now(timezone.utc)

    # Build the raw document
    raw_document = build_raw_document(
        city,
        api_response,
        run_id,
        run_started_at_utc,
        ingested_at_utc
    )

    # Insert into MongoDB
    insert_result = collection.insert_one(raw_document)

    print(f"Inserted ID = {insert_result.inserted_id}")
    print(
        f"Fetched and inserted {city['city']} "
        f"at {ingested_at_utc}"
    )
    
    return {
        "inserted_id": insert_result.inserted_id,
        "ingested_at_utc": ingested_at_utc,
    }


    


def main():

    # Load cities
    cities = load_cities(CITIES_CONFIG_PATH)

    # Generate UUID run id
    run_id = str(uuid.uuid4())
    run_started_at_utc = datetime.now(timezone.utc)

    successful_ingestions = 0
    failed_cities = []
    successful_cities = []

    # Print run_id and start time
    print(
        f"Run id: {run_id}\n"
        f"Run started at: {run_started_at_utc}"
    )
    uri = create_mongo_uri()
    client = MongoClient(uri)
    try:
        database = client[DATABASE_NAME]
        raw_collection = database[RAW_COLLECTION_NAME]
        ingestion_run_collection = database[INGESTION_RUNS_COLLECTION_NAME]
        for city in cities:
            city_ingestion_succeeded = False
            last_error = None
            attempts_made = 0

            # Retry Loop
            for attempt in range(1, MAX_ATTEMPTS + 1):
                attempts_made = attempt

                try:
                    ingest_city(
                        city,
                        raw_collection,
                        run_id,
                        run_started_at_utc,
                    )

                    successful_ingestions += 1
                    city_ingestion_succeeded = True
                    break
                
                except HTTPError as error:
                    last_error = error
                    code = error.code

                    print(
                        f"HTTP request failed with status {code} for "
                        f"{city['city']}: {error.reason}"
                        )

                    if code == 429 or 500 <= code < 600:
                        if attempt < MAX_ATTEMPTS:
                            print(f"Retrying {city['city']}...")
                            time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        break

                except (URLError, TimeoutError) as error:
                    last_error = error

                    print(
                        f"Network error for {city['city']}: {error}"
                        )

                    if attempt < MAX_ATTEMPTS:
                        print(f"Retrying {city['city']}...")
                        time.sleep(RETRY_DELAY_SECONDS)

                except ValueError as error:
                    last_error = error
                    print(
                        f"Response validation failed for "
                        f"{city['city']}: {error}"
                    )
                    break

                except PyMongoError as error:
                    last_error = error
                    print(f"MongoDB operation failed for {city['city']}: {error}")
                    break

                except Exception as error:
                    last_error = error
                    print(f"Unexpected error for {city['city']}: {error}")
                    break
            if not city_ingestion_succeeded:
                failed_cities.append(
                    {
                        "city_id": city["city_id"],
                        "city": city["city"],
                        "error_message": str(last_error),
                        "attempts_made": attempts_made,
                    }
                )
            else:
                successful_cities.append(
                    {
                        "city_id": city["city_id"],
                        "city": city["city"],
                        "attempts_made": attempts_made,
                    }
                )

        # Run status

        if successful_ingestions == len(cities):
            run_status = "completed"
        elif successful_ingestions > 0:
            run_status = "partial_failure"
        else:
            run_status = "failed"
        
        # Summary
        run_completed_at_utc = datetime.now(timezone.utc)
        run_summary = {
            "run_id": run_id,

            "run_started_at_utc": run_started_at_utc,
            "run_completed_at_utc": run_completed_at_utc,

            "status": run_status,

            "counts": {
                "cities_intended": len(cities),
                "successful_ingestions": successful_ingestions,
                "failed_ingestions": len(failed_cities),
            },

            "successful_cities": successful_cities,

            "failed_cities": failed_cities,

            "request_config": {
                "hourly_variables": HOURLY_VARIABLES,
                "past_days": PAST_DAYS,
                "forecast_days": FORECAST_DAYS,
                "timezone": API_TIMEZONE
            },
        }

        # Add run summary into ingestion_runs
        try:
            inserted_summary = ingestion_run_collection.insert_one(run_summary)
            print(f"Inserted run summary ID = {inserted_summary.inserted_id}")

        except PyMongoError as error:
            print(
                f"Ingestion run summary could not be stored. "
                f"Run id: {run_id} "
                f"Database error: {error}"
                )
            
        print("\nIngestion summary:")
        print(run_summary)

    finally:
        client.close()
    
if __name__ == "__main__":
    main()