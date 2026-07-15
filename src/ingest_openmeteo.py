import urllib.parse
import urllib.request
import json
from datetime import datetime, timezone
import uuid
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import time

# Requested hourly variables
HOURLY_VARIABLES = [
    "european_aqi",
    "pm10",
    "pm2_5",
    "nitrogen_dioxide"
]

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Cities list
CITIES = [
    {
        "city_id": 1,
        "city": "Athens",
        "country": "Greece",
        "latitude": 37.9838,
        "longitude": 23.7275
    },
    {
        
        "city_id": 2,
        "city": "Thessaloniki",
        "country": "Greece",
        "latitude": 40.6401,
        "longitude": 22.9444

    }
]

def build_url(city, hourly, base_prefix):
    """
    Builds the request url for the Open-Meteo API for hourly
    variables: european_aqi, pm10, pm2_5, nitrogen_dioxide.
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(hourly),
        "timezone": "UTC",
        "forecast_days": 1,
        "past_days": 2
    }
    return base_prefix + "?" + urllib.parse.urlencode(params, encoding="utf-8")

def fetch_response(request_url):
    """
    Fetches the Open-Meteo air_quality API response.
    """
    with urllib.request.urlopen(request_url, timeout=30) as response:
        response_body = response.read().decode("utf-8")
        api_response = json.loads(response_body)
    return api_response

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
            "timezone": "UTC",
            "forecast_days": 1,
            "past_days": 2
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

    MAX_ATTEMPTS = 3

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
        database = client["openmeteo_air_quality"]
        raw_collection = database["raw_responses"]
        ingestion_run_collection = database["ingestion_runs"]
        for city in CITIES:
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
                
                except Exception as error:
                    last_error = error
                    print(
                        f"Attempt {attempt} failed for "
                        f"{city['city']}: {error}"
                    )
                    if attempt < MAX_ATTEMPTS:
                        print(f"Retrying {city['city']}...")
                        time.sleep(2)
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

        if successful_ingestions == len(CITIES):
            run_status = "completed"
        elif successful_ingestions > 0 and successful_ingestions < len(CITIES):
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
                "cities_intended": len(CITIES),
                "successful_ingestions": successful_ingestions,
                "failed_ingestions": len(failed_cities),
            },

            "successful_cities": successful_cities,

            "failed_cities": failed_cities,

            "request_config": {
                "hourly_variables": HOURLY_VARIABLES,
                "past_days": 2,
                "forecast_days": 1,
                "timezone": "UTC"
            },
        }

        # Add run summary into ingestion_runs
        inserted_summary = ingestion_run_collection.insert_one(run_summary)
        print(f"Inserted run summary ID = {inserted_summary.inserted_id}")

        print("\nIngestion summary:")
        print(run_summary)

    finally:
        client.close()
    
if __name__ == "__main__":
    main()