# Imports
import json
from urllib.parse import parse_qs, urlparse

import pytest


from src.ingest_openmeteo import (
    AIR_QUALITY_URL,
    HOURLY_VARIABLES,
    API_TIMEZONE,
    PAST_DAYS,
    FORECAST_DAYS,
    build_url,
    validate_api_response,
    load_cities,
    determine_run_status
)


def test_build_url_contains_expected_endpoint_and_parameters():
    """Checks that build_url creates the expected Open-Meteo request URL."""
    # Arrange: Create city
    city = {
        "city_id": 1,
        "city": "Athens",
        "country": "Greece",
        "latitude": 37.9838,
        "longitude": 23.7275,
    }
    
    # Act: Building and parsing URL
    url = build_url(city, HOURLY_VARIABLES, AIR_QUALITY_URL)
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query)
    parsed_aqi_url = urlparse(AIR_QUALITY_URL)

    # Assert: parameters
    assert city["latitude"] == float(query["latitude"][0])

    assert city["longitude"] == float(query["longitude"][0])

    assert HOURLY_VARIABLES == query["hourly"][0].split(sep=",")

    assert API_TIMEZONE == query["timezone"][0]

    assert PAST_DAYS == int(query["past_days"][0])

    assert FORECAST_DAYS == int(query["forecast_days"][0])

    # Assert: endpoint
    assert parsed_url.scheme == parsed_aqi_url.scheme

    assert parsed_url.netloc == parsed_aqi_url.netloc

    assert parsed_url.path == parsed_aqi_url.path


# Validate API response
def test_validate_api_response_accepts_valid_response():
    """Checks that a structurally valid API response passes validation."""
    # Arrange: Build a test valid api response
    fake_api_response = {
        "hourly": {
            "time": [
                "2026-07-15T00:00",
                "2026-07-15T01:00",
            ],
            "european_aqi": [42, 45],
            "pm10": [18.2, 19.1],
            "pm2_5": [9.4, 10.0],
            "nitrogen_dioxide": [21.7, 23.3],
        }
    }

    # Act: Call validate function
    result = validate_api_response(
        fake_api_response,
        HOURLY_VARIABLES
    )

    # Assert: Result must be None for success
    assert result is None


# Failure-path test for API response validation
def test_validate_api_response_rejects_missing_variable():
    """Test API response for missing hourly variables."""
    # Arrange: Create a test invalid api response
    fake_invalid_api_response = {
        "hourly": {
            "time": [
                "2026-07-15T00:00",
                "2026-07-15T01:00",
            ],
            "european_aqi": [42, 45],
            "pm2_5": [9.4, 10.0],
            "nitrogen_dioxide": [21.7, 23.3],
        }
    }

    # Act and assert: validation should reject the missing variable
    with pytest.raises(
        ValueError,
        match="Missing hourly variables"
    ):
        validate_api_response(
            fake_invalid_api_response,
            HOURLY_VARIABLES
        )


# Test mismatched length
def test_validate_api_response_rejects_mismatched_lengths():
    # Arrange: Create a test invalid API response with mismatched length
    fake_api_mismatch_response = {
        "hourly": {
            "time": [
                "2026-07-15T00:00",
                "2026-07-15T01:00",
            ],
            "european_aqi": [42, 45],
            "pm10": [19.1],
            "pm2_5": [9.4, 10.0],
            "nitrogen_dioxide": [21.7, 23.3],
        }
    }

    # Act and assert: Call validation function and raise pytest error
    with pytest.raises(
        ValueError,
        match="Hourly variable 'pm10' has 1 values; expected 2"
    ):
        validate_api_response(
            fake_api_mismatch_response,
            HOURLY_VARIABLES
        )


# Test cities loading function
def test_load_cities_returns_valid_city_list(tmp_path):
    # Arrange: Config path, cities list and write temporary file
    config_path = tmp_path / "cities.json"
    cities_test_list = [
        {
            "city_id": 1,
            "city": "Athens",
            "country": "Greece",
            "latitude": 37.9838,
            "longitude": 23.7275,
        }
    ]
    with open(config_path, "w", encoding="utf-8") as file:
        json.dump(cities_test_list, file, indent=2)

    # Act: Call load cities function
    cities = load_cities(config_path)

    # Assert: Cities is equal to the created cities list, length is 1 and first name is "Athens"
    assert cities == cities_test_list
    assert len(cities) == 1
    assert cities[0]["city"] == "Athens"


# Test empty city list loading
def test_load_cities_from_empty_city_list(tmp_path):
    # Arrange: Config path, empty_list and write temporary empty json
    config_path = tmp_path / "cities.json"
    cities_empty_list = []

    with open(config_path, "w", encoding="utf-8") as file:
        json.dump(cities_empty_list, file, indent=2)
    
    # Act and assert: Load attempt for empty list
    with pytest.raises(
        ValueError,
        match="City configuration list must not be empty"
    ):
        load_cities(config_path)


# Test wrong top-level type
def test_load_cities_with_wrong_top_level_type(tmp_path):
    # Arrange: Config path, create wrong cities list, and load temporary json
    config_path = tmp_path / "cities.json"
    cities_wrong = {
        "city_id": 1,
        "city": "Athens"
    }
    with open(config_path, "w", encoding="utf-8") as file:
        json.dump(cities_wrong, file, indent=2)

    # Act and assert
    with pytest.raises(
        ValueError,
        match="City configuration must be a list"
    ):
        load_cities(config_path)


# Run status tests
def test_determine_run_status_success():
    # Arrange: all intended ingestions succeeded
    successful_ingestions = 12
    intended_ingestions = 12

    # Act: call the function and register the result
    result = determine_run_status(
        successful_ingestions,
        intended_ingestions
    )

    # Assert: check if the result is "completed"
    assert result == "completed"


def test_determine_run_status_partial_failure():
    # Arrange: one or more ingestions faied, but not all
    successful_ingestions = 1
    intended_ingestions = 12

    # Act call the function and register the result
    result = determine_run_status(
        successful_ingestions,
        intended_ingestions
    )

    # Assert check if the result is "partial_failure"
    assert result == "partial_failure"


def test_determine_run_status_failed():
    # Arrange: no successful ingestions
    successful_ingestions = 0
    intended_ingestions = 12

    # Act call the function and register the result
    result = determine_run_status(
        successful_ingestions,
        intended_ingestions
    )

    # Assert check if the result is "failed"
    assert result == "failed"
