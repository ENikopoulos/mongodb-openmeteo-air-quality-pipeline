# Imports
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src.request_config import EXPECTED_MEASUREMENTS_PER_RESPONSE
from src.transform_air_quality import (
    add_measurement_timestamp,
    add_timestamp_validity_flag,
    add_measurement_validity_flags,
    deduplicate_measurements,
    find_duplicate_measurement_keys,
    validate_measurement_array_lengths,
)


# Tests

# Test add_measurement_timestamp validity
def test_add_measurement_timestamp(spark):
    # Arrange: create small DataFrame with one valid and one invalid timestamp
    test_data = [
        {
            "test_id": 1,
            "measurement_timestamp_raw": "2026-07-20T14:00"
        },

        {
            "test_id": 2,
            "measurement_timestamp_raw": "invalid-timestamp"
        }
    ]
    test_df = spark.createDataFrame(test_data)

    # Act: call function
    result_df = (
        add_measurement_timestamp(test_df)
        .withColumn(
            "measurement_timestamp_formatted",
            F.date_format(
                "measurement_timestamp",
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    # Assert: row count and value validity
    collected_rows = result_df.collect()

    assert len(collected_rows) == 2

    result_rows = {
        row["test_id"]: row
        for row in collected_rows
    }

    assert (
        result_rows[1]["measurement_timestamp_formatted"] == "2026-07-20 14:00:00"
    )

    assert result_rows[2]["measurement_timestamp_formatted"] is None

    assert result_rows[1]["measurement_timestamp_raw"] == "2026-07-20T14:00"


# Test timestamp validity flag function
def test_add_timestamp_validity_flag(spark):
    # Arrange: create a DataFrame covering valid and invalid timestamp combinations
    test_data = [
        {
            "test_id": 1,
            "measurement_timestamp_raw": "2026-07-20T14:00",
            "measurement_timestamp": datetime(2026, 7, 20, 14, 0),
        },

        {
            "test_id": 2,
            "measurement_timestamp_raw": "invalid_timestamp",
            "measurement_timestamp": None
        },

        {
            "test_id": 3,
            "measurement_timestamp_raw": None,
            "measurement_timestamp": datetime(2026, 7, 20, 14, 0)
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Act: call the function
    result_df = add_timestamp_validity_flag(test_df)

    # Assert: check for correct responses
    collected_rows = result_df.collect()

    assert len(collected_rows) == 3

    result_rows = {
        row["test_id"]: row
        for row in collected_rows
    }

    assert result_rows[1]["is_valid_measurement_timestamp"] is True

    assert result_rows[2]["is_valid_measurement_timestamp"] is False

    assert result_rows[3]["is_valid_measurement_timestamp"] is False


# Test measurement validity flags function
def test_add_measurement_validity_flags(spark):
    # Arrange: create a small DataFrame with values for eu_aqi, pm10, pm2_5 and nitrogen_dioxide
    test_data = [
        {
            "test_id": 1,
            "european_aqi": 42.0,
            "pm10": 18.5,
            "pm2_5": 9.2,
            "nitrogen_dioxide": 21.0
        },

        {
            "test_id": 2,
            "european_aqi": -1.0,
            "pm10": -0.5,
            "pm2_5": -2.0,
            "nitrogen_dioxide": -3.0
        },

        {
            "test_id": 3,
            "european_aqi": None,
            "pm10": None,
            "pm2_5": None,
            "nitrogen_dioxide": None
        },

        {
            "test_id": 4,
            "european_aqi": 0.0,
            "pm10": 0.0,
            "pm2_5": 0.0,
            "nitrogen_dioxide": 0.0,
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Act: call function
    result_df = add_measurement_validity_flags(test_df)

    # Assert: check for correct outputs
    collected_rows = result_df.collect()

    assert len(collected_rows) == 4

    result_rows = {
        row["test_id"]: row
        for row in collected_rows
    }

    expected_flags = {
        1: (True, True, True, True),
        2: (False, False, False, False),
        3: (False, False, False, False),
        4: (True, True, True, True),
    }

    for test_id, expected in expected_flags.items():
        actual = (
            result_rows[test_id]["is_valid_european_aqi"],
            result_rows[test_id]["is_valid_pm10"],
            result_rows[test_id]["is_valid_pm2_5"],
            result_rows[test_id]["is_valid_nitrogen_dioxide"],
        )

        assert actual == expected


# Test deduplication function
def test_deduplicate_measurements(spark):
    # Arrange: create small DataFrame with a duplicate business key
    test_data = [
        {
            "run_id": 1,
            "record_id": "old",
            "city_id": 1,
            "measurement_timestamp": datetime(2026, 7, 20, 14, 0),
            "ingested_at_utc": datetime(2026, 7, 20, 15, 0),
            "european_aqi": 40.0,
            "is_valid_measurement_timestamp": True
        },

        {
            "run_id": 2,
            "record_id": "new",
            "city_id": 1,
            "measurement_timestamp": datetime(2026, 7, 20, 14, 0),
            "ingested_at_utc": datetime(2026, 7, 20, 16, 0),
            "european_aqi": 55.0,
            "is_valid_measurement_timestamp": True
        },

        {
            "run_id": 2,
            "record_id": "separate",
            "city_id": 1,
            "measurement_timestamp": datetime(2026, 7, 20, 15, 0),
            "ingested_at_utc": datetime(2026, 7, 20, 16, 0),
            "european_aqi": 60.0,
            "is_valid_measurement_timestamp": True
        },
    ]

    test_df = spark.createDataFrame(test_data)

    # Act: call function
    result_df = deduplicate_measurements(test_df)

    # Assert: check for correct outputs
    collected_rows = result_df.collect()

    assert len(collected_rows) == 2

    result_rows = {
        row["record_id"]: row
        for row in collected_rows
    }

    assert set(result_rows.keys()) == {"new", "separate"}

    assert result_rows["new"]["european_aqi"] == 55.0

    assert find_duplicate_measurement_keys(result_df).count() == 0


# Test that invalid timestamps are retained
def test_deduplicate_measurements_retains_invalid_timestamps(spark):
    # Arrange: create small DataFrame with old, new ingestion timestamps and invalid timestamps
    test_data = [
        {
            "run_id": 1,
            "record_id": "old",
            "city_id": 1,
            "measurement_timestamp": datetime(2026, 7, 20, 14, 0),
            "ingested_at_utc": datetime(2026, 7, 20, 15, 0),
            "european_aqi": 40.0,
            "is_valid_measurement_timestamp": True
        },

        {
            "run_id": 2,
            "record_id": "new",
            "city_id": 1,
            "measurement_timestamp": datetime(2026, 7, 20, 14, 0),
            "ingested_at_utc": datetime(2026, 7, 20, 16, 0),
            "european_aqi": 55.0,
            "is_valid_measurement_timestamp": True
        },

        {
            "run_id": 2,
            "record_id": "invalid_1",
            "city_id": 1,
            "measurement_timestamp": None,
            "ingested_at_utc": datetime(2026, 7, 20, 16, 0),
            "european_aqi": 60.0,
            "is_valid_measurement_timestamp": False
        },

        {
            "run_id": 2,
            "record_id": "invalid_2",
            "city_id": 1,
            "measurement_timestamp": None,
            "ingested_at_utc": datetime(2026, 7, 20, 16, 0),
            "european_aqi": 60.0,
            "is_valid_measurement_timestamp": False
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Act: call function
    result_df = deduplicate_measurements(test_df)

    # Assert: check for correct outputs
    collected_rows = result_df.collect()

    assert len(collected_rows) == 3

    result_rows = {
        row["record_id"]: row
        for row in collected_rows
    }

    assert set(result_rows.keys()) == {"new", "invalid_1", "invalid_2"}

    assert find_duplicate_measurement_keys(result_df).count() == 0

    invalid_count = result_df.filter(
        ~F.col("is_valid_measurement_timestamp")
    ).count()

    assert invalid_count == 2

    assert result_rows["new"]["european_aqi"] == 55.0


# Test hourly array-length validator
def test_validate_measurement_array_lengths_accepts_valid_lengths(spark):
    # Arrange: create small DataFrame input
    expected_length = EXPECTED_MEASUREMENTS_PER_RESPONSE
    test_data = [
        {
            "time_array": [
                f"t{index}"
                for index in range(expected_length)
            ],
            "european_aqi_array": [22.0] * expected_length,
            "pm10_array": [10.2] * expected_length,
            "pm2_5_array": [5.7] * expected_length,
            "nitrogen_dioxide_array": [23.5] * expected_length
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Act and assert: call function and we expect no error raised
    validate_measurement_array_lengths(test_df)


def test_validate_measurement_array_lengths_rejects_mismatched_lengths(spark):
    # Arrange: create small DataFrame input
    expected_length = EXPECTED_MEASUREMENTS_PER_RESPONSE
    test_data = [
        {
            "time_array": [
                f"t{index}"
                for index in range(expected_length)
            ],
            "european_aqi_array": [22.0] * (expected_length - 1),
            "pm10_array": [10.2] * expected_length,
            "pm2_5_array": [5.7] * expected_length,
            "nitrogen_dioxide_array": [23.5] * expected_length
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Act and assert
    with pytest.raises(
        RuntimeError,
        match=r"Issues found in 1 source array-length records\.",
    ):
        validate_measurement_array_lengths(test_df)


def test_validate_measurement_array_lengths_rejects_null_arrays(spark):
    # Arrange: create small input DataFrame
    expected_length = EXPECTED_MEASUREMENTS_PER_RESPONSE

    test_data = [
        {
            "time_array": [
                f"t{index}"
                for index in range(expected_length)
            ],
            "european_aqi_array": [35.0] * expected_length,
            "pm10_array": [22.0] * expected_length,
            "pm2_5_array": [5.5] * expected_length,
            "nitrogen_dioxide_array": [15.0] * expected_length
        },

        {
            "time_array": [
                f"t{index}"
                for index in range(expected_length)
            ],
            "european_aqi_array": [35.0] * expected_length,
            "pm10_array": [22.0] * expected_length,
            "pm2_5_array": [5.5] * expected_length,
            "nitrogen_dioxide_array": None
        },
    ]

    test_df = spark.createDataFrame(test_data)

    # Act and assert
    with pytest.raises(
        RuntimeError,
        match=r"Issues found in 1 source array-length records\.",
    ):
        validate_measurement_array_lengths(test_df)
