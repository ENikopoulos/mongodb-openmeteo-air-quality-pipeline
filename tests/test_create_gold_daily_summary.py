# Imports
from datetime import datetime, date

import pytest
from pyspark.sql import functions as F

from src.create_gold_daily_summary import (
    daily_aggregation,
    maximum_european_aqi_timestamp,
    validate_no_null_business_keys,
    validate_no_duplicate_keys,
)


# Test gold daily aggregation
def test_daily_aggregation(spark):
    # Arrange: create small DataFrame
    test_data = [
        {
            "city_id": 1,
            "city": "Athens",
            "is_valid_measurement_timestamp": True,
            "measurement_date": date(2026, 7, 20),
            "measurement_timestamp": datetime(2026, 7, 20, 0, 0),
            "european_aqi": 30.0,
            "pm10": 10.0,
            "pm2_5": 5.0,
            "nitrogen_dioxide": 20.0,
            "is_valid_european_aqi": True,
            "is_valid_pm10": True,
            "is_valid_pm2_5": True,
            "is_valid_nitrogen_dioxide": True,
        },

        {
            "city_id": 1,
            "city": "Athens",
            "is_valid_measurement_timestamp": True,
            "measurement_date": date(2026, 7, 20),
            "measurement_timestamp": datetime(2026, 7, 20, 1, 0),
            "european_aqi": 60.0,
            "pm10": 20.0,
            "pm2_5": 10.0,
            "nitrogen_dioxide": 40.0,
            "is_valid_european_aqi": True,
            "is_valid_pm10": True,
            "is_valid_pm2_5": True,
            "is_valid_nitrogen_dioxide": True,
        },

        {
            "city_id": 1,
            "city": "Athens",
            "is_valid_measurement_timestamp": True,
            "measurement_date": date(2026, 7, 20),
            "measurement_timestamp": datetime(2026, 7, 20, 2, 0),
            "european_aqi": 45.0,
            "pm10": None,
            "pm2_5": 15.0,
            "nitrogen_dioxide": 40.0,
            "is_valid_european_aqi": True,
            "is_valid_pm10": False,
            "is_valid_pm2_5": True,
            "is_valid_nitrogen_dioxide": True,
        },
    ]

    test_df = spark.createDataFrame(test_data)

    # Act: call function
    result_df = daily_aggregation(test_df)

    # Assert: check for correct outputs
    expected = {
        "city_id": 1,
        "city": "Athens",
        "measurement_date": date(2026, 7, 20),
        "observed_hour_count": 3,
        "expected_hour_count": 24,
        "completeness_percentage": 12.5,
        "is_complete_day": False,
        "is_fully_valid_day": False,

        # European AQI statistics
        "average_european_aqi": 45.0,
        "maximum_european_aqi": 60.0,
        "valid_daily_european_aqi_count": 3,

        # PM10 statistics
        "average_pm10": 15.0,
        "maximum_pm10": 20.0,
        "valid_daily_pm10_count": 2,

        # PM2.5 statistics
        "average_pm2_5": 10.0,
        "maximum_pm2_5": 15.0,
        "valid_daily_pm2_5_count": 3,

        # Nitrogen dioxide statistics
        "average_nitrogen_dioxide": 100.0 / 3,
        "maximum_nitrogen_dioxide": 40.0,
        "valid_daily_nitrogen_dioxide_count": 3,
    }

    collected_rows = result_df.collect()

    assert len(collected_rows) == 1

    result_row = collected_rows[0]

    for column_name, expected_value in expected.items():
        if isinstance(expected_value, float):
            assert result_row[column_name] == pytest.approx(expected_value)
        else:
            assert result_row[column_name] == expected_value


# Test maximum european aqi timestamp
def test_maximum_european_aqi_timestamp(spark):
    # Arrange: create a small DataFrame
    test_data = [
         {
            "city_id": 1,
            "city": "Athens",
            "is_valid_measurement_timestamp": True,
            "measurement_date": date(2026, 7, 20),
            "measurement_timestamp": "2026-07-20 00:00:00",
            "european_aqi": 45.0,
            "is_valid_european_aqi": True,
        },

         {
            "city_id": 1,
            "city": "Athens",
            "is_valid_measurement_timestamp": True,
            "measurement_date": date(2026, 7, 20),
            "measurement_timestamp": "2026-07-20 01:00:00",
            "european_aqi": 60.0,
            "is_valid_european_aqi": True,
        },

        {
            "city_id": 1,
            "city": "Athens",
            "is_valid_measurement_timestamp": True,
            "measurement_date": date(2026, 7, 20),
            "measurement_timestamp": "2026-07-20 02:00:00",
            "european_aqi": 60.0,
            "is_valid_european_aqi": True,
        },
    ]

    test_df = (
        spark.createDataFrame(test_data)
        .withColumn(
            "measurement_timestamp",
            F.to_timestamp(
                "measurement_timestamp",
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    # Act: call function
    result_df = (
        maximum_european_aqi_timestamp(test_df)
        .withColumn(
            "maximum_aqi_timestamp_formatted",
            F.date_format(
                "maximum_european_aqi_timestamp",
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )
    # Assert: check for correct output
    collected_rows = result_df.collect()

    assert len(collected_rows) == 1

    result_row = collected_rows[0]

    assert result_row["city_id"] == 1
    assert result_row["measurement_date"] == date(2026, 7, 20)

    assert (
        result_row["maximum_aqi_timestamp_formatted"]
        == "2026-07-20 01:00:00"
    )


# Test Gold non-null business-key validation
def test_validate_non_null_business_keys(spark):
    # Arrange: create a small input DataFrame
    test_data = [
        {
            "city_id": 1,
            "measurement_date": date(2026, 7, 20)
        },

        {
            "city_id": None,
            "measurement_date": date(2026, 7, 21)
        },
    ]

    test_df = spark.createDataFrame(test_data)

    # Act and assert
    with pytest.raises(
        RuntimeError,
        match="Test data check contains 1 rows with null business keys",
    ):
        validate_no_null_business_keys(
            test_df,
            ["city_id", "measurement_date"],
            "Test data check",
        )


# Test duplicate business-key validation
def test_validate_no_duplicate_keys(spark):
    # Arrange: Create a small input DataFrame
    test_data = [
        {
            "city_id": 1,
            "measurement_date": date(2026, 7, 20)
        },

        {
            "city_id": 1,
            "measurement_date": date(2026, 7, 20)
        },

        {
            "city_id": 1,
            "measurement_date": date(2026, 7, 21)
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Act and assert
    with pytest.raises(
        RuntimeError,
        match="Test data contains 1 duplicate key groups",
    ):
        validate_no_duplicate_keys(
            test_df,
            ["city_id", "measurement_date"],
            "Test data",
        )
