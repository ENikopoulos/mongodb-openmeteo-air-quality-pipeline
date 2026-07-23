# Imports
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from src.request_config import EXPECTED_MEASUREMENTS_PER_RESPONSE

# Constants
SILVER_PATH = "data/silver/air_quality_measurements"

# Load environment
def load_mongo_credentials():
    load_dotenv()
    root_username = quote_plus(os.environ["MONGO_ROOT_USERNAME"])
    root_password = quote_plus(os.environ["MONGO_ROOT_PASSWORD"])

    return root_username, root_password


# Mongo connection URI
def create_mongo_uri(root_username, root_password):

    return (
        f"mongodb://{root_username}:{root_password}"
        "@127.0.0.1:27017/?authSource=admin"
    )


# Spark Session
def create_spark_session(uri):

    return (
        SparkSession
        .builder
        .appName("AirQualitySilverTransform")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.mongodb.read.connection.uri", uri)
        .config("spark.mongodb.read.database", "openmeteo_air_quality")
        .config("spark.mongodb.read.collection", "raw_responses")
        .getOrCreate()
    )


# Select source field from raw_document
def select_source_fields(raw_df):

    return raw_df.select(
        F.col("run_id"),
        F.col("city.city_id").alias("city_id"),
        F.col("city.city").alias("city"),
        F.col("city.country").alias("country"),
        F.col("city.requested_latitude").alias("requested_latitude"),
        F.col("city.requested_longitude").alias("requested_longitude"),
        F.col("ingestion_metadata.ingested_at_utc").alias("ingested_at_utc"),
        F.col("raw_payload.hourly.time").alias("time_array"),
        F.col("raw_payload.hourly.european_aqi").alias("european_aqi_array"),
        F.col("raw_payload.hourly.pm10").alias("pm10_array"),
        F.col("raw_payload.hourly.pm2_5").alias("pm2_5_array"),
        F.col("raw_payload.hourly.nitrogen_dioxide").alias("nitrogen_dioxide_array"),
    )


# Validate measurement array lengths
def validate_measurement_array_lengths(selected_fields):
    array_length_issues = (
        selected_fields
        .select(
            F.size("time_array").alias("time_count"),
            F.size("european_aqi_array").alias("european_aqi_count"),
            F.size("pm10_array").alias("pm10_count"),
            F.size("pm2_5_array").alias("pm2_5_count"),
            F.size("nitrogen_dioxide_array").alias("nitrogen_dioxide_count")
        ).filter(
            (F.col("european_aqi_count") != F.col("time_count"))
            | (F.col("pm10_count") != F.col("time_count"))
            | (F.col("pm2_5_count") != F.col("time_count"))
            | (F.col("nitrogen_dioxide_count") != F.col("time_count"))
            | (F.col("time_count") != EXPECTED_MEASUREMENTS_PER_RESPONSE)
            | (F.col("time_count").isNull())
            | (F.col("european_aqi_count").isNull())
            | (F.col("pm10_count").isNull())
            | (F.col("pm2_5_count").isNull())
            | (F.col("nitrogen_dioxide_count").isNull())
        )
    )

    issue_count = array_length_issues.count()

    if issue_count > 0:
        raise RuntimeError(
            f"Issues found in {issue_count} source array-length records."
        )

    print("Measurement array-length validation passed.")


# Zip array elements into groups for each timestamp
def zip_measurement_arrays(selected_fields):
    
    return selected_fields.withColumn(
        "measurement_array",
        F.arrays_zip(
            F.col("time_array"),
            F.col("european_aqi_array"),
            F.col("pm10_array"),
            F.col("pm2_5_array"),
            F.col("nitrogen_dioxide_array")
        ),
    )


# Explode measurements
def explode_measurements(zipped_fields):
    return zipped_fields.withColumn(
        "measurement",
        F.explode("measurement_array")
    )


# Select scalar fields
def select_measurement_fields(exploded_fields):

    return exploded_fields.select(
        F.col("run_id"),
        F.col("city_id"),
        F.col("city"),
        F.col("country"),
        F.col("requested_latitude"),
        F.col("requested_longitude"),
        F.col("ingested_at_utc"),
        F.col("measurement.time_array").alias("measurement_timestamp_raw"),
        F.col("measurement.european_aqi_array").alias("european_aqi"),
        F.col("measurement.pm10_array").alias("pm10"),
        F.col("measurement.pm2_5_array").alias("pm2_5"),
        F.col("measurement.nitrogen_dioxide_array").alias("nitrogen_dioxide")
    )


# Add measurement timestamp
def add_measurement_timestamp(selected_measurements):
    return selected_measurements.withColumn(
        "measurement_timestamp",
        F.to_timestamp(F.col("measurement_timestamp_raw"), "yyyy-MM-dd'T'HH:mm")
    )


# Validation flags
def add_timestamp_validity_flag(timestamp_fields):
    return timestamp_fields.withColumn(
        "is_valid_measurement_timestamp",
        F.col("measurement_timestamp_raw").isNotNull() &
        F.col("measurement_timestamp").isNotNull()
    )


# Validation flags for measurement parameters
def add_measurement_validity_flags(timestamp_validated):
    return timestamp_validated.withColumns({
        "is_valid_european_aqi": (
            F.when(
                (F.col("european_aqi").isNotNull()) & (F.col("european_aqi") >= 0),
                True
            ).otherwise(False)
        ),
        "is_valid_pm10": (
            F.when(
                (F.col("pm10").isNotNull()) & (F.col("pm10") >= 0),
                True
            ).otherwise(False)
        ),
        "is_valid_pm2_5": (
            F.when(
                (F.col("pm2_5").isNotNull()) & (F.col("pm2_5") >= 0),
                True
            ).otherwise(False)
        ),
        "is_valid_nitrogen_dioxide": (
            F.when(
                (F.col("nitrogen_dioxide").isNotNull()) & (F.col("nitrogen_dioxide") >= 0),
                True
            ).otherwise(False)
        )
    })


# Deduplicate the measurements
def find_duplicate_measurement_keys(measurement_validated):
    return measurement_validated.filter(
        F.col("is_valid_measurement_timestamp")
    ).groupBy(
        "city_id",
        "measurement_timestamp"
    ).agg(
        F.count("*").alias("record_count")
    ).filter(
        F.col("record_count") > 1
    )


# Rank measurements by ingestion time
def add_latest_record_rank(measurement_validated):
    dedupe_window = Window.partitionBy(
        "city_id",
        "measurement_timestamp"
    ).orderBy(
        F.col("ingested_at_utc").desc(),
        F.col("run_id").desc()
    )

    return measurement_validated.filter(
        F.col("is_valid_measurement_timestamp")    
    ).withColumn(
        "deduplication_rank",
        F.row_number()
        .over(dedupe_window)
    )


# Deduplicate the final DataFrame
def deduplicate_measurements(measurement_validated):
    ranked_measurements = add_latest_record_rank(measurement_validated)
    latest_valid = ranked_measurements.filter(
        F.col("deduplication_rank") == 1
    ).drop(
        "deduplication_rank"
    )

    # Retain invalid timestamp rows for data-quality investigation.
    invalid_timestamp_rows = measurement_validated.filter(
        ~F.col("is_valid_measurement_timestamp")
    )

    return latest_valid.unionByName(invalid_timestamp_rows)


# Select silver fields
def select_silver_fields(deduplicated_measurements):
    return deduplicated_measurements.select(
        F.col("run_id"),
        F.col("ingested_at_utc"),
        F.col("city_id"),
        F.col("city"),
        F.col("country"),
        F.col("requested_latitude"),
        F.col("requested_longitude"),
        F.col("measurement_timestamp"),
        F.col("measurement_timestamp_raw"),
        F.col("european_aqi"),
        F.col("pm10"),
        F.col("pm2_5"),
        F.col("nitrogen_dioxide"),
        F.col("is_valid_measurement_timestamp"),
        F.col("is_valid_european_aqi"),
        F.col("is_valid_pm10"),
        F.col("is_valid_pm2_5"),
        F.col("is_valid_nitrogen_dioxide"),
    )


# Write data to silver in parquet
def write_silver_parquet(silver_measurements, output_path):
    silver_measurements.write.parquet(
        output_path,
        mode="overwrite"
    )


# Summarize duplicate measurements
def summarize_duplicate_measurement_keys(duplicate_keys):
    return duplicate_keys.agg(
        F.count("*").alias("duplicate_key_groups"),
        F.sum(F.col("record_count") - 1).alias("redundant_rows"),
        F.max("record_count").alias("max_records_per_key")
    )


# Validate no duplicates
def validate_no_duplicate_measurement_keys(dataframe, label):
    duplicate_keys = find_duplicate_measurement_keys(dataframe)
    duplicate_group_count = duplicate_keys.count()

    if duplicate_group_count > 0:
        raise RuntimeError(
            f"{label} contains {duplicate_group_count} duplicate key groups"
        )

    print(f"{label} duplicate-key validation passed")


# Validate row counts
def validate_row_counts_match(expected_count, actual_count, label):
    if expected_count != actual_count:
        raise RuntimeError(
            f"{label} row-count validation failed: "
            f"expected {expected_count}, found {actual_count}"
        )

    print(
        f"{label} row-count validation passed: "
        f"{actual_count} rows"
    )


# Summarize measurement validity
def summarize_measurement_validity(measurement_validated):
    return measurement_validated.groupBy(
        "is_valid_measurement_timestamp",
        "is_valid_european_aqi",
        "is_valid_pm10",
        "is_valid_pm2_5",
        "is_valid_nitrogen_dioxide",
    ).count()


# Main
def main():
    # Load mongo credentials
    root_username, root_password = load_mongo_credentials()

    # Mongo URI creation
    uri = create_mongo_uri(root_username, root_password)

    # Start spark session, connect, read, and select the source fields
    spark = None
    
    try:
        spark = create_spark_session(uri)
        spark.sparkContext.setLogLevel("WARN")

        raw_df = spark.read.format("mongodb").load()

        selected_fields = select_source_fields(raw_df)

        validate_measurement_array_lengths(selected_fields)

        zipped_fields = zip_measurement_arrays(selected_fields)

        exploded_fields = explode_measurements(zipped_fields)

        selected_measurements = select_measurement_fields(exploded_fields)

        timestamp_fields = add_measurement_timestamp(selected_measurements)

        timestamp_validated = add_timestamp_validity_flag(timestamp_fields)

        measurement_validated = add_measurement_validity_flags(timestamp_validated)
        validity_summary = summarize_measurement_validity(
            measurement_validated
        )

        validity_summary.show(truncate=False)

        duplicate_keys = find_duplicate_measurement_keys(measurement_validated)

        duplicate_summary = summarize_duplicate_measurement_keys(duplicate_keys)

        duplicate_summary.show(truncate=False)

        deduplicated_measurements = deduplicate_measurements(measurement_validated)

        validate_no_duplicate_measurement_keys(
            deduplicated_measurements,
            "Deduplicated Silver"
        )

        silver_measurements = select_silver_fields(deduplicated_measurements)

        silver_row_count = silver_measurements.count()

        # Write to silver
        write_silver_parquet(silver_measurements, SILVER_PATH)

        written_silver = spark.read.parquet(SILVER_PATH)

        written_row_count = written_silver.count()

        validate_row_counts_match(
            silver_row_count,
            written_row_count,
            "Silver read-back"
        )

        validate_no_duplicate_measurement_keys(
            written_silver,
            "Written Silver"
        )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
