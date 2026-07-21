# Imports
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from transform_air_quality import (
    SILVER_PATH,
    validate_row_counts_match,
)

# Constants
GOLD_PATH = "data/gold/air_quality_summary"
EXPECTED_HOURS_PER_DAY = 24


def create_gold_spark():
    return (
        SparkSession
        .builder
        .appName("AirQualityGoldTransform")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# Daily measurements
def daily_aggregation(silver_data):
    """
    Aggregates Silver measurements into one daily row per city.
    Calculates valid pollutant statistics, observed-hour completeness,
    metric-specific valid counts, and daily data-quality flags.
"""
    return silver_data.filter(
        F.col("is_valid_measurement_timestamp")
    ).groupBy(
        "city_id",
        "measurement_date",
    ).agg(
        F.first(
            "city",
            ignorenulls=True
        ).alias("city"),

        F.avg(
            F.when(
                F.col("is_valid_european_aqi"),
                F.col("european_aqi")
            )
        ).alias("average_european_aqi"),

        F.max(
            F.when(
                F.col("is_valid_european_aqi"),
                F.col("european_aqi")
            )
        ).alias("maximum_european_aqi"),

        F.avg(
            F.when(
                F.col("is_valid_pm10"),
                F.col("pm10")
            )
        ).alias("average_pm10"),

        F.max(
            F.when(
                F.col("is_valid_pm10"),
                F.col("pm10")
            )
        ).alias("maximum_pm10"),

        F.avg(
            F.when(
                F.col("is_valid_pm2_5"),
                F.col("pm2_5")
            )
        ).alias("average_pm2_5"),

        F.max(
            F.when(
                F.col("is_valid_pm2_5"),
                F.col("pm2_5")
            )
        ).alias("maximum_pm2_5"),

        F.avg(
            F.when(
                F.col("is_valid_nitrogen_dioxide"),
                F.col("nitrogen_dioxide")
            )
        ).alias("average_nitrogen_dioxide"),

        F.max(
            F.when(
                F.col("is_valid_nitrogen_dioxide"),
                F.col("nitrogen_dioxide")
            )
        ).alias("maximum_nitrogen_dioxide"),

        F.count_distinct("measurement_timestamp").alias("observed_hour_count"),

        F.sum(
            F.when(
                F.col("is_valid_european_aqi"),
                1
            ).otherwise(0)
        ).alias("valid_daily_european_aqi_count"),

        F.sum(
            F.when(
                F.col("is_valid_pm10"),
                1
            ).otherwise(0)
        ).alias("valid_daily_pm10_count"),

        F.sum(
            F.when(
                    F.col("is_valid_pm2_5"),
                1
            ).otherwise(0)
        ).alias("valid_daily_pm2_5_count"),

        F.sum(
            F.when(
                F.col("is_valid_nitrogen_dioxide"),
                1
            ).otherwise(0)
        ).alias("valid_daily_nitrogen_dioxide_count")
    ).withColumn(
        "expected_hour_count",
        F.lit(EXPECTED_HOURS_PER_DAY)
    ).withColumns(
        {
            "completeness_percentage": F.col("observed_hour_count") / F.col("expected_hour_count") * 100,

            "is_complete_day": (F.col("observed_hour_count") == F.col("expected_hour_count")),

            "is_fully_valid_day": (
                (F.col("observed_hour_count") == F.col("expected_hour_count"))
                & (F.col("valid_daily_european_aqi_count") == F.col("expected_hour_count"))
                & (F.col("valid_daily_pm10_count") == F.col("expected_hour_count"))
                & (F.col("valid_daily_pm2_5_count") == F.col("expected_hour_count"))
                & (F.col("valid_daily_nitrogen_dioxide_count") == F.col("expected_hour_count"))
            )
        }
    )


# Timestamp of worst european AQI
def maximum_european_aqi_timestamp(silver_data):
    return (
        silver_data.filter(
            F.col("is_valid_measurement_timestamp")
            & F.col("is_valid_european_aqi"),
        ).withColumn(
            "ranked_city_by_aqi",
            F.row_number().over(
                Window.partitionBy(
                    "city_id",
                    "measurement_date"
                ).orderBy(
                    F.col("european_aqi").desc(),
                    F.col("measurement_timestamp").asc(),
                )
            ),
        ).filter(
            F.col("ranked_city_by_aqi") == 1
        ).select(
            F.col("city_id"),
            F.col("measurement_date"),
            F.col("measurement_timestamp").alias("maximum_european_aqi_timestamp")
        )
    )


# Write daily report to parquet
def write_daily_report_to_gold(daily_report, gold_path):
    daily_report.write.parquet(
        gold_path,
        mode="overwrite"
    )


# Validate schema
def validate_schema(expected_df, actual_df, label):
    expected_schema = [
        (field.name, field.dataType.simpleString())
        for field in expected_df.schema.fields
    ]

    actual_schema = [
        (field.name, field.dataType.simpleString())
        for field in actual_df.schema.fields
    ]

    if expected_schema != actual_schema:
        raise RuntimeError(
            f"{label} schema does not match the written DataFrame"
        )

    print(f"{label} schema validation passed")


# Validate non-null business keys
def validate_no_null_business_keys(dataframe, key_columns, label):
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    null_condition = F.col(key_columns[0]).isNull()

    for column_name in key_columns[1:]:
        null_condition = (
            null_condition
            | F.col(column_name).isNull()
        )

    null_key_count = dataframe.filter(null_condition).count()

    if null_key_count > 0:
        raise RuntimeError(
            f"{label} contains {null_key_count} rows "
            "with null business keys"
        )

    print(f"{label} business-key validation passed")


# Validate no duplicate keys
def validate_no_duplicate_keys(dataframe, key_columns, label):
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    duplicate_keys = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_group_count = duplicate_keys.count()

    if duplicate_group_count > 0:
        raise RuntimeError(
            f"{label} contains {duplicate_group_count} duplicate key groups"
        )

    print(f"{label} duplicate-key validation passed")


def main():
    # Create a spark session and read the parquet files
    spark = None

    try:
        spark = create_gold_spark()
        spark.sparkContext.setLogLevel("WARN")

        silver_data = spark.read.parquet(SILVER_PATH)

        # Create date column
        silver_data = silver_data.withColumn(
            "measurement_date",
            F.to_date(F.col("measurement_timestamp"))
        )

        # Daily aggregation
        daily = daily_aggregation(silver_data)

        # Maximum AQI timestamp
        max_aqi = maximum_european_aqi_timestamp(silver_data)

        # Joined daily and max aqi
        daily_report = daily.join(
            max_aqi,
            on=["city_id", "measurement_date"],
            how="left"
        )

        expected_row_count = daily_report.count()

        # Write daily_report to gold
        write_daily_report_to_gold(daily_report, GOLD_PATH)

        # Read Gold data back
        gold_readback = spark.read.parquet(GOLD_PATH)

        actual_row_count = gold_readback.count()

        # Validate row counts
        validate_row_counts_match(
            expected_row_count,
            actual_row_count,
            "Gold read-back"
        )

        # Check column names and data types
        validate_schema(
            daily_report,
            gold_readback,
            "Gold read-back"
        )

        # Check for null business keys
        validate_no_null_business_keys(
            gold_readback,
            ["city_id", "measurement_date"],
            "Gold read-back"
        )

        # Check for duplicate keys
        validate_no_duplicate_keys(
            gold_readback,
            ["city_id", "measurement_date"],
            "Gold read-back"
        )

        print("Gold read-back checks passed.")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
