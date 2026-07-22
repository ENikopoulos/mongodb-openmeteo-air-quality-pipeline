import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession
        .builder
        .appName("SharedTestSession")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    spark_session.stop()