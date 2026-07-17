# imports 
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def load_environment_variables():
    
    load_dotenv()
    root_username = quote_plus(os.environ["MONGO_ROOT_USERNAME"])
    root_password = quote_plus(os.environ["MONGO_ROOT_PASSWORD"])
    
    return root_username, root_password


def create_mongo_uri(root_username, root_password):
    
    return (
        f"mongodb://{root_username}:{root_password}"
        "@127.0.0.1:27017/?authSource=admin"
    )


def create_spark_session(uri):
    
    spark =(
        SparkSession.builder
        .appName("SparkMongo")
        .config("spark.mongodb.read.connection.uri", uri)
        .config("spark.mongodb.read.database", "openmeteo_air_quality")
        .config("spark.mongodb.read.collection", "raw_responses")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    
    root_username, root_password = load_environment_variables()

    uri = create_mongo_uri(root_username, root_password)

    spark = create_spark_session(uri)
    try:

        read_result = spark.read.format("mongodb").load()
        read_result.printSchema()

        document_count = read_result.count()
        print(f"Raw MongoDB documents: {document_count}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()