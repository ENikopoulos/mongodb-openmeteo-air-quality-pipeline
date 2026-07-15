import os
import urllib.parse
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

# Configuration
DATABASE_NAME = "openmeteo_air_quality"
RAW_COLLECTION_NAME = "raw_responses"
INGESTION_RUNS_COLLECTION_NAME = "ingestion_runs"

# Load dotenv variables
def load_environment():
    load_dotenv()
    root_username = urllib.parse.quote_plus(os.environ["MONGO_ROOT_USERNAME"])
    root_password = urllib.parse.quote_plus(os.environ["MONGO_ROOT_PASSWORD"])
    return root_username, root_password

# Build connection URI
def build_uri(root_username, root_password):
    
    return f"mongodb://{root_username}:{root_password}@localhost:27017/?authSource=admin"

def create_collection_index(collection, fields, unique=False):
    """
    Creates an index on a MongoDB collection and returns its name.

    Args:
        collection: PyMongo collection on which to create the index.
        fields: Ordered list of (field_name, direction) pairs.
        unique: Whether MongoDB should enforce unique indexed values.

    Returns:
        The name of the created or existing index.
    """
    return collection.create_index(
        fields,
        unique=unique
    )
    

def main():
    
    # Load environment variables
    root_username, root_password = load_environment()

    # Build URI
    uri = build_uri(root_username, root_password)

    # Create client
    client = MongoClient(uri)
    
    try:
        database = client[DATABASE_NAME]
        raw_collection = database[RAW_COLLECTION_NAME]
        ingestion_run_collection = database[INGESTION_RUNS_COLLECTION_NAME]

        # Create indexes for raw_collection
        run_id_raw_index = create_collection_index(
            raw_collection,
            [("run_id", ASCENDING)]
        )
        city_ingestion_index = create_collection_index(
            raw_collection,
            [
                ("city.city_id", ASCENDING),
                ("ingestion_metadata.ingested_at_utc", DESCENDING)
            ]
        )
        
        # Create indexes for ingestion_run_collection
        run_id_ingest_index = create_collection_index(
            ingestion_run_collection,
            [("run_id", ASCENDING)],
            unique=True
        )

        run_completed_index = create_collection_index(
            ingestion_run_collection,
            [("run_completed_at_utc", DESCENDING)]
        )

        # Print logs
        print("Indexes created or confirmed:")
        print(f"- {run_id_raw_index}")
        print(f"- {city_ingestion_index}")
        print(f"- {run_id_ingest_index}")
        print(f"- {run_completed_index}")


    except PyMongoError as error:
        print(f"MongoDB index setup failed: {error}")

    finally:
        client.close()

if __name__ == "__main__":
    main()
