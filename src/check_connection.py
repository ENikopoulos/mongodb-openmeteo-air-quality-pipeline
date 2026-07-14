# Imports
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import quote_plus

def load_env():
    load_dotenv()

    # Root username
    ROOT_USERNAME = quote_plus(os.environ["MONGO_ROOT_USERNAME"])

    # Root password
    ROOT_PASSWORD = quote_plus(os.environ["MONGO_ROOT_PASSWORD"])

    return ROOT_USERNAME, ROOT_PASSWORD

def build_uri(ROOT_USERNAME, ROOT_PASSWORD):
    
    return f"mongodb://{ROOT_USERNAME}:{ROOT_PASSWORD}@localhost:27017/?authSource=admin"

def main():
    # Load .env
    ROOT_USERNAME, ROOT_PASSWORD = load_env()

    # Build uri
    uri = build_uri(ROOT_USERNAME, ROOT_PASSWORD)

    client = MongoClient(uri)
    try:
        client.admin.command("ping")
        print("Successful Connection")
         # Database, collection and data
        data = {
                "city": "Athens",
                "european_aqi": 65,
                "measurement_timestamp": "tmstamp",
                "source": "openmeteo"
            }
        database = client["learning_db"]
        collection = database["air_quality_test"]
        insert_result = collection.insert_one(data)
        print(f"inserted id = {insert_result.inserted_id}")
        retrieved_document = collection.find_one({"_id": insert_result.inserted_id})
        print(retrieved_document)

    finally:
        client.close()


if __name__ == "__main__":
    main()
