import os
from typing import List, Dict
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from contextlib import contextmanager

def get_database_connection():
    """
    Create and return a MongoDB connection using environment variables
    """
    load_dotenv()
    
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        raise ValueError("MONGO_URI not found in environment variables")
    
    return MongoClient(mongo_uri)

@contextmanager
def database_connection(db_name: str, collection_name: str):
    """
    Context manager for database connections
    Args:
        db_name: Name of the database
        collection_name: Name of the collection
    """
    client = get_database_connection()
    try:
        yield client[db_name][collection_name]
    finally:
        client.close()

def store_batch_to_mongodb(collection, batch_data: List[Dict]) -> None:
    """
    Store a batch of records in MongoDB using bulk operations
    """
    try:
        operations = []
        for record in batch_data:
            operations.append(
                UpdateOne(
                    {'id': record['id']},  # Using 'id' as the unique identifier
                    {'$set': record},
                    upsert=True
                )
            )
        
        if operations:
            result = collection.bulk_write(operations, ordered=False)
            print(f"Successfully stored batch of {len(batch_data)} records")
            print(f"Inserted: {result.upserted_count}, Modified: {result.modified_count}")
    except Exception as e:
        print(f"Error storing batch in MongoDB: {str(e)}")
