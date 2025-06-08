import os
import json
import time
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import TradeParams
from pymongo import MongoClient
from typing import Dict, List

def fetch_market_details(client: ClobClient, condition_id: str) -> Dict:
    """
    Fetch market details for a specific condition ID using the CLOB client
    """
    try:
        return client.get_market(condition_id=condition_id)
    except Exception as e:
        print(f"Error fetching market details for condition ID {condition_id}: {str(e)}")
        return None

def store_market_details_to_mongodb(
    market_details: Dict,
    collection,
    condition_id: str
) -> None:
    """
    Store market details in MongoDB using condition ID as primary key
    """
    try:
        # Add the condition ID to the market details
        market_details['conditionId'] = condition_id
        collection.replace_one(
            {'conditionId': condition_id},
            market_details,
            upsert=True
        )
        print(f"Successfully stored details for condition ID: {condition_id}")
    except Exception as e:
        print(f"Error storing market details in MongoDB for condition ID {condition_id}: {str(e)}")

def get_condition_ids_from_mongodb(collection) -> List[str]:
    """
    Get all unique condition IDs from the markets collection
    """
    try:
        # Query for all unique condition IDs
        condition_ids = collection.distinct('conditionId')
        return condition_ids
    except Exception as e:
        print(f"Error fetching condition IDs from MongoDB: {str(e)}")
        return []

def main():
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    MONGO_URI = os.getenv('MONGO_URI')
    PK = os.getenv('PK')
    
    if not all([MONGO_URI, PK]):
        raise ValueError("Missing required environment variables. Please check your .env file")
    
    # Initialize MongoDB connection first
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client.polymarket
    markets_collection = db.markets_new2
    markets_details_collection = db.markets_details_new2
    
    # Initialize CLOB client
    host = "https://clob.polymarket.com"
    chain_id = 137
    clob_client = ClobClient(host, key=PK, chain_id=chain_id)
    clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
    
    try:
        # Get all condition IDs from MongoDB
        condition_ids = get_condition_ids_from_mongodb(markets_collection)
        total_markets = len(condition_ids)
        print(f"Found {total_markets} unique markets to process")
        
        # Process each condition ID
        for i, condition_id in enumerate(condition_ids, 1):
            print(f"\nProcessing market {i}/{total_markets}")
            print(f"Condition ID: {condition_id}")
            
            # Skip if already processed
            if markets_details_collection.find_one({'conditionId': condition_id}):
                print(f"Market {condition_id} already processed, skipping...")
                continue
            
            # Fetch and store market details
            market_details = fetch_market_details(clob_client, condition_id)
            if market_details:
                store_market_details_to_mongodb(
                    market_details,
                    markets_details_collection,
                    condition_id
                )
            
            # Rate limiting
            time.sleep(0.5)
    
    finally:
        mongo_client.close()

if __name__ == "__main__":
    main()