import os
import time
import requests
from typing import List, Dict
from dotenv import load_dotenv
from pymongo import MongoClient

def fetch_polymarket_markets(
    gamma_endpoint: str,
    closed: bool = False,
    volume_num_min: float = None,
    batch_size: int = 100
) -> List[Dict]:
    """
    Fetch all markets from Polymarket
    """
    all_markets = []
    offset = 0

    while True:
        params = {
            "limit": batch_size,
            "offset": offset,
            "closed": str(closed).lower(),
            "ascending": "true"
        }
        
        if volume_num_min is not None:
            params["volume_num_min"] = volume_num_min

        try:
            response = requests.get(f"{gamma_endpoint}/markets", params=params)
            response.raise_for_status()
            markets = response.json()
            
            if not markets:
                break
            
            all_markets.extend(markets)
            print(f"Fetched {len(all_markets)} markets...")
            
            offset += batch_size
            time.sleep(0.5)  # Delay to avoid rate limiting
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching markets: {str(e)}")
            break

    print(f"\nFetch completed! Total markets: {len(all_markets)}")
    return all_markets

def store_markets_to_mongodb(
    markets: List[Dict],
    collection
) -> None:
    """
    Store markets in MongoDB using market ID as primary key
    """
    stored_count = 0

    try:
        for market in markets:
            collection.replace_one(
                {'id': market['id']},
                market,
                upsert=True
            )
            stored_count += 1

        print(f"Successfully stored {stored_count} markets in MongoDB")
        
    except Exception as e:
        print(f"Error storing markets in MongoDB: {str(e)}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    MONGO_URI = os.getenv('MONGO_URI')
    GAMMA_ENDPOINT = os.getenv('GAMMA_ENDPOINT')
    
    if not all([MONGO_URI, GAMMA_ENDPOINT]):
        raise ValueError("Missing required environment variables. Please check your .env file")

    # Initialize MongoDB connection once
    client = MongoClient(MONGO_URI)
    db = client.polymarket
    markets_collection = db.markets_new2

    try:
        # Fetch and store active markets
        print("Fetching active markets...")
        active_markets = fetch_polymarket_markets(
            gamma_endpoint=GAMMA_ENDPOINT,
            closed=False,
            volume_num_min=1000  # Optional: Set minimum volume
        )
        store_markets_to_mongodb(active_markets, markets_collection)

        # Fetch and store closed markets
        print("\nFetching closed markets...")
        closed_markets = fetch_polymarket_markets(
            gamma_endpoint=GAMMA_ENDPOINT,
            closed=True,
            volume_num_min=1000  # Optional: Set minimum volume
        )
        store_markets_to_mongodb(closed_markets, markets_collection)

    finally:
        client.close()