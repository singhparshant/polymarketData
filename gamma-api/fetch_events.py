import os
import time
import requests
from typing import List, Dict
from dotenv import load_dotenv
from pymongo import MongoClient

def fetch_polymarket_events(
    gamma_endpoint: str,
    closed: bool = False,
    volume_min: float = None,
    batch_size: int = 100
) -> List[Dict]:
    """
    Fetch all events from Polymarket
    """
    all_events = []
    offset = 0

    while True:
        params = {
            "limit": batch_size,
            "offset": offset,
            "closed": str(closed).lower(),
            "ascending": "true"
        }
        
        if volume_min is not None:
            params["volume_min"] = volume_min

        try:
            response = requests.get(f"{gamma_endpoint}/events", params=params)
            response.raise_for_status()
            events = response.json()
            
            if not events:
                break
            
            all_events.extend(events)
            print(f"Fetched {len(all_events)} events...")
            
            offset += batch_size
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching events: {str(e)}")
            break

    print(f"\nFetch completed! Total events: {len(all_events)}")
    return all_events

def store_events_to_mongodb(
    events: List[Dict],
    collection
) -> None:
    """
    Store events in MongoDB using event ID as primary key
    """
    stored_count = 0

    try:
        for event in events:
            # Use the event's id field directly
            collection.replace_one(
                {'id': event['id']},
                event,
                upsert=True
            )
            stored_count += 1

        print(f"Successfully stored {stored_count} events in MongoDB")
        
    except Exception as e:
        print(f"Error storing events in MongoDB: {str(e)}")

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
    events_collection = db.events_new2

    try:
        # Fetch and store active events
        print("Fetching active events...")
        active_events = fetch_polymarket_events(
            gamma_endpoint=GAMMA_ENDPOINT,
            closed=False,
            volume_min=1000
        )
        store_events_to_mongodb(active_events, events_collection)

        # Fetch and store closed events
        print("\nFetching closed events...")
        closed_events = fetch_polymarket_events(
            gamma_endpoint=GAMMA_ENDPOINT,
            closed=True,
            volume_min=1000
        )
        store_events_to_mongodb(closed_events, events_collection)

    finally:
        client.close()