import os
import time
from typing import Dict
from dotenv import load_dotenv
from utils.database import database_connection, store_batch_to_mongodb
from utils.subgraph_query import query_subgraph, get_last_processed_id

# Database configuration
DB_NAME = "the-graph-polymarket-orderbook"
COLLECTION_NAME = "enrichedOrderFills_new"

# The Graph API endpoint
SUBGRAPH_URL = "https://gateway.thegraph.com/api/{}/subgraphs/id/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC"

# Define the GraphQL query
ENRICHED_ORDER_FILLEDS_QUERY = """
    query GetEnrichedOrderFilleds($timestamp: Int!) {
        enrichedOrderFilleds(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where: { timestamp_gte: $timestamp }
        ) {
            transactionHash
            timestamp
            size
            side
            price
            orderHash
            id
            market {
                id
            }
            maker {
                id
            }
            taker {
                id
            }
        }
    }
"""

def get_last_timestamp(collection) -> int:
    """Get the highest timestamp from the collection"""
    last_record = collection.find_one(sort=[("timestamp", -1)])
    return int(last_record["timestamp"]) if last_record else 0

def process_enriched_order_filleds() -> None:
    """
    Process all enriched order fills with pagination and store in MongoDB
    """
    api_key = os.getenv('API_KEY')
    if not api_key:
        raise ValueError("API_KEY not found in environment variables")

    with database_connection(DB_NAME, COLLECTION_NAME) as collection:
        # Create indexes for better performance
        collection.create_index('id', unique=True)
        collection.create_index('timestamp')
        
        # Get the last processed timestamp
        last_timestamp = get_last_timestamp(collection)
        if last_timestamp:
            print(f"Resuming from timestamp: {last_timestamp}")
        
        total_processed = 0
        
        while True:
            print(f"Fetching records from timestamp: {last_timestamp}")
            result = query_subgraph(
                query=ENRICHED_ORDER_FILLEDS_QUERY,
                variables={'timestamp': last_timestamp},
                subgraph_url=SUBGRAPH_URL,
                api_key=api_key
            )
            
            if not result or 'data' not in result or not result['data']['enrichedOrderFilleds']:
                print("No more data to fetch")
                break
            
            current_batch = result['data']['enrichedOrderFilleds']
            store_batch_to_mongodb(collection, current_batch)
            
            total_processed += len(current_batch)
            print(f"Total enriched order fills processed: {total_processed}")
            
            if len(current_batch) < 1000:  # Less than max results means we're done
                break
            
            # Get the last timestamp from the current batch
            last_timestamp = int(current_batch[-1]['timestamp'])
            time.sleep(0.2)  # Rate limiting

def main():
    load_dotenv()
    
    try:
        print("Starting to process enriched order fills...")
        process_enriched_order_filleds()
        print("Completed processing all enriched order fills")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()

