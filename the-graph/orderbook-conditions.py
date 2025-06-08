import os
import time
from typing import Dict
from dotenv import load_dotenv
from utils.database import database_connection, store_batch_to_mongodb
from utils.subgraph_query import query_subgraph, get_last_processed_id

# Database configuration
DB_NAME = "the-graph-polymarket-orderbook"
COLLECTION_NAME = "conditions"

# The Graph API endpoint
SUBGRAPH_URL = "https://gateway.thegraph.com/api/{}/subgraphs/id/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC"

# Define the GraphQL query
CONDITIONS_QUERY = """
    query GetConditions($skip: Int!) {
        conditions(
            first: 1000,
            orderBy: id,
            orderDirection: asc,
            skip: $skip
        ) {
            resolutionTimestamp
            resolutionHash
            questionId
            payouts
            payoutNumerators
            payoutDenominator
            outcomeSlotCount
            oracle
            id
            fixedProductMarketMakers {
                id
            }
        }
    }
"""

def process_conditions() -> None:
    """
    Process all conditions with pagination and store in MongoDB
    """
    api_key = os.getenv('API_KEY')
    if not api_key:
        raise ValueError("API_KEY not found in environment variables")

    with database_connection(DB_NAME, COLLECTION_NAME) as collection:
        # Create index for better performance
        collection.create_index('id', unique=True)
        
        # Get the last processed ID
        last_id = get_last_processed_id(collection)
        if last_id:
            print(f"Resuming from ID: {last_id}")
        
        skip = 0
        total_processed = 0
        
        while True:
            print(f"Fetching records with skip={skip}")
            result = query_subgraph(
                query=CONDITIONS_QUERY,
                variables={'skip': skip},
                subgraph_url=SUBGRAPH_URL,
                api_key=api_key
            )
            
            if not result or 'data' not in result or not result['data']['conditions']:
                print("No more data to fetch")
                break
            
            current_batch = result['data']['conditions']
            store_batch_to_mongodb(collection, current_batch)
            
            total_processed += len(current_batch)
            print(f"Total conditions processed: {total_processed}")
            
            if len(current_batch) < 1000:  # Less than max results means we're done
                break
            
            skip += 1000
            time.sleep(0.1)  # Rate limiting

def main():
    load_dotenv()
    
    try:
        print("Starting to process conditions...")
        process_conditions()
        print("Completed processing all conditions")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()