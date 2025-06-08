import os
import time
from dotenv import load_dotenv
from utils.database import database_connection, store_batch_to_mongodb
from utils.subgraph_query import query_subgraph

# Database configuration
DB_NAME = "the-graph-polymarket-activity"
COLLECTION_NAME = "negRiskConversions"

# The Graph API endpoint
SUBGRAPH_URL = "https://gateway.thegraph.com/api/{}/subgraphs/id/Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"

# Define the GraphQL query
NEG_RISK_CONVERSIONS_QUERY = """
    query GetNegRiskConversions($timestamp: Int!) {
        negRiskConversions(
            first: 1000,
            orderBy: timestamp,
            orderDirection: asc,
            where: { timestamp_gte: $timestamp }
        ) {
            timestamp
            stakeholder
            questionCount
            negRiskMarketId
            indexSet
            id
            amount
        }
    }
"""

def get_last_timestamp(collection) -> int:
    """Get the highest timestamp from the collection"""
    last_record = collection.find_one(sort=[("timestamp", -1)])
    return int(last_record["timestamp"]) if last_record else 0

def process_neg_risk_conversions() -> None:
    """
    Process all negative risk conversions with pagination and store in MongoDB
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

            print(f"Querying subgraph for timestamp: {last_timestamp}")
            print(f"Query: {NEG_RISK_CONVERSIONS_QUERY}")
            print(f"Variables: {SUBGRAPH_URL}")

            result = query_subgraph(
                query=NEG_RISK_CONVERSIONS_QUERY,
                variables={'timestamp': last_timestamp},
                subgraph_url=SUBGRAPH_URL,
                api_key=api_key
            )
            
            if not result or 'data' not in result or not result['data']['negRiskConversions']:
                print("No more data to fetch")
                break
            
            current_batch = result['data']['negRiskConversions']
            store_batch_to_mongodb(collection, current_batch)
            
            total_processed += len(current_batch)
            print(f"Total negative risk conversions processed: {total_processed}")
            
            if len(current_batch) < 1000:  # Less than max results means we're done
                break
            
            # Get the last timestamp from the current batch
            last_timestamp = int(current_batch[-1]['timestamp'])
            time.sleep(0.1)  # Rate limiting

def main():
    load_dotenv()
    
    try:
        print("Starting to process negative risk conversions...")
        process_neg_risk_conversions()
        print("Completed processing all negative risk conversions")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()