import os
import time
from dotenv import load_dotenv
from utils.database import database_connection, store_batch_to_mongodb
from utils.subgraph_query import query_subgraph

# Database configuration
DB_NAME = "the-graph-polymarket-orderbook"
COLLECTION_NAME = "accounts"

# The Graph API endpoint
SUBGRAPH_URL = "https://gateway.thegraph.com/api/{}/subgraphs/id/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC"

# Define the GraphQL query
ACCOUNTS_QUERY = """
    query GetAccounts($skip: Int!) {
        accounts(
            first: 1000,
            orderBy: id,
            orderDirection: asc,
            skip: $skip
        ) {
            id
            creationTimestamp
            lastSeenTimestamp
            collateralVolume
            scaledCollateralVolume
            profit
            numTrades
            lastTradedTimestamp
            fpmmPoolMemberships {
                id
            }
            scaledProfit
        }
    }
"""

def process_accounts() -> None:
    """
    Process all accounts with pagination and store in MongoDB
    """
    api_key = os.getenv('API_KEY')
    if not api_key:
        raise ValueError("API_KEY not found in environment variables")

    with database_connection(DB_NAME, COLLECTION_NAME) as collection:
        # Create index for better performance
        collection.create_index('id', unique=True)
        
        skip = 0
        total_processed = 0
        
        while True:
            print(f"Fetching records with skip={skip}")
            result = query_subgraph(
                query=ACCOUNTS_QUERY,
                variables={'skip': skip},
                subgraph_url=SUBGRAPH_URL,
                api_key=api_key
            )
            
            if not result or 'data' not in result or not result['data']['accounts']:
                print("No more data to fetch")
                break
            
            current_batch = result['data']['accounts']
            store_batch_to_mongodb(collection, current_batch)
            
            total_processed += len(current_batch)
            print(f"Total accounts processed: {total_processed}")
            
            if len(current_batch) < 1000:  # Less than max results means we're done
                break
            
            skip += 1000
            time.sleep(0.1)  # Rate limiting

def main():
    load_dotenv()
    
    try:
        print("Starting to process accounts...")
        process_accounts()
        print("Completed processing all accounts")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()