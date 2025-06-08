# todo

import os
import time
from dotenv import load_dotenv
from utils.database import database_connection, store_batch_to_mongodb
from utils.subgraph_query import query_subgraph

# Database configuration
DB_NAME = "the-graph-polymarket-activity"
COLLECTION_NAME = "fpmms"

# The Graph API endpoint
SUBGRAPH_URL = "https://gateway.thegraph.com/api/{}/subgraphs/id/Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"

# Define the GraphQL query
FPMMS_QUERY = """
    query GetFPMMs($lastId: String!) {
        fixedProductMarketMakers(
            first: 1000,
            orderBy: id,
            orderDirection: asc,
            where: { id_gt: $lastId }
        ) {
            id
        }
    }
"""


def get_last_id(collection) -> str:
    """Get the highest ID from the collection"""
    last_record = collection.find_one(sort=[("id", -1)])
    return last_record["id"] if last_record else ""


def process_fpmms() -> None:
    """
    Process all FPMMs with pagination and store in MongoDB
    """
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY not found in environment variables")

    with database_connection(DB_NAME, COLLECTION_NAME) as collection:
        # Create index for better performance
        collection.create_index("id", unique=True)

        # Get the last processed ID
        last_id = get_last_id(collection)
        if last_id:
            print(f"Resuming from ID: {last_id}")

        total_processed = 0

        while True:
            print(f"Fetching records after ID: {last_id}")
            result = query_subgraph(
                query=FPMMS_QUERY,
                variables={"lastId": last_id},
                subgraph_url=SUBGRAPH_URL,
                api_key=api_key,
            )

            if (
                not result
                or "data" not in result
                or not result["data"]["fixedProductMarketMakers"]
            ):
                print("No more data to fetch")
                break

            current_batch = result["data"]["fixedProductMarketMakers"]
            store_batch_to_mongodb(collection, current_batch)

            total_processed += len(current_batch)
            print(f"Total FPMMs processed: {total_processed}")

            if len(current_batch) < 1000:  # Less than max results means we're done
                break

            # Get the last ID from the current batch
            last_id = current_batch[-1]["id"]
            time.sleep(0.1)  # Rate limiting


def main():
    load_dotenv()

    try:
        print("Starting to process FPMMs...")
        process_fpmms()
        print("Completed processing all FPMMs")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
