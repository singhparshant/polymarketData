# This script adds the conditionId for FPMMs in a MongoDB collection

import os
import time
from dotenv import load_dotenv
from utils.database import database_connection, store_batch_to_mongodb
from utils.subgraph_query import query_subgraph
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database configuration
DB_NAME = "polygon_polymarket"
COLLECTION_NAME = "fpmms"

# The Graph API endpoint
SUBGRAPH_URL = "https://gateway.thegraph.com/api/{}/subgraphs/id/6c58N5U4MtQE2Y8njfVrrAfRykzfqajMGeTMEvMmskVz"

# Define the GraphQL query
FPMMS_QUERY = """
    query GetFPMMs($id: String!) {
        fpmms(
            where: { id: $id }
        ) {
            id
            conditionId
        }
    }
"""


def process_fpmms() -> None:
    """Process the FPMMs using 5 threads"""
    with database_connection(DB_NAME, COLLECTION_NAME) as collection:
        fpmms = list(
            collection.find()
        )  # Convert cursor to list for safe multi-threading
        print(f"Total FPMMs: {len(fpmms)}")

        def process_single_fpmm(fpmm):
            # Fetch data from The Graph
            data = query_subgraph(
                FPMMS_QUERY,
                variables={"id": fpmm["fpmm_address"]},
                subgraph_url=SUBGRAPH_URL,
                api_key=os.getenv("API_KEY"),
            )
            # print(data["data"]["fpmms"])

            # Update the document if data is valid
            # if data and "fpmms" in data and data["fpmms"]:
            condition_id = data["data"]["fpmms"][0].get("conditionId")
            if condition_id:
                collection.update_one(
                    {"fpmm_address": fpmm["fpmm_address"]},
                    {"$set": {"conditionId": condition_id}},
                )
                print(f"Updated {fpmm['fpmm_address']} with conditionId {condition_id}")
            else:
                print(f"No conditionId found for {fpmm['fpmm_address']}")
            # else:
            #     print(f"No data for {fpmm['fpmm_address']}")

        # Use ThreadPoolExecutor to process in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_single_fpmm, fpmm) for fpmm in fpmms]
            for future in as_completed(futures):
                # This will re-raise any exceptions from the threads
                future.result()

    # Store in MongoDB
    # store_batch_to_mongodb(data, collection)


if __name__ == "__main__":
    process_fpmms()
