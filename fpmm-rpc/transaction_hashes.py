# This script retrieves unique transaction hashes from various collections in a MongoDB database,
# counts them, and stores them in a separate collection. It also includes functionality to handle
# transactions related to merges and splits, ensuring that all unique transaction hashes are captured

from pymongo import MongoClient
from web3 import Web3
import time
from bson import ObjectId


def get_transaction_counts():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["polygon_polymarket"]

    # Get unique transaction hashes for each collection
    buy_hashes = db["FPMMBuy"].distinct("transaction_hash")
    sell_hashes = db["FPMMSell"].distinct("transaction_hash")
    funding_added_hashes = db["FundingAdded"].distinct("transaction_hash")
    funding_removed_hashes = db["FundingRemoved"].distinct("transaction_hash")

    # Count unique transactions in each collection
    total_count = (
        len(buy_hashes)
        + len(sell_hashes)
        + len(funding_added_hashes)
        + len(funding_removed_hashes)
    )
    # Print results
    print(f"Sum of all unique transactions: {total_count}")

    # Count truly unique transactions across all collections
    all_unique_txs = set(
        buy_hashes + sell_hashes + funding_added_hashes + funding_removed_hashes
    )
    print(f"Total unique transactions across all collections: {len(all_unique_txs)}")

    # Add these to the transaction_hashes collection if not already present
    existing_hashes = db.transaction_hashes.distinct("transaction_hash")
    new_hashes = [
        tx_hash for tx_hash in all_unique_txs if tx_hash not in existing_hashes
    ]
    if new_hashes:
        batch_size = 1000
        for i in range(0, len(new_hashes), batch_size):
            batch = new_hashes[i : i + batch_size]
            db.transaction_hashes.insert_many(
                [{"transaction_hash": tx_hash} for tx_hash in batch]
            )
            print(f"Inserted batch {i//batch_size + 1}: {len(batch)} documents")
            time.sleep(0.2)  # Sleep 0.2 seconds between batches


def store_transaction_hashes():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["the-graph-polymarket-orderbook"]
    db_polygon = client["polygon_polymarket"]

    amm_fpmms = db_polygon["fpmms"].find({"total_interactions": {"$gt": 0}})
    condition_ids = [fpmm["conditionId"] for fpmm in amm_fpmms]

    # Get unique transaction hashes for each collection
    split_hashes = db["splits"].aggregate(
        [
            {
                "$match": {
                    "timestamp": {"$lt": "1704067200"},
                    "condition.id": {"$in": condition_ids},
                }
            },
            {"$group": {"_id": "$id"}},
            {"$project": {"_id": 0, "id": "$_id"}},
        ]
    )
    print("Crossed the splits")
    # Convert to list
    split_hashes_list = [doc["id"] for doc in split_hashes]
    print("Crossed the splits list", len(split_hashes_list))
    merge_hashes = db["merges"].aggregate(
        [
            {
                "$match": {
                    "timestamp": {"$lt": "1704067200"},
                    "condition.id": {"$in": condition_ids},
                }
            },
            {"$group": {"_id": "$id"}},
            {"$project": {"_id": 0, "id": "$_id"}},
        ]
    )
    print("Crossed the merges")
    # Convert to list
    merge_hashes_list = [doc["id"] for doc in merge_hashes]
    print("Crossed the merges list", len(merge_hashes_list))
    # Combine and get unique transaction hashes
    all_hashes = set(split_hashes_list + merge_hashes_list)
    print("Crossed the all hashes")
    print(len(all_hashes))
    print("Crossed the all hashes length")
    # Create or clear the transaction_hashes collection
    db.drop_collection("transaction_hashes_merges_and_splits")
    tx_collection = client["polygon_polymarket"]["transaction_hashes_merges_and_splits"]

    # Insert each hash as a document
    documents = [{"transaction_hash": tx_hash} for tx_hash in all_hashes]
    if documents:
        batch_size = 10000
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            tx_collection.insert_many(batch)
            print(f"Inserted batch {i//batch_size + 1}: {len(batch)} documents")
            time.sleep(0.2)  # Sleep 0.2 seconds between batches

    # Print results
    count = tx_collection.count_documents({})
    print(
        f"Stored {count} unique transaction hashes in 'transaction_hashes' collection"
    )
    return count


# Run the function
get_transaction_counts()
# store_transaction_hashes()
