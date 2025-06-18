# This script populates timestamps to event documents in a MongoDB collection using block numbers
# Change the collection accordingly which you want to update.

from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
from json import load
import pprint
import threading
import requests
import time
from pymongo import MongoClient, UpdateOne
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from tqdm import tqdm
import os
from collections import defaultdict
from functools import partial
from dotenv import load_dotenv

db = MongoClient("mongodb://localhost:27017/")["polygon_polymarket"]

load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

w31 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w31")}")
)

w32 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w32")}")
)

w33 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w33")}")
)

w34 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w34")}")
)

w35 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w35")}")
)

w31.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w32.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w33.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w34.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w35.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

w3_providers = [w31, w32, w33, w34, w35]


def get_contract_creation_hash(fpmm_addresses):
    url = f"https://api.polygonscan.com/api?module=contract&action=getcontractcreation&contractaddresses={fpmm_addresses}&apikey={POLYGON_API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data.get("result", []) if data.get("status") == "1" else []


def add_creation_hashes():
    # Get all FPMMs without creation hash
    all_fpmms = list(
        db.fpmms.find(
            {"creation_transaction_hash": {"$exists": False}}, {"fpmm_address": 1}
        )
    )

    # Process in chunks of 5
    for i in range(0, len(all_fpmms), 5):
        batch = all_fpmms[i : i + 5]
        addresses = ",".join([f["fpmm_address"] for f in batch])

        results = get_contract_creation_hash(addresses)

        for result in results:
            db.fpmms.update_one(
                {"fpmm_address": result["contractAddress"].lower()},
                {"$set": {"creation_transaction_hash": result["txHash"]}},
            )
        print(f"Processed {i} of {len(all_fpmms)}")
        time.sleep(0.2)


def add_creation_timestamps():
    # Get all FPMMs without creation timestamp
    all_fpmms = list(
        db.fpmms.find(
            {
                "creation_timestamp": {"$exists": False},
                "creation_block_number": {"$exists": False},
                "creation_transaction_hash": {"$exists": True},
            }
        )
    )

    updates = []
    count = 0

    for fpmm in all_fpmms:
        tx = w31.eth.get_transaction(fpmm["creation_transaction_hash"])
        blockNumber = tx["blockNumber"]
        block = w32.eth.get_block(blockNumber)
        timestamp = block["timestamp"]

        updates.append(
            UpdateOne(
                {"fpmm_address": fpmm["fpmm_address"]},
                {
                    "$set": {
                        "creation_timestamp": timestamp,
                        "creation_block_number": blockNumber,
                        "creation_date": datetime.datetime.fromtimestamp(
                            timestamp
                        ).strftime("%Y-%m-%d"),
                    }
                },
            )
        )

        count += 1
        print(f"Processed {count} of {len(all_fpmms)}")
        # Bulk update all at once
        if len(updates) >= 100:
            result = db.fpmms.bulk_write(updates)
            print(f"Updated {result.modified_count} records")
            updates = []


def get_active_clob_fpmms():
    all_fpmms = list(
        db.fpmms.find(
            # {"creation_date": {"$gte": "2024-01-01"}, "total_interactions": {"$eq": 0}}
            {"creation_date": {"$gte": "2023-01-01"}, "total_interactions": {"$gt": 0}}
        )
    )
    pprint.pprint(all_fpmms[0])
    checksum_address = Web3.to_checksum_address(
        "0x0095a54a4c548362d69a5aea8d2f1664f86cfdc6"
    )
    tx_count = w31.eth.get_transaction_count(checksum_address)
    print(tx_count)


def fetch_block_timestamps(block_numbers, w3, pbar):
    """Fetches timestamps for a list of block numbers using a single w3 provider."""
    block_timestamps = {}
    for block_num in block_numbers:
        try:
            block = w3.eth.get_block(block_num)
            block_timestamps[block_num] = block["timestamp"]
            time.sleep(0.1)  # Small delay to avoid rate limits
        except Exception as e:
            print(f"Error fetching block {block_num}: {e}")
            # Handle error, maybe store failed block numbers or skip
            pass  # Or handle more robustly
        if pbar:
            pbar.update(1)
    return block_timestamps


def populate_timestamps_for_events():
    client = MongoClient(
        "mongodb://localhost:27017/"
    )  # Assuming client is initialized here or globally
    db = client["polygon_polymarket"]

    print("Fetching events without timestamps...")
    # Fetch events, but only the blockNumber and _id
    # We fetch transaction_hash as well, as it might be indexed and used elsewhere
    events_to_update = list(
        db.FPMMFundingRemoved.find(
            {"timestamp": {"$exists": False}}, {"blockNumber": 1, "transaction_hash": 1}
        ).limit(200000)
    )
    print(f"Found {len(events_to_update)} events requiring timestamp updates.")

    if not events_to_update:
        print("No events found without timestamps. Exiting.")
        return

    # Get unique block numbers
    unique_block_numbers = list(
        set(
            event.get("blockNumber")
            for event in events_to_update
            if event.get("blockNumber") is not None
        )
    )
    print(f"Found {len(unique_block_numbers)} unique block numbers.")

    if not unique_block_numbers:
        print("No valid block numbers found in events. Exiting.")
        return

    # Divide unique block numbers evenly among providers
    num_providers = len(w3_providers)  # Use your global providers list
    n = len(unique_block_numbers)
    # Calculate chunk size, ensuring roughly even distribution
    chunk_size = (n + num_providers - 1) // num_providers
    block_number_chunks = [
        unique_block_numbers[i * chunk_size : (i + 1) * chunk_size]
        for i in range(num_providers)
    ]

    # Filter out any empty chunks that might result if n < num_providers
    block_number_chunks = [chunk for chunk in block_number_chunks if chunk]

    print(
        f"Fetching timestamps for {len(unique_block_numbers)} unique blocks divided into {len(block_number_chunks)} chunks..."
    )
    block_timestamp_map = {}
    error_block_numbers = []

    total_blocks = len(unique_block_numbers)
    pbar = tqdm(total=total_blocks, desc="Fetching block timestamps (blocks)")

    with ThreadPoolExecutor(max_workers=num_providers) as executor:
        futures = []
        for i, chunk in enumerate(block_number_chunks):
            provider_idx = i % num_providers
            # Use partial to pass the same pbar to each thread
            futures.append(
                executor.submit(
                    fetch_block_timestamps, chunk, w3_providers[provider_idx], pbar
                )
            )

        for future in as_completed(futures):
            result = future.result()
            if result:
                block_timestamp_map.update(result)

    pbar.close()

    print(f"Fetched timestamps for {len(block_timestamp_map)} blocks.")
    # Optionally handle blocks that failed to fetch timestamps

    print("Generating update operations and performing bulk writes...")
    updates = []
    bulk_write_chunk_size = 2000  # Define bulk write size
    total_updated = 0

    for event in tqdm(events_to_update, desc="Generating updates"):
        block_num = event.get("blockNumber")
        event_id = event.get("_id")  # Use the MongoDB _id for updates

        if (
            block_num is not None
            and event_id is not None
            and block_num in block_timestamp_map
        ):
            timestamp = block_timestamp_map[block_num]
            # Use _id for efficient updates
            updates.append(
                UpdateOne({"_id": event_id}, {"$set": {"timestamp": timestamp}})
            )

            # Perform bulk write when chunk size is reached
            if len(updates) >= bulk_write_chunk_size:
                try:
                    result = db.FPMMFundingRemoved.bulk_write(updates, ordered=False)
                    total_updated += result.modified_count
                    # print(f"Bulk wrote {result.modified_count} timestamp updates.")
                except Exception as e:
                    print(f"Error during bulk write: {e}")
                    # Handle bulk write errors (e.g., log failed updates)
                updates = []

    # Perform final bulk write for remaining updates
    if updates:
        try:
            result = db.FPMMFundingRemoved.bulk_write(updates, ordered=False)
            total_updated += result.modified_count
            # print(f"Final bulk wrote {result.modified_count} timestamp updates.")
        except Exception as e:
            print(f"Error during final bulk write: {e}")
            # Handle bulk write errors

    print(
        f"Finished populating timestamps. Total events potentially updated: {total_updated}"
    )


if __name__ == "__main__":
    # add_creation_hashes()
    # add_creation_timestamps()
    # get_active_clob_fpmms()
    populate_timestamps_for_events()
