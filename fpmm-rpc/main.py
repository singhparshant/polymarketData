from pymongo import MongoClient
from web3 import Web3
from fetch_blocks import fetch_all_token_transfers, fetch_token_transfers
from fetch_transactions import (
    get_trade_info_from_hash,
    process_transaction_hashes_parallel,
)
from pprint import pprint, pformat
import time
from tqdm import tqdm
import signal
import sys
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from web3.middleware import ExtraDataToPOAMiddleware


def connect_to_mongodb():
    """Connect to MongoDB and return the database."""
    client = MongoClient("mongodb://localhost:27017/")
    return client["polygon_polymarket"]


def get_last_processed_address(db):
    """Get the last processed FPMM address from the fpmms collection."""
    last_fpmm = db.fpmms.find_one(sort=[("fpmm_address", -1)])
    return last_fpmm["fpmm_address"] if last_fpmm else "0x0"


def sanitize_for_mongodb(data):
    """Convert large integers to strings to avoid MongoDB limitations."""
    if isinstance(data, dict):
        return {k: sanitize_for_mongodb(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_for_mongodb(v) for v in data]
    elif isinstance(data, int):
        # Convert large integers to strings
        if abs(data) > 2**63 - 1:
            return str(data)
    return data


# def cleanup_current_fpmm(db, fpmm_address):
#     """
#     Clean up all data related to the current FPMM address being processed.
#     """
#     collections = ["FPMMBuy", "FPMMSell", "FundingAdded", "FundingRemoved", "fpmms"]
#     for collection in collections:
#         db[collection].delete_many({"fpmm_address": fpmm_address})
#     print(f"\nCleaned up data for FPMM: {fpmm_address}")


def signal_handler(signum, frame):
    """
    Handle cleanup when the script is interrupted.
    """
    print("\nInterrupt received. Cleaning up before exit...")
    # if hasattr(signal_handler, "current_fpmm"):
    #     db = connect_to_mongodb()
    #     cleanup_current_fpmm(db, signal_handler.current_fpmm)
    sys.exit(1)


def process_single_fpmm(
    doc, target_db, existing_hashes_set, max_retries=3, thread_idx=0
):
    """Process a single FPMM with retries."""
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Store current FPMM address in signal handler
            signal_handler.current_fpmm = doc["fpmm_address"]

            try:
                transfers = fetch_all_token_transfers(doc["fpmm_address"], thread_idx)
            except Exception as e:
                if "Result window is too large" in str(e):
                    print(
                        f"\nNote: Hit 10k transfer limit for {doc['fpmm_address']}, processing available transfers"
                    )
                    transfers = getattr(e, "partial_results", [])
                else:
                    print(
                        f"Error fetching transfers for {doc['fpmm_address']}: {str(e)}"
                    )
                    raise e

            # Process transfers if they exist
            unique_transfers = {}
            for transfer in transfers:
                tx_hash = transfer.get("hash")
                if tx_hash and tx_hash not in unique_transfers:
                    unique_transfers[tx_hash] = transfer

            transfers = list(unique_transfers.values())

            transfer_hashes = [
                transfer.get("hash")
                for transfer in transfers
                if transfer.get("hash") not in existing_hashes_set
            ]
            process_transaction_hashes_parallel(transfer_hashes)

            return True

        except Exception as e:
            print(f"Error processing {doc['fpmm_address']}: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                print(
                    f"Retrying FPMM {doc['fpmm_address']}... (Attempt {retry_count + 1} of {max_retries})"
                )
                time.sleep(5)  # Wait 5 seconds before retrying
            else:
                print(
                    f"Max retries reached for FPMM {doc['fpmm_address']}. Skipping..."
                )
                return False


def process_fpmm_addresses(batch_size=100, num_threads=4):
    """
    Fetch FPMM addresses and process them using multiple threads.
    """
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Connect to source database
    client = MongoClient("mongodb://localhost:27017/")
    db = client["polygon_polymarket"]
    existing_hashes = list(db.transaction_hashes.find())
    existing_hashes_set = {hash["transaction_hash"] for hash in existing_hashes}

    processed_count = 0
    count_lock = threading.Lock()

    def process_batch(batch_docs, thread_idx):
        nonlocal processed_count  # So we can modify the outer variable
        for doc in batch_docs:
            success = process_single_fpmm(doc, db, existing_hashes_set, thread_idx)
            if success:
                with count_lock:
                    processed_count += 1
                    print(
                        f"(Total processed: {processed_count}) [Thread {thread_idx}] --------- Processed {doc['fpmm_address']} "
                    )
            time.sleep(0.2)

    try:
        # while True:
        # Fetch next batch of documents
        batch = list(
            db.fpmms.find(
                {
                    "total_interactions": {"$gt": 0},
                    "fpmm_address": {
                        "$gt": "0x041b005181c5375bf9687465f28899c98d1ab0db"
                    },
                },
                sort=[("fpmm_address", 1)],
            )
            # .limit(batch_size * num_threads)  # Fetch enough for all threads
        )

        print(f"Processing batch of {len(batch)} documents with {num_threads} threads")

        # Split batch into sub-batches for threads
        sub_batches = [[] for _ in range(num_threads)]
        for i, doc in enumerate(batch):
            sub_batches[i % num_threads].append(doc)

        # Process sub-batches in parallel
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(process_batch, sub_batch, thread_idx)
                for thread_idx, sub_batch in enumerate(sub_batches)
                if sub_batch
            ]
            for future in futures:
                future.result()

            # time.sleep(0.2)  # Brief pause between batches

    except Exception as e:
        print(f"Database error: {str(e)}")
        raise e

    # Print the final count after all threads are done
    print(f"Total processed addresses: {processed_count}")


def get_fpmm_info(fpmm_address):
    """Get FPMM info from the polygon/rpc host and just print it"""
    RPC_URL = "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    fpmm_info = w3.eth.get  # get_transaction(fpmm_address)
    print(fpmm_info)


def main():
    """Main function to control the script execution."""

    try:
        # res = fetch_token_transfers("0x01a4333b6aCb5091cF0219646f35E289546F4656")
        # with open("last_process_hash.txt", "w") as f:
        #     f.write(pformat(res))
        process_fpmm_addresses()
        return 0
    except Exception as e:
        pprint({"error": str(e), "type": "main_execution_error"})
        return 1


if __name__ == "__main__":
    exit(main())
