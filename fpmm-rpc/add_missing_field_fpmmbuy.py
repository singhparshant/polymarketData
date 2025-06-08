import time
import warnings
from pymongo import MongoClient, UpdateOne
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from pprint import pprint
from web3.logs import STRICT, IGNORE, DISCARD, WARN
import os

# import ctfabi.json
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
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

w36 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w36")}")
)

w37 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w37")}")
)

w38 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w38")}")
)

w39 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w39")}")
)

w310 = Web3(
    Web3.HTTPProvider(f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv("w310")}")
)


w31.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w32.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w33.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w34.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w35.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w36.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w37.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w38.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w39.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w310.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

# Load ABI from ctfabi.json
ctf_abi = None
script_dir = os.path.dirname(os.path.abspath(__file__))
abi_file_path = os.path.join(script_dir, "ctfabi.json")

try:
    with open(abi_file_path, "r") as f:
        ctf_abi = json.load(f)
except Exception as e:
    print(f"Error loading ctfabi.json: {e}")

providers = [w31, w32, w33, w34, w35, w36, w37, w38, w39, w310]
contracts = [w.eth.contract(abi=ctf_abi) for w in providers]

# Constants
USDC_DECIMALS = 6

# Contract addresses
CONTRACTS = {
    "USDC": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
    "ConditionalTokens": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
}

# Event signatures
EVENT_SIGNATURES = {
    "Transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "Approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    "FPMMBuy": "0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8",
    "FPMMSell": "0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4",
    "FundingAdded": "0xec2dc3e5a3bb9aa0a1deb905d2bd23640d07f107e6ceb484024501aad964a951",
    "FundingRemoved": "0x8b4b2c8ebd04c47fc8bce136a85df9b93fcb1f47c8aa296457d4391519d190e7",
}

global_updates_by_collection = defaultdict(list)
global_updates_lock = threading.Lock()


def convert_hex_bytes_to_hex_str(obj):
    if hasattr(obj, "hex"):
        return obj.hex()
    elif isinstance(obj, (list, tuple)):
        return [convert_hex_bytes_to_hex_str(item) for item in obj]
    elif hasattr(obj, "__dict__"):  # For AttributeDict
        return {k: convert_hex_bytes_to_hex_str(v) for k, v in dict(obj).items()}
    elif isinstance(obj, dict):
        return {k: convert_hex_bytes_to_hex_str(v) for k, v in obj.items()}
    return obj


def convert_attribute_dict_to_dict(receipt):
    receipt_dict = dict(receipt)
    return convert_hex_bytes_to_hex_str(receipt_dict)


def convert_receipt_to_dict(receipt):
    receipt_dict = dict(receipt)

    if "logs" in receipt_dict:
        receipt_dict["logs"] = [dict(log) for log in receipt_dict["logs"]]

    return convert_hex_bytes_to_hex_str(receipt_dict)


def parse_fpmmtrade_logs(
    rich_log, transaction_dict, trade_type, receipt_dict, event_log
):
    trade_info = {}

    trade_info["type"] = trade_type
    # Add transaction details from transaction_dict
    trade_info["blockNumber"] = transaction_dict["blockNumber"]
    trade_info["gas"] = transaction_dict["gas"]
    trade_info["gasPrice"] = transaction_dict["gasPrice"]
    trade_info["transactionIndex"] = transaction_dict["transactionIndex"]
    trade_info["maxFeePerGas"] = transaction_dict.get("maxFeePerGas", 0)
    trade_info["maxPriorityFeePerGas"] = transaction_dict.get("maxPriorityFeePerGas", 0)
    trade_info["sender"] = transaction_dict["from"]

    # Add transaction hash for all trade types
    trade_info["transaction_hash"] = "0x" + convert_hex_bytes_to_hex_str(
        rich_log["transactionHash"]
    )
    trade_info["contract_address"] = rich_log["address"]
    trade_info["fpmm_address"] = rich_log["address"].lower()

    if rich_log["event"] == "FPMMFundingAdded":
        trade_info["funder"] = rich_log["args"]["funder"]
        trade_info["sharesMinted"] = rich_log["args"]["sharesMinted"]
        trade_info["amountsAdded"] = rich_log["args"]["amountsAdded"]
        trade_info["collateralAmount"] = rich_log["args"]["sharesMinted"]

        return trade_info

    if trade_info["type"] == "FundingRemoved":
        trade_info["funder"] = rich_log["args"]["funder"]
        trade_info["sharesBurnt"] = rich_log["args"]["sharesBurnt"]
        trade_info["amountsRemoved"] = rich_log["args"]["amountsRemoved"]
        trade_info["collateralRemovedFromFeePool"] = rich_log["args"][
            "collateralRemovedFromFeePool"
        ]

        return trade_info

    trade_info["trader"] = "0x" + event_log["topics"][1][-40:]

    if trade_info["type"] == "Buy":
        for log in receipt_dict["logs"]:
            topic0 = "0x" + log["topics"][0]
            if (
                log["address"].lower() == CONTRACTS["USDC"].lower()
                and topic0 == EVENT_SIGNATURES["Transfer"]
            ):
                from_address = "0x" + log["topics"][1][-40:]
                if from_address.lower() == trade_info["trader"].lower():
                    trade_info["inputAssetId"] = CONTRACTS["USDC"]
                    trade_info["inputAmount"] = int(log["data"][2:], 16)
                    break

    elif trade_info["type"] == "Sell":
        for log in receipt_dict["logs"]:
            if (
                log["address"].lower() == CONTRACTS["ConditionalTokens"].lower()
                and log["topics"][0]
                == "c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
            ):
                from_address = "0x" + log["topics"][2][-40:]
                if from_address.lower() == trade_info["trader"].lower():
                    data = log["data"]
                    token_id_hex = data[:64].rstrip("0")
                    token_id = int(token_id_hex, 16)
                    value = int(data[64:128], 16)
                    trade_info["inputAssetId"] = token_id
                    trade_info["inputAmount"] = value
                    break

    fpmm_data = event_log["data"][2:]

    if trade_info["type"] == "Buy":
        trade_info["outputAmount"] = rich_log["args"]["outcomeTokensBought"]
        trade_info["CPMMFee"] = rich_log["args"]["feeAmount"]

        for log in receipt_dict["logs"]:
            if (
                log["address"].lower() == CONTRACTS["ConditionalTokens"].lower()
                and log["topics"][0]
                == "c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
            ):
                to_addr = "0x" + log["topics"][3][-40:]
                if to_addr.lower() == trade_info["trader"].lower():
                    data = log["data"]
                    token_id_hex = data[:64].rstrip("0")
                    token_id = int(token_id_hex, 16)
                    trade_info["outputAssetId"] = token_id
                    break

    else:  # Sell
        return_amount = int(fpmm_data[:64][:-2], 16)
        fee_amount = int(fpmm_data[64:128][:-2], 16)
        outcome_token_sold = int(fpmm_data[128:][:-2], 16)
        trade_info["outputAmount"] = return_amount
        trade_info["CPMMFee"] = fee_amount
        trade_info["outputAssetId"] = CONTRACTS["USDC"]

    return trade_info


def get_trade_info_from_hash(tx_hash, w3, my_contract):
    processed_events_info = []
    try:
        tx = w3.eth.get_transaction(tx_hash)
        time.sleep(0.1)
        tx_dict = convert_attribute_dict_to_dict(tx)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        receipt_dict = convert_receipt_to_dict(receipt)  # For parse_fpmmtrade_logs

        for log_entry in receipt.logs:
            if not log_entry.get("topics"):
                continue

            event_signature = "0x" + log_entry["topics"][0].hex()

            event_processor = None
            trade_type = None

            if event_signature == EVENT_SIGNATURES["FPMMBuy"]:
                event_processor = my_contract.events.FPMMBuy().process_receipt(
                    receipt, errors=DISCARD
                )
                trade_type = "Buy"
            elif event_signature == EVENT_SIGNATURES["FPMMSell"]:
                event_processor = my_contract.events.FPMMSell().process_receipt(
                    receipt, errors=DISCARD
                )
                trade_type = "Sell"
            elif event_signature == EVENT_SIGNATURES["FundingAdded"]:
                event_processor = my_contract.events.FPMMFundingAdded().process_receipt(
                    receipt, errors=DISCARD
                )
                trade_type = "FundingAdded"
            elif event_signature == EVENT_SIGNATURES["FundingRemoved"]:
                event_processor = (
                    my_contract.events.FPMMFundingRemoved().process_receipt(
                        receipt, errors=DISCARD
                    )
                )
                trade_type = "FundingRemoved"

            if event_processor and trade_type:
                try:

                    # Convert the raw log_entry to dict with hex strings for parse_fpmmtrade_logs
                    current_event_log_dict = convert_hex_bytes_to_hex_str(log_entry)

                    trade_info = parse_fpmmtrade_logs(
                        event_processor[0],  # Decoded event data for this specific log
                        tx_dict,
                        trade_type,
                        receipt_dict,  # Full receipt dict (used for finding related transfers)
                        current_event_log_dict,  # Raw data of this specific log
                    )
                    if trade_info:
                        processed_events_info.append(trade_info)
                except Exception as e:
                    print(
                        f"Error processing log for {trade_type} in {tx_hash} at log index {log_entry.get('logIndex')}: {e}"
                    )
                    processed_events_info.append(
                        {
                            "error": f"Log processing error for {trade_type}: {str(e)}",
                            "transaction_hash": tx_hash,
                            "log_index": log_entry.get("logIndex"),
                            "type": "log_processing_error",
                        }
                    )

        return processed_events_info  # Returns a list of all processed events, or an empty list

    except Exception as e:
        print(f"Error fetching transaction or receipt for {tx_hash}: {e}")
        return [
            {  # Return a list with a single error object
                "error": str(e),
                "transaction_hash": tx_hash,
                "type": "transaction_processing_error",
            }
        ]


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


def process_single_hash(tx_hash, w3, my_contract):
    try:
        processed_events = get_trade_info_from_hash(tx_hash, w3, my_contract)
        if not processed_events:
            return

        for event_info in processed_events:
            if "error" in event_info:
                # If there's an error for one event, return the error for the hash
                return (tx_hash, f"Error processing an event: {event_info['error']}")

            trade_type = event_info.get("type")
            if not trade_type:
                return (tx_hash, "Event info missing 'type' field")

            # collection_name = f"FPMM{trade_type}"
            # collection = db[collection_name]

            sanitized_event_info = sanitize_for_mongodb(event_info)
            event_tx_hash = sanitized_event_info.get("transaction_hash")
            if not event_tx_hash:
                return (
                    tx_hash,
                    "Event info missing 'transaction_hash' after sanitization",
                )

            # collection.update_one(
            #     {"transaction_hash": event_tx_hash},
            #     {"$set": sanitized_event_info},
            #     upsert=True,
            # )

            # time.sleep(0.3)  # Reduced sleep slightly

        # Return success after processing all events for this hash
        return (sanitized_event_info, None)
    except Exception as e:
        # Catch exceptions during get_trade_info_from_hash or other steps
        return (tx_hash, f"Exception processing hash: {str(e)}")


def process_transaction_hashes(hashes, w3, my_contract, pbar):
    transaction_hashes_dict = {}
    print(
        f"thread index is {threading.get_ident()} and provider is {w3.provider.endpoint_uri} and batch size is {len(hashes)}"
    )
    for hash in hashes:
        try:
            event_info, error = process_single_hash(hash, w3, my_contract)
            if error:
                # transaction_hashes_dict[hash] = error
                pass
            else:
                transaction_hashes_dict[hash] = event_info
        except Exception as e:
            print(f"Error processing hash {hash}: {e}")
        if pbar:
            pbar.update(1)
        time.sleep(0.05)
    return transaction_hashes_dict


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017/")
    db = client["polygon_polymarket"]
    fpmmBuy = db["FPMMBuy"]

    # Find FPMMBuy docs missing inputAmount or inputAssetId
    messedup_hashes = list(
        fpmmBuy.find(
            {
                "$or": [
                    {"inputAmount": {"$exists": False}},
                    {"inputAssetId": {"$exists": False}},
                ],
                # "transaction_hash": "0x27dc54f8b0cb0dc64092eee794f06c069356010affcf251ebf61491c88e8464b",
            },
            {"transaction_hash": 1},
        )
    )  # Adjust limit as needed

    hashes = [doc["transaction_hash"] for doc in messedup_hashes]

    num_providers = len(providers)
    n = len(hashes)
    chunk_size = (n + num_providers - 1) // num_providers

    # Batch the hashes
    batches_chunks = [
        hashes[i * chunk_size : (i + 1) * chunk_size] for i in range(num_providers)
    ]

    batches_chunks = [batch for batch in batches_chunks if batch]

    print(f"Processing {len(hashes)} divided into {len(batches_chunks)} chunks")
    transaction_hashes_dict = {}

    pbar = tqdm(total=len(hashes), desc="Processing batches")

    with ThreadPoolExecutor(max_workers=num_providers) as executor:
        futures = []
        for i, chunk in enumerate(batches_chunks):
            provider_idx = i % num_providers
            # Use partial to pass the same pbar to each thread
            futures.append(
                executor.submit(
                    process_transaction_hashes,
                    chunk,
                    providers[provider_idx],
                    contracts[provider_idx],
                    pbar,
                ),
            )

        for future in as_completed(futures):
            result = future.result()
            if result:
                transaction_hashes_dict.update(result)

    pbar.close()

    print(f"Fetched timestamps for {len(transaction_hashes_dict)} blocks.")
    # Optionally handle blocks that failed to fetch timestamps

    print("Generating update operations and performing bulk writes...")
    updates = []
    bulk_write_chunk_size = 2000  # Define bulk write size
    total_updated = 0

    count = 0
    for event in tqdm(messedup_hashes, desc="Generating updates"):

        event_tx_hash = event.get("transaction_hash")
        event_id = event.get("_id")  # Use the MongoDB _id for updates

        if (
            event_tx_hash is not None
            and event_id is not None
            and event_tx_hash in transaction_hashes_dict
        ):
            event_info = transaction_hashes_dict[event_tx_hash]
            if (
                event_info.get("inputAmount") is None
                or event_info.get("inputAssetId") is None
            ):
                count += 1
                continue

            # Use _id for efficient updates
            updates.append(
                UpdateOne(
                    {"_id": event_id},
                    {
                        "$set": {
                            "inputAmount": event_info["inputAmount"],
                            "inputAssetId": event_info["inputAssetId"],
                        }
                    },
                )
            )

            # Perform bulk write when chunk size is reached
            if len(updates) >= bulk_write_chunk_size:
                try:
                    result = db.FPMMBuy.bulk_write(updates, ordered=False)
                    total_updated += result.modified_count
                    # print(f"Bulk wrote {result.modified_count} timestamp updates.")
                except Exception as e:
                    print(f"Error during bulk write: {e}")
                    # Handle bulk write errors (e.g., log failed updates)
                updates = []

    # Perform final bulk write for remaining updates
    if updates:
        try:
            result = db.FPMMBuy.bulk_write(updates, ordered=False)
            total_updated += result.modified_count
            # print(f"Final bulk wrote {result.modified_count} timestamp updates.")
        except Exception as e:
            print(f"Error during final bulk write: {e}")
            # Handle bulk write errors

    print(
        f"Finished populating timestamps. Total events potentially updated: {total_updated}"
    )
    print(f"Count of events that were not updated: {count}")
