from math import log
import time
import warnings
from pymongo import MongoClient, UpdateOne
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from pprint import pp, pprint
from web3.logs import STRICT, IGNORE, DISCARD, WARN
import os

# import ctfabi.json
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
from collections import defaultdict
from dotenv import load_dotenv

# w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
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
    # Add safety checks
    if not rich_log or not rich_log.get("args"):
        print(f"Warning: rich_log or rich_log.args is None for trade_type {trade_type}")
        return None

    if not transaction_dict:
        print(f"Warning: transaction_dict is None for trade_type {trade_type}")
        return None

    if not receipt_dict or not receipt_dict.get("logs"):
        print(
            f"Warning: receipt_dict or receipt_dict.logs is None for trade_type {trade_type}"
        )
        return None

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
        # trade_info["collateralAmount"] = rich_log["args"]["sharesMinted"]

        collateral_found = False
        for log in receipt_dict["logs"]:
            topic0 = "0x" + log["topics"][0]
            if (
                # log["address"].lower() == CONTRACTS["USDC"].lower()
                topic0 == EVENT_SIGNATURES["Transfer"]
                and trade_info["funder"].lower()
                == "0x" + log["topics"][1][-40:].lower()
                and trade_info["fpmm_address"].lower()
                == "0x" + log["topics"][2][-40:].lower()
            ):
                trade_info["collateralAmount"] = int(log["data"][2:], 16)
                collateral_found = True
        if not collateral_found:
            print(
                f"No collateralAmount added for tx hash {trade_info.get('transaction_hash', 'unknown')}"
            )
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

    # if trade_info["type"] == "Buy":
    #     trade_info["inputAmount"] = rich_log["args"]["investmentAmount"]
    #     trade_info["inputAssetId"] = CONTRACTS["USDC"]
    # for log in receipt_dict["logs"]:
    #     topic0 = "0x" + log["topics"][0]
    #     if (
    #         log["address"].lower() == CONTRACTS["USDC"].lower()
    #         and topic0 == EVENT_SIGNATURES["Transfer"]
    #     ):
    #         from_address = "0x" + log["topics"][1][-40:]
    #         if from_address.lower() == trade_info["trader"].lower():
    #             trade_info["inputAssetId"] = CONTRACTS["USDC"]
    #             trade_info["inputAmount"] = int(log["data"][2:], 16)
    #             break

    if trade_info["type"] == "Sell":
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
        trade_info["inputAmount"] = rich_log["args"]["investmentAmount"]
        trade_info["inputAssetId"] = CONTRACTS["USDC"]

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
        time.sleep(0.2)
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

            if event_processor is not None and trade_type is not None:
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
                    # print("Trade info processed:", trade_info)
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


def process_single_hash(tx_hash, db, w3, my_contract):
    try:
        processed_events = get_trade_info_from_hash(tx_hash, w3, my_contract)
        if not processed_events:
            return (tx_hash, "No events found for this transaction hash")

        for event_info in processed_events:
            if "error" in event_info:
                # If there's an error for one event, return the error for the hash
                return (tx_hash, f"Error processing an event: {event_info['error']}")

            trade_type = event_info.get("type")
            # print(f"db is {db}, trade_type is {trade_type}, event_info is {event_info}")
            if not trade_type:
                return (tx_hash, "Event info missing 'type' field")

            collection_name = f"FPMM{trade_type}"
            collection = db[collection_name]

            sanitized_event_info = sanitize_for_mongodb(event_info)
            event_tx_hash = sanitized_event_info.get("transaction_hash")
            if not event_tx_hash:
                return (
                    tx_hash,
                    "Event info missing 'transaction_hash' after sanitization",
                )

            # print(f"Updating {event_tx_hash} in {collection_name}")
            # collection.update_one(
            #     {"transaction_hash": event_tx_hash},
            #     {"$set": sanitized_event_info},
            #     upsert=True,
            # )
            if not collection.find_one({"transaction_hash": event_tx_hash}):
                print(f"Adding new event {event_tx_hash} to {collection_name}")
                collection.update_one(
                    {"transaction_hash": event_tx_hash},
                    {"$set": sanitized_event_info},
                    upsert=True,
                )
            time.sleep(0.2)  # Reduced sleep slightly

        # Return success after processing all events for this hash
        return (tx_hash, None)
    except Exception as e:
        # Catch exceptions during get_trade_info_from_hash or other steps
        return (tx_hash, f"Exception processing hash: {str(e)}")


def process_transaction_hashes_parallel(
    hashes=[], db=None, fpmm_address=None, max_workers=5
):
    # if db is None:
    #     client = MongoClient("mongodb://localhost:27017/")
    #     db = client["polygon_polymarket"]
    # if hashes is None:
    #     hashes = ["0x1b31ac5fdd8dc3695d224d5d7199867fb7476e50ba0a771539f7132443d198bf"]

    error_hashes = []
    lock = threading.Lock()
    processed_count = 0
    last_hash = None

    def worker(args):
        try:
            tx_hash, idx = args
            # print(f"Worker started for {tx_hash}")
            w3 = providers[idx % len(providers)]
            my_contract = contracts[idx % len(contracts)]
            result = process_single_hash(tx_hash, db, w3, my_contract)
            # print(f"process_single_hash returned for {tx_hash}")
            time.sleep(0.1)
            with lock:
                nonlocal processed_count, last_hash
                processed_count += 1
                last_hash = tx_hash
                if result[1] is not None:
                    error_hashes.append((result[0], result[1]))
            return tx_hash
        except Exception as e:
            print(f"Exception in worker for hash {args[0]}: {e}")
            raise

    total_hashes = len(hashes)
    # print(f"Submitting {total_hashes} hashes to the executor")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(worker, (tx_hash, i)) for i, tx_hash in enumerate(hashes)
        ]
        # for _ in tqdm(as_completed(futures), total=total_hashes, desc="Processing"):
        #     pass

    if error_hashes:
        print("Errors encountered for the following hashes:")
        for tx_hash, error in error_hashes:
            print(f"{tx_hash}: {error}")


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017/")
    db = client["polygon_polymarket"]

    # hashes = list(db.FPMMFundingAdded.find({}, {"transaction_hash": 1, "_id": 0}))
    # hashes = [h["transaction_hash"] for h in hashes]
    hashes = [
        "0x839f073985d679ec0a72876f4ad4cf9f85eb8b6176bbf5e936e18ef8c5a0d2d0",
        "0xe024429928f2cbf64ac73838d287e6df378965bac1bd0865b58edfe9e2e7016a",
        "0xffe3d93a005492c8e1ae97ce2dadda8d74725923b0d9339b92db745a2a9ab952",
        "0x23eb20194b0202951b11f6075593b68ef038ba4f6944b39cfa2c2f4c90db4bfb",
    ]

    total_hashes = len(hashes)
    print(f"Total transaction hashes to process: {total_hashes}")
    process_transaction_hashes_parallel(hashes=hashes, db=db)
