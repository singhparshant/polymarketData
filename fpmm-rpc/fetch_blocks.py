from json import load
from pymongo import MongoClient
import requests
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from datetime import datetime
from pprint import pprint
import time
from dotenv import load_dotenv
import os

load_dotenv()

# API and RPC configuration
POLYGON_API_KEY = [
    os.getenv("POLYGON_API_KEY"),
    os.getenv("POLYGON_API_KEY_2"),
]
POLYGON_API_URL = "https://api.polygonscan.com/api"
RPC_URL = "https://polygon-rpc.com"

# Initialize Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))
w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)


def fetch_token_transfers(address, contract_address=None):
    """Fetch ERC-20 token transfers for a given address from Polygon."""
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": "0",
        "endblock": "99999999",
        "page": "1",
        "offset": "10000",
        "sort": "desc",
        "apikey": POLYGON_API_KEY,
    }

    if contract_address:
        params["contractaddress"] = contract_address

    response = requests.get(POLYGON_API_URL, params=params)
    return response.json()


def convert_to_json_serializable(obj):
    """Convert Web3 objects to JSON serializable format."""
    if hasattr(obj, "hex"):  # Handles HexBytes and similar objects
        return obj.hex()
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    elif hasattr(obj, "__dict__"):  # For AttributeDict
        return {k: convert_to_json_serializable(v) for k, v in dict(obj).items()}
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    return obj


def get_transaction_details(tx_hash):
    """Get detailed transaction information using Web3."""
    tx = w3.eth.get_transaction(tx_hash)
    receipt = w3.eth.get_transaction_receipt(tx_hash)

    tx_details = {
        "transaction": dict(tx),
        "receipt": dict(receipt),
        "block": dict(w3.eth.get_block(tx["blockNumber"])),
        "timestamp": datetime.fromtimestamp(
            w3.eth.get_block(tx["blockNumber"])["timestamp"]
        ).strftime("%Y-%m-%d %H:%M:%S"),
    }

    return convert_to_json_serializable(tx_details)


def fetch_all_token_transfers(address, contract_address=None, thread_idx=0):
    """Fetch all token transfers with pagination."""
    all_transfers = []
    page = 1
    # print("Fetching all token transfers for address: ", address)
    while True:
        params = {
            "module": "account",
            "action": "tokentx",
            "address": address,
            "startblock": "0",
            "endblock": "99999999",
            "page": str(page),
            "offset": "1000",  # Maximum 5000 records per page
            "sort": "desc",
            "apikey": POLYGON_API_KEY[page % len(POLYGON_API_KEY)],
        }

        if contract_address:
            params["contractaddress"] = contract_address

        response = requests.get(POLYGON_API_URL, params=params)
        result = response.json()
        # print("Result: ", result)

        if result["status"] != "1":
            print(f"Error fetching page {page}: {result['message']}")
            break

        transfers = result["result"]
        if not transfers:  # No more transfers to fetch
            break

        all_transfers.extend(transfers)
        # print(f"Fetched page {page} ({len(transfers)} transfers)")

        if len(transfers) < 1000:  # Last page
            break

        page += 1
        time.sleep(0.1)  # Add small delay to avoid rate limiting

    return all_transfers


def main():
    """Fetch and display token transfers for a specific address."""

    address = "0x031f449787e7853ec556c8c8fc4299650f7a8c6f"
    transfers = fetch_all_token_transfers(address)
    print(len(transfers))
    # Connect to mongodb polygon_polymarket db
    # client = MongoClient("mongodb://localhost:27017/")
    # db = client["polygon_polymarket"]

    # Get all FPMM addresses from the fpmms collection
    # fpmm_addresses = db.fpmms.find({}, {"_id": 0, "fpmm_address": 1})

    # for fpmm_address in fpmm_addresses:
    #     print(
    #         "Fetching all token transfers for address: ", fpmm_address["fpmm_address"]
    #     )
    #     try:
    #         transfers = fetch_all_token_transfers(fpmm_address["fpmm_address"])
    #         if len(transfers) > 10000:
    #             print(fpmm_address["fpmm_address"])
    #             break
    #     except Exception as e:
    #         print(e)


if __name__ == "__main__":
    main()
