from itertools import chain
import os
from pprint import pprint
import re
from time import sleep
import requests
from web3 import Web3
from web3.exceptions import BlockNotFound
from dotenv import load_dotenv
from main import connect_to_mongodb
from fetch_transactions import process_transaction_hashes_parallel

load_dotenv()


def main():
    """Main function to fetch and process AMM addresses from a file and query the Chainbase API."""

    mongo_client = connect_to_mongodb()
    db = mongo_client["polygon_polymarket"]
    existing_hashes = set(
        doc["transaction_hash"]
        for collection in ["FPMMFundingRemoved", "FPMMBuy", "FPMMSell"]
        for doc in db[collection].find({}, {"transaction_hash": 1, "_id": 0})
    )

    # Read the file and extract AMM addresses
    relevant_amms = []

    try:
        with open("../calculations/relevant_amms.txt", "r") as file:
            for line in file:
                # Look for lines containing "Processed" followed by a hex address
                match = re.search(r"Processed\s+(0x[a-fA-F0-9]+)", line.strip())
                if match:
                    amm_address = match.group(
                        1
                    ).lower()  # Convert to lowercase for consistency
                    relevant_amms.append(amm_address)

        print(
            f"Successfully extracted {len(relevant_amms)} AMM addresses from relevant_amms.txt"
        )
        # print(f"First few addresses: {relevant_amms[:5]}")

    except FileNotFoundError:
        print(
            "relevant_amms.txt file not found. Please make sure the file exists in the current directory."
        )
    except Exception as e:
        print(f"Error reading file: {e}")

    chainbase_base_url = "https://api.chainbase.online/v1/token/transfers"
    headers = {"x-api-key": os.getenv("chainbase_api_key")}
    relevant_amms.sort()  # Sort addresses for consistent processing
    for idx, fpmm in enumerate(relevant_amms):
        all_funding_removal_events = []  # Collect all events first
        page = 1

        while True:
            params = {
                "page": page,
                "limit": 100,
                "chain_id": 137,
                "contract_address": fpmm,
                # "from_block": 14444919,
                # "to_block": 14444921,
            }

            response = requests.get(chainbase_base_url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error fetching transfers for {fpmm}")
                break

            data = response.json()

            # Check if we have data and if there are more pages
            if not data.get("data") or len(data["data"]) == 0:
                break

            funding_removal_events = [
                t["transaction_hash"]
                for t in data["data"]
                if t["to_address"] == "0x0000000000000000000000000000000000000000"
            ]

            # Append to the collected events
            all_funding_removal_events.extend(funding_removal_events)

            # Check if this was the last page
            if len(data["data"]) < 100:  # Less than limit means last page
                break

            page += 1
            sleep(0.1)

        # Now subtract existing hashes from all collected events
        new_hashes_to_add = list(set(all_funding_removal_events) - existing_hashes)
        try:
            process_transaction_hashes_parallel(new_hashes_to_add, mongo_client)
            print(
                f"Processed {len(new_hashes_to_add)} new funding removal events for {fpmm}. Completed {idx + 1}/{len(relevant_amms)}"
            )
        except Exception as e:
            print(f"Error processing transactions for {fpmm}: {e}")


if __name__ == "__main__":
    main()
