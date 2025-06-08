from pymongo import MongoClient
from pprint import pprint
import csv
from collections import defaultdict


def main():
    # Connect to MongoDB
    client_polymarket = MongoClient("mongodb://localhost:27017/")
    db_polymarket = client_polymarket["polygon_polymarket"]

    client_orderbook = MongoClient(
        "mongodb://localhost:27017/"
    )  # Assuming same instance, different DB
    db_orderbook = client_orderbook["the-graph-polymarket-orderbook"]

    # pnl is a dictionary: pnl[fpmm_address_str][lp_address_str] = profit_loss_value
    pnl = {}

    # Get FPMM addresses to process (e.g., limit 100)
    # Ensure your 'fpmms' collection has 'fpmm_address' and 'condition_id'
    fpmm_docs = list(db_polymarket["fpmms"].find())
    conditions_cursor = db_orderbook["conditions-new"].find()
    conditions_dict = {doc["id"].lower(): doc for doc in conditions_cursor}

    funding_added_cursor = db_polymarket["FPMMFundingAdded"].find()
    funding_added_dict = defaultdict(list)
    for doc in funding_added_cursor:
        if "fpmm_address" in doc and doc["fpmm_address"] is not None:
            funding_added_dict[doc["fpmm_address"].lower()].append(doc)

    funding_removed_cursor = db_polymarket["FPMMFundingRemoved"].find()
    funding_removed_dict = defaultdict(list)
    for doc in funding_removed_cursor:
        if "fpmm_address" in doc and doc["fpmm_address"] is not None:
            funding_removed_dict[doc["fpmm_address"].lower()].append(doc)

    if not fpmm_docs:
        print("No FPMMs found in the 'fpmms' collection to process.")
        return

    for fpmm_doc in fpmm_docs:
        fpmm_address = fpmm_doc.get("fpmm_address").lower()
        condition_id = fpmm_doc.get("conditionId").lower()

        if not fpmm_address or not condition_id:
            print(
                f"Skipping FPMM doc due to missing fpmm_address or condition_id: {fpmm_doc.get('_id')}"
            )
            continue

        # print(f"\nProcessing FPMM: {fpmm_address} with Condition ID: {condition_id}")
        pnl[fpmm_address] = {}

        # 1. Determine Winning Outcome
        outcomes = []
        condition_info = conditions_dict.get(condition_id)

        if condition_info and "payouts" in condition_info and condition_info["payouts"]:
            outcomes = condition_info["payouts"]
        else:
            print(
                f"  Warning: Could not determine winning outcome for Condition ID {condition_id}. Payout data missing or incomplete."
            )
            continue

        # 2. Process FundingAdded events
        # Assuming 'contract_address' in events maps to 'fpmm_address'
        added_events_cursor = funding_added_dict.get(fpmm_address, [])
        for add_event in added_events_cursor:
            lp_address = add_event.get("funder").lower()
            # Assuming 'collateralAmount' is the USDC value provided by the LP
            collateral_added = add_event.get("collateralAmount", 0)
            if not lp_address or not collateral_added:
                continue

            current_pnl = pnl[fpmm_address].get(lp_address, 0)
            pnl[fpmm_address][lp_address] = current_pnl - collateral_added
            # print(f"  LP {lp_address} Added: -{collateral_added}. New PnL: {pnl[fpmm_address][lp_address]}")

        # 3. Process FundingRemoved events
        removed_events_cursor = funding_removed_dict.get(fpmm_address, [])
        for remove_event in removed_events_cursor:
            lp_address = remove_event.get("funder").lower()
            amounts_removed_list = remove_event.get("amountsRemoved", [])
            collateral_from_fee_pool = remove_event.get(
                "collateralRemovedFromFeePool", 0
            )

            if not lp_address:
                continue

            if len(outcomes) != len(amounts_removed_list):
                print(
                    f"  WTF: Length of outcomes {outcomes} does not match length of amountsRemoved {amounts_removed_list} for LP {lp_address} in FPMM {fpmm_address}."
                )
                continue

            value_from_main_assets = 0

            for i, outcome in enumerate(outcomes):
                try:
                    multiplier = float(outcome)
                except (ValueError, TypeError):
                    multiplier = 0.0  # fallback if outcome is not a valid number
                value_from_main_assets += amounts_removed_list[i] * multiplier

            current_pnl = pnl[fpmm_address].get(lp_address, 0)
            pnl[fpmm_address][lp_address] = (
                current_pnl + value_from_main_assets + collateral_from_fee_pool
            )
            # print(f"  LP {lp_address} Removed: +{value_from_main_assets} (assets) + {collateral_from_fee_pool} (fees). New PnL: {pnl[fpmm_address][lp_address]}")

    print("\n--- Final PnL ---")
    pprint(pnl)

    # Clean up: Remove FPMMs with no LP activity to make PnL dict cleaner
    final_pnl_cleaned = {
        fpmm: {lp: lp_pnl / 1_000_000 for lp, lp_pnl in lp_pnls.items()}
        for fpmm, lp_pnls in pnl.items()
        if lp_pnls  # Only include FPMMs that had some LP PnL calculated
    }

    # print("\n--- Final PnL (Cleaned) ---")
    # pprint(final_pnl_cleaned)
    # Print the pnl for each LP in  a map - {LP: ["pnl", "pnl", ...]}

    # Aggregate PnL by LP across all FPMMs
    lp_aggregate = {}

    for fpmm, lp_pnls in final_pnl_cleaned.items():
        for lp, pnl_value in lp_pnls.items():
            if lp not in lp_aggregate:
                lp_aggregate[lp] = []
            lp_aggregate[lp].append((fpmm, pnl_value))

    # Print all FPMMs and PnL for each LP together
    # for lp, fpmm_pnls in lp_aggregate.items():
    #     print(f"LP: {lp}")
    #     for fpmm, pnl_value in fpmm_pnls:
    #         print(f"  FPMM: {fpmm}  PnL: {pnl_value}")
    #     print("-" * 30)

    # Prepare data for CSV: list of (lp, fpmm, pnl_value)
    csv_rows = []
    for lp, fpmm_pnls in lp_aggregate.items():
        # Sort FPMMs for this LP by PnL descending
        sorted_fpmm_pnls = sorted(fpmm_pnls, key=lambda x: x[1], reverse=True)
        for fpmm, pnl_value in sorted_fpmm_pnls:
            csv_rows.append((lp, fpmm, pnl_value))

    # Optionally, sort all rows by LP (not strictly necessary if you want to keep grouped by LP)
    csv_rows.sort(key=lambda x: (x[0], -x[2]))

    # Write to CSV
    with open("AMMpnl.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["lp", "fpmm", "pnl"])
        for row in csv_rows:
            writer.writerow(row)

    print("Results written to AMMpnl.csv")


if __name__ == "__main__":
    main()
