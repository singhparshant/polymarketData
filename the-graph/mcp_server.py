from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from utils.database import database_connection
from pymongo import MongoClient
import os

# Load MONGO_URI (and any other env vars) from .env
load_dotenv()

# Initialize the MCP server (this name will show up in Claude as the tool group)
mcp = FastMCP("the-graph")


@mcp.tool()
async def get_fpmm_transactions(
    fpmm_address: str, market_maker_address: str = None, limit: int = 1000
):
    """
    Source: custom
    Description: Retrieve all transactions (buys, sells, funding added, funding removed) for a given FPMM address from the 'polygon_polymarket' database. Optionally filter by market maker address.
    Args:
        fpmm_address (str): The FPMM contract address (case-insensitive match).
        market_maker_address (str, optional): The market maker address (trader/funder) to filter by.
        limit (int, optional): Maximum number of results per transaction type (default 1000).
    Returns:
        dict: Transactions grouped by type: { 'FPMMBuy': [...], 'FPMMSell': [...], 'FundingAdded': [...], 'FundingRemoved': [...] }
    """
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["polygon_polymarket"]
    collections = {
        "FPMMBuy": "FPMMBuy",
        "FPMMSell": "FPMMSell",
        "FundingAdded": "FundingAdded",
        "FundingRemoved": "FundingRemoved",
    }
    results = {}
    fpmm_address_lower = fpmm_address.lower()
    for tx_type, coll_name in collections.items():
        coll = db[coll_name]
        query = {"fpmm_address": {"$regex": f"^{fpmm_address_lower}$", "$options": "i"}}
        # Only filter by market maker address if it's provided
        if market_maker_address:
            if tx_type in ["FPMMBuy", "FPMMSell"]:
                query["trader"] = {
                    "$regex": f"^{market_maker_address}$",
                    "$options": "i",
                }
            elif tx_type in ["FundingAdded", "FundingRemoved"]:
                query["funder"] = {
                    "$regex": f"^{market_maker_address}$",
                    "$options": "i",
                }
        # Get all transactions for this fpmm when market_maker_address is not provided
        docs = list(coll.find(query, {"_id": 0}).sort("blockNumber", 1).limit(limit))
        results[tx_type] = docs
    client.close()
    return results


if __name__ == "__main__":
    # Start the MCP server over stdio (Claude for Desktop will pick this up)
    mcp.run(transport="stdio")
