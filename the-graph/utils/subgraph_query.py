import os
import requests
from typing import Dict, Optional
from pymongo.collection import Collection


def get_last_processed_id(
    collection: Collection, order_direction: str = "asc", id_field: str = "id"
) -> Optional[str]:
    """
    Get the last processed ID from the database based on sort direction
    Args:
        collection: MongoDB collection to query
        order_direction: Sort direction ('asc' or 'desc')
        id_field: Name of the field to use as ID (defaults to 'id')
    """
    sort_direction = 1 if order_direction == "asc" else -1
    last_record = collection.find_one(sort=[(id_field, sort_direction)])
    return last_record[id_field] if last_record else None


def query_subgraph(
    query: str, variables: Dict, subgraph_url: str, api_key: str
) -> Dict:
    """
    Generic function to query The Graph API
    """
    formatted_url = subgraph_url.format(api_key)

    try:
        response = requests.post(
            formatted_url,
            headers={"Content-Type": "application/json"},
            json={"query": query, "variables": variables},
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code}:", response.text)
            return None
    except Exception as e:
        print(f"Error querying The Graph API: {str(e)}")
        return None
