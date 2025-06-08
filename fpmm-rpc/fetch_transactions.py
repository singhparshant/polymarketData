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


w31.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w32.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w33.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w34.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
w35.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
# Load ABI from ctfabi.json
ctf_abi = None
script_dir = os.path.dirname(os.path.abspath(__file__))
abi_file_path = os.path.join(script_dir, "ctfabi.json")

try:
    with open(abi_file_path, "r") as f:
        ctf_abi = json.load(f)
except Exception as e:
    print(f"Error loading ctfabi.json: {e}")

providers = [w31, w32, w33, w34, w35]
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


def process_single_hash(tx_hash, db, w3, my_contract):
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

            time.sleep(0.3)  # Reduced sleep slightly

        # Return success after processing all events for this hash
        return (tx_hash, None)
    except Exception as e:
        # Catch exceptions during get_trade_info_from_hash or other steps
        return (tx_hash, f"Exception processing hash: {str(e)}")


def process_transaction_hashes_parallel(
    hashes=["0xe0c6f83f55e87cea5dcb613fc9913b0903052094400a160c629b2d728c36c7ae"],
):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["polygon_polymarket"]
    #     "0x7335365a5bb702319576300d86e8882b086262b9f21a7c3a13acf305f3d6885c",
    #     "0x744473dd45b85b4e52db92b819b823ab25b5bfa0a7ac0e250593c040f019b9c7",
    #     "0x75a8b50e9306875cc2b6fce2e78da05ab57b61db4ef8773b9602242a172be31e",
    #     "0x76b4cad7f35f6b219b5368fa2a2743c2cd795c0b6a53e6e0b0be29b80c4589e1",
    #     "0x76dd73c8869c1222a3aefc3bd939a2c83c88af065edf3f2016eb33b00cb3f41d",
    #     "0x78817f560a7c8df64d83a74b812c175271b0fe256dd82d59f9588fe507bbd3c0",
    #     "0x7977266a4ce6cea4e1b0e059929b55ea74967305b363614c299fcc44c7fd9b13",
    #     "0x7a8d2d8f8f14b5ac741fb7efe945b82d66bc1d7ad50b7ae2c470d8a81357191e",
    #     "0x7a9bd43a707a361260951856c2e4ab7f0333d981475600916b48a25ad31d61b6",
    #     "0x7bd63fd0c00b92c5d003ebd0f1ba8594bc0de3576b11d7497ff48bc9808663f0",
    #     "0x8479098c053124d04558fb74a71a2e6606cd643023d86ce95c8907a8ec6ef31d",
    #     "0x869ac37cfdf8061bc50257201ce0e3c054823dd97e15e3aa5ff9425b2adf0a77",
    #     "0x876ac59848cdfedbe25901e3c0318ed98f668be6c37b3cb0e51ac7a378513e59",
    #     "0x8a6085e581862b0e98b5de9174def7049b0dfe7831b3694b36f122fd9e083b81",
    #     "0x8b8d84dd9d1106778bc47bb91aadbeed153b05496a56c18fb53d46857c972c67",
    #     "0x91444faa05e4b9f30e7914b11480a1fa5fbb97344b0ee2be25f42fd28d8fc670",
    #     "0x95d1f3ff66b8fa39a3698f2ac9df889a4a90cd9188d27aa55fda2e966a6d2fb9",
    #     "0x96b4f1ce5c394ae727296bae1eb95dd60601768e6c6a067339be92188bdab46a",
    #     "0x985b8fc9f9f0eb4ff23dc9d9f5abc05261fc52cffcdce7a9f79ffaf3792570c3",
    #     "0x9b5dbbc96352915e988dbc8ebca651a6edc28cd8bc3e64b7a9220ebdd1940060",
    #     "0x9b84172bb74fadb19319ec78fc3cea9af0df8a35e1a9315af64f2cdb72c00d06",
    #     "0x9f0cd6a096347c2d07caff8999149fffb664b1e752261e742045d837cf82b412",
    #     "0x9f1e2bf1d3c19e7ff870d304ddfeb65408c3d210a5b65a32473cde958903f0a1",
    #     "0xa02fc7fc65f467ea6329af966860e9d48ad0fb27ffa0f8ff6383854aad7cedf8",
    #     "0xa782b2be70ac6ad4218f273b8602697943ee3a505e2810552cc97939db786350",
    #     "0xa9bb34a451e3f1816dc43b1ae77d156f95933f7bbfc4e5aca2ed5ef0dff6a624",
    #     "0xae584240689f8ca5378e58bc9385209969857c2857509d8b4ea804213eb9c5ae",
    #     "0xb051acb5cb32ecf5fee7126eaab556a899afe351058b8f1eb95ef250a85a6409",
    #     "0xb3b82e126cee75633851d0927a0a87f0258d97142c08e2548f66d0cbcce30e7f",
    #     "0xb3f0c325f6659cdd46d6f47a29d185a0d4ee749f546648d423bbaeafbf7439d5",
    #     "0xb408c0404f59f5afdac4b353950159ffb285e91b1bef0ec3a719b3cf1eea2713",
    #     "0xb50be01679fd4e595cbad7cca96ec08779859c5255030a038c142cef66162a98",
    #     "0xb5b73a260d5c5f64dc15e289ebf1b0e002058491fe316fa94a23efddedd3d3e2",
    #     "0xb5fdd87bf4e0971f7e3b2467bfba1fdd47375923918747c1e3ebf5a7c440a5c5",
    #     "0xb697f65d038572e08f2c7f62a756917b1e07f0a65aa4fb217e52b10c9f9eec23",
    #     "0xb6d8b26280d42c6d8f1c36536f6d7a5b0e16062ce577aa900224415b217e93e4",
    #     "0xba711f89a1afea69affff6203fc90ade4a95ffae8f8b53b0a6a0c64d0b369193",
    #     "0xbaa232f392cfdbf85302a14ad79ce674b19b8dc97cf2f4bd84bde3393d2c34dd",
    #     "0xbabf453c7567e0a9fea41efc87ccc4b3550107b5ce13326e759c65212992c5ec",
    #     "0xbabba4d1028e56102897913a757f9dc05ba3c693a57d2f7bfe80c27df4dca8c5",
    #     "0xbbdd15e787538ea8cad5d0ec42d21ae236dd9908f5e92471d6835881fcae95ea",
    #     "0xbd2e6da1df408f8e0ff0227f1b7f7fe2c811f8483ed1d79a71778aaa9d794326",
    #     "0xbd5f01bee6d78c6997d20a90ad47b67457b81ff4ed277f6e77215ac3c1aada09",
    #     "0xbda3c99ef64196883abb80ae79434f03f263c5d2a28f0c367efe80b9a9a49229",
    #     "0xbfb229f566cced8345642cb79ed46929f9d3ec2c411015f8b3a74fef72f0ebc4",
    #     "0xc3af0bcd76695cab579034d48bcca03b8f4ac028a537a7effae2380a96ee3be9",
    #     "0xc49dbf0c04a00fa15613f7e6241ef51ec7684a6f937c690509ee7facd4b2ef2d",
    #     "0xc695c8078e7d434eea5f8fd42b8532b6ac00e62797197af829902ed1c35a123b",
    #     "0xc789a914190a3d7896bf8eda941f7bb147df3fd640e5dc3bfcd3010e176a30c4",
    #     "0xcb8f6b92f72f33d84a58bfa1a3efeca7d29eaca8d1e891b102ecd920802c4618",
    #     "0xcc8d2e4f85746b0c8995dc2bdca3c06f5ab2a5151c07f292c47b4ae788d67e1c",
    #     "0xcd8158d8b41763996538e5d9210d83f6271e9afc34058835e02f4016f77c1681",
    #     "0xcf8b193f975a54e7bd05b017095da9cf19f8bb5e6aa6b99a0190e0592561c727",
    #     "0xd072aeb18e9e5251958de7fd67ce20eec336fb66fee6a9fed11b5ac0c2b7124a",
    #     "0xd1a87bac66cd9b7401d32aaae312ba6e405b67366f632de464fecde060435913",
    #     "0xd507a900523b25647a2c9f64192a9b743233b93a85ecb5aa31547ed7468c6028",
    #     "0xd56fba0e477895baa6d68789ca3145e1596401d1ccf7b471a44abf4810a4e91e",
    #     "0xd964343f2dbae7821d77813f778ce803ed3b60932e84e224ea89b37dbb4d81c1",
    #     "0xda13b2e950f2c1912e586d2bc60881308c25be8d0a0e758d6878556959597666",
    #     "0xdbe4575c95f5dc2ef804aab24a9f2926d3782fee1b03b4685d6b12250b905425",
    #     "0xdc48074deb28f404d685e1e1970dacc8cac2cb8314001ce405b9777dcada2619",
    #     "0xddc19701797734d858cca2c65e501886051875cdcb2c3261a88780a392d5c6a1",
    #     "0xde7e48e9ed85c259366234c2bc25ae4d8d7d4addbf6d9aaa72a34922e815af4d",
    #     "0xe26cb9f7513e6970c38135ddd5a27238900138571e6bf3bdd46f8ede25c822c1",
    #     "0xe44ee8b9c282a19da97f006f7914ed496ca8942c91dcc8eca5b086cabf81c3ec",
    #     "0xe8ea93fde69267b433ad602355dc5826e27f83ee7766f3a5853fe3824152a747",
    #     "0xee8baf7533174400ca4960914802f434ea3dfc1cff5181c6458166d952cba0bd",
    #     "0xeeb4cbfd5175e726157d856272b2a2db7f04161a5c0d97ddb35bbec447be17c9",
    #     "0xf82357cd3d7ba9e840dd9219507849b2581038cba484e99674e2cbd41b5f9f88",
    #     "0xff6fec3f6a2c74868c0ab4af453a23cd7867fc949021d46d144c5a655ad00ed8",
    # ]

    # hashes = ["0x2e7c9a6df97c73354e13e9bf04e749bf9b0498e2a5bd2038d11a191844ce7363"]
    total_hashes = len(hashes)
    error_hashes = []
    lock = threading.Lock()
    processed_count = 0
    last_hash = None

    def worker(args):
        tx_hash, idx = args
        w3 = providers[idx % len(providers)]
        my_contract = contracts[idx % len(contracts)]
        result = process_single_hash(tx_hash, db, w3, my_contract)
        time.sleep(0.2)
        with lock:
            nonlocal processed_count, last_hash
            processed_count += 1
            last_hash = tx_hash
            if result[1] is not None:
                error_hashes.append((result[0], result[1]))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(worker, (tx_hash, i)) for i, tx_hash in enumerate(hashes)
        ]
        # for _ in tqdm(as_completed(futures), total=total_hashes, desc="Processing"):
        #     pass  # tqdm updates the bar as each future completes

    # print(f"Processed {total_hashes} transaction hashes.")
    if error_hashes:
        print("Errors encountered for the following hashes:")
        for tx_hash, error in error_hashes:
            print(f"{tx_hash}: {error}")


if __name__ == "__main__":
    process_transaction_hashes_parallel()
