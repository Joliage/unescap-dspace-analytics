# run_bot_mapping.py
import os
import json
import time
import requests
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv

# -------------------------
# Load environment variables
# -------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
BOT_MAP_COL = os.getenv("BOT_MAPPING_COLLECTION")
JSON_FILE = os.getenv("RAW_JSON_FILE")
API_KEY = os.getenv("ABSTRACT_API_KEY")
API_URL = os.getenv("ABSTRACT_API_URL")
LOOKUP_LIMIT = int(os.getenv("LOOKUP_LIMIT", 5000))

# -------------------------
# Connect to MongoDB
# -------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
bot_mapping = db[BOT_MAP_COL]

# -------------------------
# Load JSON data
# -------------------------
if not os.path.exists(JSON_FILE):
    raise FileNotFoundError(f"{JSON_FILE} not found.")

with open(JSON_FILE, "r", encoding="utf-8") as f:
    raw_json = json.load(f)

docs = raw_json.get("response", {}).get("docs", [])
print(f"Loaded {len(docs)} records from the JSON file.")

# -------------------------
# Get unique IPs to lookup
# -------------------------
unique_ips = {doc.get("ip") for doc in docs if doc.get("ip")}
existing_ips = set(bot_mapping.distinct("ip"))
ips_to_lookup = [ip for ip in unique_ips if ip not in existing_ips]

if len(ips_to_lookup) > LOOKUP_LIMIT:
    ips_to_lookup = ips_to_lookup[:LOOKUP_LIMIT]

print(f"{len(ips_to_lookup)} new IPs to lookup via API.")

# -------------------------
# Function to check if IP is bot
# -------------------------
def is_ip_bot(api_response):
    sec = api_response.get("security", {})
    flags = [
        sec.get("is_vpn", False),
        sec.get("is_proxy", False),
        sec.get("is_tor", False),
        sec.get("is_hosting", False),
        sec.get("is_relay", False),
        sec.get("is_mobile", False),
        sec.get("is_abuse", False),
    ]
    return any(flags)

# -------------------------
# Lookup IPs via API
# -------------------------
last_request_time = 0
for ip in tqdm(ips_to_lookup, desc="IP Lookup Progress", unit="ip"):
    elapsed = time.time() - last_request_time
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    try:
        r = requests.get(
            API_URL,
            params={"api_key": API_KEY, "ip_address": ip},
            timeout=10
        )
        data = r.json()
        bot_flag = is_ip_bot(data)

        # Save to MongoDB
        bot_mapping.update_one(
            {"ip": ip},
            {"$set": {"is_bot": bot_flag, "response": data}},
            upsert=True
        )
    except Exception as ex:
        bot_mapping.update_one(
            {"ip": ip},
            {"$set": {"is_bot": True, "response": {"error": str(ex)}}},
            upsert=True
        )

    last_request_time = time.time()

# -------------------------
# Filter clean docs
# -------------------------
map_dict = {m["ip"]: m["is_bot"] for m in bot_mapping.find({}, {"ip": 1, "is_bot": 1})}
clean_docs = [doc for doc in docs if not map_dict.get(doc.get("ip"), True)]
print(f"Filtered {len(clean_docs)} clean records (bots removed).")
print("Bot mapping process complete.")
