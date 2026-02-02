import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm

# -------------------------
# Load environment
# -------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
BOT_MAP_COL = os.getenv("BOT_MAPPING_COLLECTION")
JSON_FILE = os.getenv("RAW_JSON_FILE")

# -------------------------
# Mongo connection
# -------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

bot_mapping = db[BOT_MAP_COL]
filtered_ips_col = db["filtered_ips"]

# -------------------------
# Load raw JSON
# -------------------------
with open(JSON_FILE, "r", encoding="utf-8") as f:
    raw_json = json.load(f)

docs = raw_json.get("response", {}).get("docs", [])

print(f"Loaded {len(docs)} records from JSON")

# -------------------------
# Build IP bot lookup map
# -------------------------
ip_bot_map = {
    m["ip"]: m["is_bot"]
    for m in bot_mapping.find({}, {"ip": 1, "is_bot": 1})
}

print(f"Loaded {len(ip_bot_map)} bot-mapping records")

# -------------------------
# Filter + store clean docs
# -------------------------
inserted = 0

for doc in tqdm(docs, desc="Filtering clean records"):
    ip = doc.get("ip")

    # skip if IP missing or flagged bot
    if not ip or ip_bot_map.get(ip, True):
        continue

    filtered_ips_col.insert_one(doc)
    inserted += 1

print(f"\nInserted {inserted} clean records into filtered_ips collection")
