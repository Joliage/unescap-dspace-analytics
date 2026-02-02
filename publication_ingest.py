# publication_ingest.py
import os
import json
import time
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv
import cloudscraper

# -------------------------
# Load environment variables
# -------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# -------------------------
# Connect to MongoDB
# -------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

filtered_ips_col = db["filtered_ips"]
publication_col = db["publication"]
programs_col = db["programs"]
publisher_col = db["publisher"]

# -------------------------
# Setup CloudScraper
# -------------------------
scraper = cloudscraper.create_scraper(
    browser={
        "browser": "chrome",
        "platform": "windows",
        "mobile": False
    }
)

# -------------------------
# Helper functions
# -------------------------
def fetch_publication_json(owning_item_id):
    """Fetch publication metadata JSON using CloudScraper."""
    url = f"https://repository.unescap.org/server/api/core/items/{owning_item_id}"

    headers = {
        "Accept": "application/json",
        "Referer": "https://repository.unescap.org/",
    }

    try:
        response = scraper.get(url, headers=headers, timeout=20)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed {owning_item_id}: HTTP {response.status_code}")
            return None

    except Exception as e:
        print(f" Failed {owning_item_id}: {e}")
        return None


def extract_and_store_metadata(item_data):
    """Extract publication, program, and publisher info and store in MongoDB."""
    if not item_data:
        return False

    pub_id = item_data.get("id")
    metadata = item_data.get("metadata", {})

    # -------------------------
    # Publication
    # -------------------------
    publication_doc = {
        "id": pub_id,
        "title": metadata.get("dc.title", [{}])[0].get("value")
        if metadata.get("dc.title") else None,
        "handle": item_data.get("handle"),
        "dateIssued": metadata.get("dc.date.issued", [{}])[0].get("value")
        if metadata.get("dc.date.issued") else None,
        "type": metadata.get("dc.type", [{}])[0].get("value")
        if metadata.get("dc.type") else None,
    }

    publication_col.update_one(
        {"id": pub_id},
        {"$set": publication_doc},
        upsert=True
    )

    # -------------------------
    # Programs
    # -------------------------
    programme_list = metadata.get("escap.programmeOfWork", [])
    areas_of_work = [
    area.get("value")
    for area in metadata.get("escap.areasOfWork", [])
        ]

    for prog in programme_list:
        programs_col.update_one(
            {
                "publication_id": pub_id,
                "programOfWork": prog.get("value")
            },
            {
                "$set": {
                    "publication_id": pub_id,
                    "programOfWork": prog.get("value"),
                    "areasOfWork": areas_of_work
                }
            },
            upsert=True
        )

    # -------------------------
    # Publisher
    # -------------------------
    publisher_doc = {
        "publication_id": pub_id,
        "publisher": metadata.get("dc.publisher", [{}])[0].get("value")
        if metadata.get("dc.publisher") else None,
        "publisherPlace": metadata.get("escap.publisherPlace", [{}])[0].get("value")
        if metadata.get("escap.publisherPlace") else None,
        "rights": metadata.get("dc.rights", [{}])[0].get("value")
        if metadata.get("dc.rights") else None,
    }

    publisher_col.update_one(
        {"publication_id": pub_id},
        {"$set": publisher_doc},
        upsert=True
    )

    return True


# -------------------------
# Main ingestion loop
# -------------------------
filtered_docs = list(filtered_ips_col.find({}))
total = len(filtered_docs)

print(f"Found {total} non-bot records to process for publications.")

success = 0
skipped = 0
failed = 0

for doc in tqdm(filtered_docs, desc="Processing Publications", unit="pub"):
    owning_items = doc.get("owningItem", [])

    if not owning_items:
        skipped += 1
        continue

    for item_id in owning_items:
        data = fetch_publication_json(item_id)

        if data:
            result = extract_and_store_metadata(data)
            if result:
                success += 1
            else:
                failed += 1
        else:
            skipped += 1

        time.sleep(1)  # polite delay
