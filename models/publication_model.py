from pymongo import MongoClient

class PublicationModel:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.publication_col = self.db["publication"]
        self.programs_col = self.db["programs"]
        self.publisher_col = self.db["publisher"]

    def save_publication(self, processed):
        """
        Save a single processed publication into separate collections
        """
        pub_id = processed["id"]

        # Core publication info
        pub_data = {
            "id": pub_id,
            "title": processed.get("title"),
            "handle": processed.get("handle"),
            "dateIssued": processed.get("dateIssued"),
            "type": processed.get("type")
        }
        self.publication_col.update_one({"id": pub_id}, {"$set": pub_data}, upsert=True)

        # Programs / Areas of work
        programs_data = {
            "publication_id": pub_id,
            "programOfWork": processed.get("programOfWork"),
            "areaOfWork": processed.get("areaOfWork")
        }
        self.programs_col.update_one({"publication_id": pub_id}, {"$set": programs_data}, upsert=True)

        # Publisher info
        publisher_data = {
            "publication_id": pub_id,
            "publisher": processed.get("publisher"),
        }
        self.publisher_col.update_one({"publication_id": pub_id}, {"$set": publisher_data}, upsert=True)

    def get_all_publication_ids(self):
        return [doc["id"] for doc in self.db["bot_mapping"].find({"is_bot": False}, {"id": 1})]
