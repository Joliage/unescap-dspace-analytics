print(">>> LOADED publication_controller.py <<<")

class PublicationController:
    def __init__(self, db, api_base_url):
        self.db = db
        self.api_base_url = api_base_url
        self.publication_col = db["publication"]
        self.programs_col = db["programs"]
        self.publisher_col = db["publisher"]
        self.stats_col = db["publication_stats"]

    def fetch_publication_data(self, pub_id):
        # Get metadata from DB
        publication = self.publication_col.find_one({"id": pub_id}, {"_id": 0})  # REMOVE _id
        if not publication:
            return None

        programs = list(self.programs_col.find({"publication_id": pub_id}, {"_id": 0, "publication_id": 0}))
        publisher = self.publisher_col.find_one({"publication_id": pub_id}, {"_id": 0, "publication_id": 0})
        stats = self.stats_col.find_one({"publication_id": pub_id}, {"_id": 0})

        return {
            **publication,
            "programs": programs,
            "publisher": publisher,
            "stats": stats or {"views": 0, "downloads": 0}
        }


    def get_publication_with_stats(self, publication_id, stats_controller):
        metadata = self.get_publication_metadata(publication_id)
        if not metadata:
            return None

        stats = stats_controller.get_publication_stats(publication_id)

        return {
            "metadata": metadata,
            "stats": stats
        }
