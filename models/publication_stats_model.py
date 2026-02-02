# models/publication_stats_model.py
from collections import defaultdict

class PublicationStatsModel:
    def __init__(self, db):
        self.collection = db["publication_stats"]

    def build_daily_stats(self, events):
        """
        Aggregates per publication per day
        """
        stats = defaultdict(lambda: {"views": 0, "downloads": 0})

        for e in events:
            key = (e["publication_id"], e["date"])
            if e["event_type"] == "VIEW":
                stats[key]["views"] += 1
            else:
                stats[key]["downloads"] += 1

        for (pub_id, date), counts in stats.items():
            self.collection.update_one(
                {
                    "publication_id": pub_id,
                    "date": str(date)
                },
                {
                    "$set": {
                        "publication_id": pub_id,
                        "date": str(date),
                        "views": counts["views"],
                        "downloads": counts["downloads"]
                    }
                },
                upsert=True
            )
