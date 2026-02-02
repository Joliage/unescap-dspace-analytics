# models/monthly_stats_model.py
from collections import defaultdict

class MonthlyStatsModel:
    def __init__(self, db):
        self.daily = db["publication_stats"]
        self.monthly = db["monthly_stats"]

    def build_monthly_stats(self):
        stats = defaultdict(lambda: {"views": 0, "downloads": 0})

        for doc in self.daily.find({}):
            month = doc["date"][:7]  # YYYY-MM
            key = (doc["publication_id"], month)

            stats[key]["views"] += doc.get("views", 0)
            stats[key]["downloads"] += doc.get("downloads", 0)

        for (pub_id, month), counts in stats.items():
            self.monthly.update_one(
                {
                    "publication_id": pub_id,
                    "month": month
                },
                {
                    "$set": {
                        "publication_id": pub_id,
                        "month": month,
                        "views": counts["views"],
                        "downloads": counts["downloads"]
                    }
                },
                upsert=True
            )
