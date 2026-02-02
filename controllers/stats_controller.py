from models.event_model import EventModel
from models.publication_stats_model import PublicationStatsModel
from models.monthly_stats_model import MonthlyStatsModel


print(">>> LOADED stats_controller.py <<<")

class StatsController:
    def __init__(self, db):
        self.db = db
        self.filtered_ips_col = db["filtered_ips"]
        self.monthly_stats_col = db["monthly_stats"]
        self.publication_stats_col = db["publication_stats"]

    def get_publication_stats(self, pub_id):
        # Get monthly stats for this publication
        monthly = list(self.monthly_stats_col.find({"publication_id": pub_id}, {"_id": 0}))
        # Optionally get total stats from publication_stats
        total = self.publication_stats_col.find_one({"publication_id": pub_id}, {"_id": 0})
        return {"monthly": monthly, "total": total}


    def aggregate_publication_stats(self):
        pipeline = [
            {"$unwind": "$owningItem"},
            {"$group": {
                "_id": "$owningItem",
                "views": {
                    "$sum": {
                        "$cond": [{"$eq": ["$statistics_type", "view"]}, 1, 0]
                    }
                },
                "downloads": {
                    "$sum": {
                        "$cond": [{"$eq": ["$statistics_type", "download"]}, 1, 0]
                    }
                }
            }}
        ]

        stats = list(self.filtered_ips_col.aggregate(pipeline))

        for s in stats:
            self.publication_stats_col.update_one(
                {"publication_id": s["_id"]},
                {"$set": {
                    "publication_id": s["_id"],
                    "views": s["views"],
                    "downloads": s["downloads"]
                }},
                upsert=True
            )
 
    def aggregate_monthly_stats(self):
        pipeline = [
            {"$unwind": "$owningItem"},

            # Convert "time" (ISO string) to Date
            {
                "$addFields": {
                    "eventDate": {
                        "$toDate": "$time"
                    }
                }
            },

            # Extract year and month
            {
                "$addFields": {
                    "year": {"$year": "$eventDate"},
                    "month": {"$month": "$eventDate"}
                }
            },

            # Group by publication + year + month
            {
                "$group": {
                    "_id": {
                        "publication_id": "$owningItem",
                        "year": "$year",
                        "month": "$month"
                    },
                    "views": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$statistics_type", "view"]},
                                1,
                                0
                            ]
                        }
                    },
                    "downloads": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$statistics_type", "download"]},
                                1,
                                0
                            ]
                        }
                    }
                }
            }
        ]

        stats = list(self.filtered_ips_col.aggregate(pipeline))

        for s in stats:
            self.monthly_stats_col.update_one(
                {
                    "publication_id": s["_id"]["publication_id"],
                    "year": s["_id"]["year"],
                    "month": s["_id"]["month"]
                },
                {
                    "$set": {
                        "publication_id": s["_id"]["publication_id"],
                        "year": s["_id"]["year"],
                        "month": s["_id"]["month"],
                        "views": s["views"],
                        "downloads": s["downloads"]
                    }
                },
                upsert=True
            )



    def run_full_pipeline(self):
        self.aggregate_publication_stats()
        self.aggregate_monthly_stats()

