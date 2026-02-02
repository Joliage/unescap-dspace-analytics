# models/event_model.py
from datetime import datetime

class EventModel:
    def __init__(self, db):
        self.filtered_ips = db["filtered_ips"]

    def get_clean_events(self):
        """
        Returns normalized events:
        publication_id, date, event_type (VIEW/DOWNLOAD)
        """
        events = []

        cursor = self.filtered_ips.find({})
        for doc in cursor:
            pub_ids = doc.get("owningItem", [])
            if not pub_ids:
                continue

            event_type = self._categorize_event(doc)
            date = self._extract_date(doc.get("time"))

            for pub_id in pub_ids:
                events.append({
                    "publication_id": pub_id,
                    "date": date,
                    "event_type": event_type
                })

        return events

    def _categorize_event(self, doc):
        # DSpace SOLR rule (simplified but correct)
        bundle = doc.get("bundleName", [])
        if "ORIGINAL" in bundle:
            return "DOWNLOAD"
        return "VIEW"

    def _extract_date(self, time_str):
        if not time_str:
            return None
        return datetime.fromisoformat(time_str.replace("Z", "")).date()
