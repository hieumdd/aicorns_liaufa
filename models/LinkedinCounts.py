from models.models import Liaufa
from models import transform_ts
from components.getter import SimpleGetter


class LinkedinCounts(Liaufa):
    getter = SimpleGetter
    table = "linkedin_counts"
    endpoint = "linkedin/counts/"
    page_size = 1000
    p_key = ["id"]
    ordering_key = "updated"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "date", "type": "DATE"},
        {"name": "type", "type": "STRING"},
        {"name": "count", "type": "INTEGER"},
        {"name": "li_account", "type": "INTEGER"},
        {"name": "campaign_instance", "type": "INTEGER"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "date": row.get("date"),
                "type": row.get("type"),
                "count": row.get("count"),
                "li_account": row.get("li_account"),
                "campaign_instance": row.get("campaign_instance"),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
