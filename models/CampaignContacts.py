from models.models import Liaufa, transform_ts
from components.getter import SimpleGetter


class CampaignContacts(Liaufa):
    getter = SimpleGetter
    table = "campaign_contacts"
    endpoint = "campaign-contacts/"
    page_size = 10000
    p_key = ["id"]
    ordering_key = "updated"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "contact", "type": "INTEGER"},
        {"name": "campaign_instance", "type": "INTEGER"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "contact": row.get("contact"),
                "campaign_instance": row.get("campaign_instance"),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
