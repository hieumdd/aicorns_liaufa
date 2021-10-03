from models.models import Liaufa, transform_ts
from components.getter import SimpleGetter


class CampaignInstances(Liaufa):
    getter = SimpleGetter
    table = "campaign_instances"
    endpoint = "campaign-instances/"
    page_size = 1000
    p_key = ["id"]
    ordering_key = ["updated"]

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {
            "name": "campaign",
            "type": "RECORD",
            "fields": [
                {"name": "id", "type": "INTEGER"},
            ],
        },
        {"name": "active", "type": "BOOLEAN"},
        {"name": "start_at", "type": "TIMESTAMP"},
        {"name": "li_account", "type": "INTEGER"},
        {"name": "flag_connector", "type": "BOOLEAN"},
        {"name": "company", "type": "INTEGER"},
        {"name": "limit_requests_daily", "type": "INTEGER"},
        {"name": "limit_follow_up_messages_daily", "type": "INTEGER"},
        {"name": "campaign_type", "type": "INTEGER"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "name": row.get("name"),
                "campaign": {
                    "id": row.get("campaign").get("id"),
                }
                if row.get("campaign")
                else {},
                "active": row.get("active"),
                "start_at": transform_ts(row.get("start_at")),
                "li_account": row.get("li_account"),
                "flag_connector": row.get("flag_connector"),
                "company": row.get("company"),
                "limit_requests_daily": row.get("limit_requests_daily"),
                "limit_follow_up_messages_daily": row.get(
                    "limit_follow_up_messages_daily"
                ),
                "campaign_type": row.get("campaign_type"),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
