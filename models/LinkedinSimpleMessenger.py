from models.models import Liaufa, transform_ts
from components.getter import ReverseGetter, AsyncGetter


class LinkedinSimpleMessenger(Liaufa):
    getter = ReverseGetter
    # getter = AsyncGetter
    table = "linkedin_simple_messenger"
    endpoint = "linkedin/simple-messenger/"
    page_size = 100
    ordering_key = "updated"
    p_key = ["id"]
    incre_key = "updated"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "contact_status", "type": "INTEGER"},
        {"name": "conversation_status", "type": "INTEGER"},
        {
            "name": "contact",
            "type": "record",
            "fields": [{"name": "id", "type": "INTEGER"}],
        },
        {
            "name": "li_account",
            "type": "record",
            "fields": [{"name": "id", "type": "INTEGER"}],
        },
        {"name": "has_new_messages", "type": "BOOLEAN"},
        {"name": "connected_at", "type": "TIMESTAMP"},
        {"name": "invited_at", "type": "TIMESTAMP"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "contact_status": row.get("contact_status"),
                "conversation_status": row.get("conversation_status"),
                "contact": {
                    "id": row.get("contact").get("id"),
                }
                if row.get("contact")
                else {},
                "li_account": {
                    "id": row.get("li_account").get("id"),
                }
                if row.get("li_account")
                else {},
                "has_new_messages": row.get("has_new_messages"),
                "connected_at": transform_ts(row.get("connected_at")),
                "invited_at": transform_ts(row.get("invited_at")),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
