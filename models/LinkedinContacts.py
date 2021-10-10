from models.models import Liaufa, transform_ts
from components.getter import DeltaGetter, AsyncGetter


class LinkedinContacts(Liaufa):
    getter = DeltaGetter
    # getter = AsyncGetter
    table = "linkedin_contacts"
    endpoint = "linkedin/contacts/"
    page_size = 100
    p_key = ["id"]
    ordering_key = "updated"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "company_name", "type": "STRING"},
        {"name": "profile_link", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "name": row.get("name"),
                "company_name": row.get("company_name"),
                "profile_link": row.get("profile_link"),
                "email": row.get("email"),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
