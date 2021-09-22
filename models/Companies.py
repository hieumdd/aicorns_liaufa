import json

from models.models import Liaufa
from models import transform_ts
from components.getter import SimpleGetter


class Companies(Liaufa):
    getter = SimpleGetter
    table = "companies"
    endpoint = "companies/"
    page_size = 100
    p_key = ["id"]
    ordering_key = "updated"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "agency", "type": "INTEGER"},
        {"name": "li_accounts_count", "type": "STRING"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "name": row.get("name"),
                "agency": row.get("agency"),
                "li_accounts_count": json.dumps(row.get("li_accounts_count")),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
