from models.models import Liaufa, transform_ts
from components.getter import SimpleGetter


class Tags(Liaufa):
    getter = SimpleGetter
    table = "tags"
    endpoint = "tags/"
    page_size = 100
    p_key = ["id"]
    ordering_key = "updated"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "company", "type": "INTEGER"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "name": row.get("name"),
                "company": row.get("company"),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]
