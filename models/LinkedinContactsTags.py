from models.models import Liaufa
from components.getter import SimpleGetter


class LinkedinContactsTags(Liaufa):
    getter = SimpleGetter
    table = "LinkedinContactsTags"
    endpoint = "linkedin/contacts/tags/"
    page_size = 10
    p_key = ["id"]
    ordering_key = None

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "tag", "type": "INTEGER"},
        {"name": "contact", "type": "INTEGER"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "tag": row.get("tag"),
                "contact": row.get("contact"),
            }
            for row in rows
        ]
