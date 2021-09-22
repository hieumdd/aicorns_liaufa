from models.models import Liaufa
from components.getter import SimpleGetter


class LinkedinAccounts(Liaufa):
    getter = SimpleGetter
    table = "LinkedinAccount"
    endpoint = "linkedin/accounts/"
    page_size = 1000
    p_key = ["id"]
    ordering_key = "id"

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "company", "type": "INTEGER"},
        {"name": "limit_connection_requests_daily", "type": "INTEGER"},
        {"name": "limit_messages_daily", "type": "INTEGER"},
        {"name": "sales_navigator", "type": "BOOLEAN"},
        {"name": "fuse_limit", "type": "STRING"},
        {"name": "lower_bound_connection_requests_daily", "type": "INTEGER"},
        {"name": "upper_bound_connection_requests_daily", "type": "INTEGER"},
        {"name": "lower_bound_messages_daily", "type": "INTEGER"},
        {"name": "upper_bound_messages_daily", "type": "INTEGER"},
        {"name": "range_limits", "type": "BOOLEAN"},
    ]

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "name": row.get("name"),
                "company": row.get("company"),
                "limit_connection_requests_daily": row[
                    "limit_connection_requests_daily"
                ],
                "limit_messages_daily": row.get("limit_messages_daily"),
                "sales_navigator": row.get("sales_navigator"),
                "fuse_limit": row.get("fuse_limit"),
                "lower_bound_connection_requests_daily": row[
                    "lower_bound_connection_requests_daily"
                ],
                "upper_bound_connection_requests_daily": row[
                    "upper_bound_connection_requests_daily"
                ],
                "lower_bound_messages_daily": row.get("lower_bound_messages_daily"),
                "upper_bound_messages_daily": row.get("upper_bound_messages_daily"),
                "range_limits": row.get("range_limits"),
            }
            for row in rows
        ]
