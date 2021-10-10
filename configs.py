from datetime import datetime, timezone

from google.cloud import bigquery

BASE_URL = "https://api.liaufa.com/api/v1"
CONTENT_TYPE = {
    "Content-Type": "application/json",
}

NOW = datetime.utcnow()
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

BQ_CLIENT = bigquery.Client()
DATASET = "Liaufa"
MIN_TIMESTAMP = datetime(2018, 1, 1, tzinfo=timezone.utc)
