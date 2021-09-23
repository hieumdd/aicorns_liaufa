from abc import ABCMeta, abstractmethod
from datetime import datetime
import importlib

import requests
from google.cloud import bigquery

from configs import BQ_CLIENT, DATASET, TIMESTAMP_FORMAT

transform_ts = (
    lambda x: datetime.strptime(x, TIMESTAMP_FORMAT).isoformat(timespec="seconds")
    if x
    else None
)

TABLES = {
    "simple": [
        "LinkedinAccounts",
        "CampaignContacts",
        "CampaignInstances",
        "Companies",
        "LinkedinCounts",
        "LinkedinContactsTags",
        "Tags",
    ],
    "reverse": [
        # "LinkedinContacts",
        "LinkedinSimpleMessenger",
    ],
}


class Liaufa(metaclass=ABCMeta):
    @staticmethod
    def factory(table):
        try:
            module = importlib.import_module(f"models.{table}")
            model = getattr(module, table)
            return model()
        except (ImportError, AttributeError):
            raise ValueError(table)

    @property
    @abstractmethod
    def table(self):
        pass

    @property
    @abstractmethod
    def endpoint(self):
        pass

    @property
    @abstractmethod
    def page_size(self):
        pass

    @property
    @abstractmethod
    def ordering_key(self):
        pass

    @property
    @abstractmethod
    def p_key(self):
        pass

    def __init__(self):
        self._getter = self.getter(self)

    @abstractmethod
    def _transform(self, rows):
        pass

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}._stage_{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    def _update(self):
        incre_key = getattr(self, "incre_key", None)
        incre_key = f"ORDER BY {incre_key} DESC" if incre_key else ""
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT (row_num)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {','.join(self.p_key)} {incre_key}) AS row_num
            FROM {DATASET}._stage_{self.table}
        ) WHERE row_num = 1"""
        BQ_CLIENT.query(query).result()

    def run(self):
        with requests.Session() as session:
            rows = self._getter.get(session)
        response = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self._transform(rows)
            response["output_rows"] = self._load(rows)
        return response
