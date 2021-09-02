import os
import sys
import json
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime
import asyncio

import requests
import aiohttp
from google.cloud import bigquery

BASE_URL = "https://api.liaufa.com/api/v1"
CONTENT_TYPE = {"Content-Type": "application/json"}
MAX_ITERATION = 50

NOW = datetime.utcnow()
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

BQ_CLIENT = bigquery.Client()
DATASET = "Liaufa"

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def transform_ts(ts):
    if ts:
        return datetime.strptime(ts, TIMESTAMP_FORMAT).isoformat(timespec="seconds")


async def get_headers():
    url = f"{BASE_URL}/token/"
    params = {
        "h": "https://app.aicorns.com",
    }
    payload = {
        "username": os.getenv("USERNAME"),
        "password": os.getenv("API_PWD"),
    }
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.post(
                url,
                params=params,
                json=payload,
                headers={**CONTENT_TYPE},
            ) as r:
                if r.status == 429:
                    asyncio.sleep(5)
                    continue
                res = await r.json()
            access_token = res["access"]
            break
    return {
        **CONTENT_TYPE,
        "Authorization": f"Bearer {access_token}",
    }


class Getter(metaclass=ABCMeta):
    def __init__(self, model):
        self.endpoint = model.endpoint
        self.page_size = model.page_size

    @abstractmethod
    def get(self):
        pass


class SimpleGetter(Getter):
    def __init__(self, endpoint, page_size):
        super().__init__(endpoint, page_size)

    def get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        rows = []
        params = {
            "page_size": self.page_size,
            "page": 1,
        }
        headers = asyncio.run(get_headers())
        with requests.Session() as session:
            while params["page"] < MAX_ITERATION:
                with session.get(
                    url,
                    params=params,
                    headers=headers,
                ) as r:
                    if r.status_code == 404:
                        break
                    elif r.status_code == 401:
                        headers = asyncio.run(get_headers())
                        continue
                    else:
                        res = r.json()
                rows.extend(res.get("results"))
                params["page"] += 1
        return rows


class ReverseGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.ordering_key = model.ordering_key
        self.table = model.table

    def get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        reverse_stop = self._get_reverse_stop()
        x = reverse_stop.strftime(TIMESTAMP_FORMAT)
        headers = asyncio.run(get_headers())
        rows = []
        with requests.Session() as session:
            count = self._get_count(session, url, headers)
            calls_needed = math.ceil(count / self.page_size)
            params = {
                "page_size": self.page_size,
                "page": calls_needed,
                "ordering": self.ordering_key,
            }
            params
            while True:
                with session.get(url, params=params, headers=headers) as r:
                    if r.status_code == 404:
                        print(404)
                        break
                    elif r.status_code == 401:
                        headers = asyncio.run(get_headers())
                        continue
                    else:
                        res = r.json()
                _rows = res.get("results")
                rows.extend(_rows)
                if (
                    datetime.strptime(_rows[-1][self.ordering_key], TIMESTAMP_FORMAT)
                    < reverse_stop
                ):
                    _rows
                    break
                else:
                    params["page"] -= 1
        return rows

    def _get_count(self, session, url, headers):
        params = {
            "page_size": 1,
            "page": 1,
        }
        with session.get(url, params=params, headers=headers) as r:
            res = r.json()
        count = res["count"]
        return count

    def _get_reverse_stop(self):
        query = f"""
        SELECT MAX({self.ordering_key}) AS max_incre
        FROM {DATASET}.{self.table}"""
        rows = BQ_CLIENT.query(query).result()
        result = [dict(row.items()) for row in rows][0]["max_incre"]
        return result


class AsyncGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.ordering_key = model.ordering_key

    def get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        return asyncio.run(self._get(url))

    async def _get(self, url):
        connector = aiohttp.TCPConnector(limit=20)
        timeout = aiohttp.ClientTimeout(total=540)
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            headers = await get_headers()
            count = await self._get_count(session, url, headers)
            calls_needed = math.ceil(count / self.page_size)
            tasks = [
                asyncio.create_task(self._get_one(session, url, headers, i))
                for i in range(1, calls_needed + 1)
            ]
            rows = await asyncio.gather(*tasks)
        rows = [item for sublist in rows for item in sublist]
        return rows

    async def _get_count(self, session, url, headers):
        params = {
            "page_size": 1,
            "page": 1,
        }
        async with session.get(url, params=params, headers=headers) as r:
            res = await r.json()
        count = res["count"]
        return count

    async def _get_one(self, session, url, headers, i):
        params = {
            "page_size": self.page_size,
            "page": i,
        }
        if self.ordering_key:
            params["ordering"] = self.ordering_key
        _headers = headers
        while True:
            async with session.get(
                url,
                params=params,
                headers=_headers,
            ) as r:
                if r.status == 401:
                    _headers = await get_headers()
                    continue
                elif r.status == 404:
                    results = []
                    break
                else:
                    res = await r.json()
                    results = res["results"]
                    print(i)
                    break
        results = [
            {
                **result,
                # "_page": i,
            }
            for result in results
        ]
        return results


class Liaufa(metaclass=ABCMeta):
    @staticmethod
    def factory(resource):
        if resource == "LinkedinAccounts":
            return LinkedinAccount()
        elif resource == "CampaignContacts":
            return CampaignContacts()
        elif resource == "CampaignInstances":
            return CampaignInstances()
        elif resource == "LinkedinContacts":
            return LinkedinContacts()
        elif resource == "LinkedinSimpleMessenger":
            return LinkedinSimpleMessenger()
        elif resource == "Companies":
            return Companies()
        elif resource == "LinkedinCounts":
            return LinkedinCounts()
        elif resource == "LinkedinContactsTags":
            return LinkedinContactsTags()
        elif resource == "Tags":
            return Tags()
        else:
            raise NotImplementedError(resource)

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
        pass

    @abstractmethod
    def _transform(self, rows):
        pass

    def load(self, rows):
        with open(f"configs/{self.table}.json", "r") as f:
            schema = json.load(f)["schema"]
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema,
            ),
        ).result()

    def update(self):
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
        rows = self.getter.get()
        response = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self._transform(rows)
            loads = self.load(rows)
            self.update()
            response["output_rows"] = loads.output_rows
        return response


class LinkedinAccount(Liaufa):
    table = "LinkedinAccount"
    endpoint = "linkedin/accounts/"
    page_size = 1000
    ordering_key = "id"
    p_key = ["id"]

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

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


class CampaignContacts(Liaufa):
    table = "CampaignContacts"
    endpoint = "campaign-contacts/"
    page_size = 10000
    p_key = ["id"]
    ordering_key = "updated"

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

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


class CampaignInstances(Liaufa):
    table = "CampaignInstances"
    endpoint = "campaign-instances/"
    page_size = 1000
    p_key = ["id"]
    ordering_key = ["updated"]

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

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


class LinkedinContacts(Liaufa):
    table = "LinkedinContacts"
    endpoint = "linkedin/contacts/"
    page_size = 100
    p_key = ["id"]
    ordering_key = "updated"

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

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


class LinkedinSimpleMessenger(Liaufa):
    table = "LinkedinSimpleMessenger"
    endpoint = "linkedin/simple-messenger/"
    page_size = 100
    ordering_key = "updated"
    p_key = ["id"]
    # incre_key = "updated"

    def __init__(self):
        self.getter = AsyncGetter(self)
        # self.getter = ReverseGetter(self)
        super().__init__()

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


class Companies(Liaufa):
    table = "Companies"
    endpoint = "companies/"
    page_size = 100
    p_key = ["id"]
    ordering_key = "updated"

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

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


class LinkedinCounts(Liaufa):
    table = "LinkedinCounts"
    endpoint = "linkedin/counts/"
    page_size = 1000
    p_key = ["id"]
    ordering_key = "updated"

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "date": row.get("date"),
                "type": row.get("type"),
                "count": row.get("count"),
                "li_account": row.get("li_account"),
                "campaign_instance": row.get("campaign_instance"),
                "created": transform_ts(row.get("created")),
                "updated": transform_ts(row.get("updated")),
            }
            for row in rows
        ]


class LinkedinContactsTags(Liaufa):
    table = "LinkedinContactsTags"
    endpoint = "linkedin/contacts/tags/"
    page_size = 10
    p_key = ["id"]
    ordering_key = None

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "tag": row.get("tag"),
                "contact": row.get("contact"),
            }
            for row in rows
        ]


class Tags(Liaufa):
    table = "Tags"
    endpoint = "tags/"
    page_size = 100
    p_key = ["id"]
    ordering_key = "updated"

    def __init__(self):
        self.getter = AsyncGetter(self)
        super().__init__()

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
