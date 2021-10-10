import os
import sys
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime
import asyncio
import time

import requests
import aiohttp

from configs import (
    BASE_URL,
    CONTENT_TYPE,
    TIMESTAMP_FORMAT,
    BQ_CLIENT,
    DATASET,
    MIN_TIMESTAMP,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def get_headers(attempt=0):
    try:
        with requests.post(
            url=f"{BASE_URL}/token/",
            params={
                "h": "https://app.aicorns.com",
            },
            json={
                "username": os.getenv("USERNAME"),
                "password": os.getenv("API_PWD"),
            },
            headers={
                **CONTENT_TYPE,
            },
        ) as r:
            if r.status_code == 429:
                if attempt < 5:
                    time.sleep(5)
                    return get_headers(attempt + 1)
                elif attempt >= 5:
                    raise Exception("Too many attempts")
            elif r.status_code == 200:
                res = r.json()
                access_token = res["access"]
                return {
                    **CONTENT_TYPE,
                    "Authorization": f"Bearer {access_token}",
                }
            else:
                r.raise_for_status()
    except requests.exceptions.SSLError:
        return get_headers(attempt + 1)


async def get_headers_async(session):
    async with session.post(
        url=f"{BASE_URL}/token/",
        params={
            "h": "https://app.aicorns.com",
        },
        json={
            "username": os.getenv("USERNAME"),
            "password": os.getenv("API_PWD"),
        },
        headers={
            **CONTENT_TYPE,
        },
    ) as r:
        if r.status == 429:
            await asyncio.sleep(5)
            return await get_headers_async(session)
        elif r.status == 200:
            res = await r.json()
            access_token = res["access"]
            return {
                **CONTENT_TYPE,
                "Authorization": f"Bearer {access_token}",
            }
        else:
            r.raise_for_status()


class Getter(metaclass=ABCMeta):
    def __init__(self, model):
        self.endpoint = model.endpoint
        self.page_size = model.page_size

    @abstractmethod
    def get(self):
        pass


class SimpleGetter(Getter):
    def get(self, session, headers=None, page=1):
        headers = get_headers() if not headers else headers
        print(page)
        with session.get(
            f"{BASE_URL}/{self.endpoint}",
            params={
                "page_size": self.page_size,
                "page": page,
            },
            headers=headers,
        ) as r:
            if r.status_code == 404:
                return []
            elif r.status_code == 401:
                return self.get(session, page=page)
            elif r.status_code == 200:
                res = r.json()
                return res.get("results") + self.get(
                    session,
                    headers=headers,
                    page=page + 1,
                )
            else:
                r.raise_for_status()


class ReverseGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.ordering_key = model.ordering_key
        self.table = model.table

    def get(self, session):
        url = f"{BASE_URL}/{self.endpoint}"
        reverse_stop = self._get_reverse_stop()
        headers = get_headers()
        count = self._get_count(session, url, headers)
        calls_needed = math.ceil(count / self.page_size)
        return self._get(session, url, reverse_stop, calls_needed, headers)

    def _get(self, session, url, reverse_stop, page, headers=None):
        headers = get_headers() if not headers else headers
        with session.get(
            url,
            params={
                "page_size": self.page_size,
                "page": page,
                "ordering": self.ordering_key,
            },
            headers=headers,
        ) as r:
            if r.status_code == 404:
                return []
            elif r.status_code == 401:
                time.sleep(5)
                return self._get(session, page=page)
            elif r.status_code == 200:
                res = r.json()
                _rows = res["results"]
                if (
                    datetime.strptime(
                        _rows[-1][self.ordering_key],
                        TIMESTAMP_FORMAT,
                    )
                    < reverse_stop
                ):
                    next_run = []
                else:
                    next_run = self._get(session, url, reverse_stop, page - 1, headers)
                return res["results"] + next_run
            else:
                r.raise_for_status()

    def _get_count(self, session, url, headers):
        with session.get(
            url,
            params={
                "page_size": 1,
                "page": 1,
            },
            headers=headers,
        ) as r:
            res = r.json()
        return res["count"]

    def _get_reverse_stop(self):
        query = f"""
        SELECT MAX({self.ordering_key}) AS max_incre
        FROM {DATASET}.{self.table}"""
        rows = BQ_CLIENT.query(query).result()
        result = [dict(row.items()) for row in rows][0]["max_incre"]
        return result if result else MIN_TIMESTAMP


class DeltaGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.p_key = model.p_key
        self.table = model.table

    def get(self, session):
        current_rows = self._get_current_rows()
        starting_page = math.floor(current_rows / self.page_size)
        return self._get(session, starting_page)

    def _get(self, session, page, headers=None):
        headers = get_headers() if not headers else headers
        with session.get(
            f"{BASE_URL}/{self.endpoint}",
            params={
                "page_size": self.page_size,
                "page": page,
            },
            headers=headers,
        ) as r:
            if r.status_code == 404:
                return []
            elif r.status_code == 401:
                return self._get(session, page=page)
            elif r.status_code == 200:
                res = r.json()
                return res.get("results") + self._get(
                    session,
                    headers=headers,
                    page=page + 1,
                )
            else:
                r.raise_for_status()

    def _get_current_rows(self):
        query = f"""
        SELECT COUNT(*) AS cnt
        FROM {DATASET}.{self.table}"""
        rows = BQ_CLIENT.query(query).result()
        result = [dict(row.items()) for row in rows][0]["cnt"]
        return result


class AsyncGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.ordering_key = model.ordering_key

    def get(self, session):
        url = f"{BASE_URL}/{self.endpoint}"

        async def get_async():
            connector = aiohttp.TCPConnector(limit=10)
            timeout = aiohttp.ClientTimeout(total=3600)
            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
            ) as session:
                headers = get_headers()
                count = await self._get_count(session, url, headers)
                calls_needed = math.ceil(count / self.page_size)
                tasks = [
                    asyncio.create_task(self._get_one(session, url, headers, i))
                    # for i in range(1, calls_needed + 1)
                    for i in range(900, 960)
                    # for i in range(1, calls_needed + 1)
                    # for i in range(1, calls_needed + 1)
                    # for i in range(1, calls_needed + 1)
                ]
                rows = await asyncio.gather(*tasks)
                return [i for j in rows for i in j]

        return asyncio.run(get_async())

    async def _get_count(self, session, url, headers):
        async with session.get(
            url,
            params={
                "page_size": 1,
                "page": 1,
            },
            headers=headers,
        ) as r:
            res = await r.json()
        return res["count"]

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
                    print(i, 401)
                    _headers = await get_headers_async(session)
                    continue
                elif r.status == 404:
                    print(i, 404)
                    return []
                elif r.status == 502:
                    print(i, 502)
                    continue
                else:
                    res = await r.json()
                    return res["results"]
