import os
import json
import time
from abc import ABCMeta, abstractmethod

import requests

BASE_URL = "https://api.liaufa.com/api/v1"
CONTENT_TYPE = {"Content-Type": "application/json"}


def get_headers():
    url = f"{BASE_URL}/token/"
    params = {
        "h": "https://app.aicorns.com",
    }
    payload = {
        "username": os.getenv("USERNAME"),
        "password": os.getenv("PWD"),
    }
    with requests.post(
        url,
        params=params,
        json=payload,
        headers={**CONTENT_TYPE},
    ) as r:
        r
        res = r.json()
    access_token = res["access"]
    return {
        **CONTENT_TYPE,
        "Authorization": f"Bearer {access_token}",
    }


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

    def __init__(self, headers=None):
        self.headers = get_headers() if not headers else headers
        # get_headers()

    def _get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        rows = []
        params = {
            "page_size": self.page_size,
            "page": 1,
        }
        with requests.Session() as session:
            while True:
                with session.get(
                    url,
                    params=params,
                    headers=self.headers,
                ) as r:
                    if r.status_code == 404:
                        break
                    else:
                        res = r.json()
                rows.extend(res["results"])
                params["page"] += 1
        return rows

    @abstractmethod
    def _transform(self, _rows):
        pass

    def load(self, rows):
        with open(f"export/{self.table}.json", "w") as f:
            json.dump(rows, f)

    def run(self):
        start = time.time()
        print(start)
        rows = self._get()
        rows = self._transform(rows)
        self.load(rows)
        end = time.time()
        print("total", end - start)


class LinkedinAccount(Liaufa):
    table = "LinkedinAccount"
    endpoint = "linkedin/accounts/"
    page_size = 1000

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class CampaignContacts(Liaufa):
    table = "CampaignContacts"
    endpoint = "campaign-contacts/"
    page_size = 10000

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class CampaignInstances(Liaufa):
    table = "CampaignInstances"
    endpoint = "campaign-instances/"
    page_size = 1000

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class LinkedinContacts(Liaufa):
    table = "LinkedinContacts"
    endpoint = "linkedin/contacts/"
    page_size = 100

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class LinkedinSimpleMessenger(Liaufa):
    table = "LinkedinSimpleMessenger"
    endpoint = "linkedin/simple-messenger/"
    page_size = 100
    # page_size = 10

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class Companies(Liaufa):
    table = "Companies"
    endpoint = "companies/"
    page_size = 100

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class LinkedinCounts(Liaufa):
    table = "LinkedinCounts"
    endpoint = "linkedin/counts/"
    page_size = 1000

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class LinkedinContactsTags(Liaufa):
    table = "LinkedinContactsTags"
    endpoint = "linkedin/contacts/tags/"
    page_size = 10

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


class Tags(Liaufa):
    table = "Tags"
    endpoint = "tags/"
    page_size = 100

    def __init__(self):
        super().__init__()

    def _transform(self, _rows):
        return _rows


# headers = get_headers()
# with requests.Session() as session:
# x = LinkedinSimpleMessenger()
# x.run()
