import os
import json
import time
from abc import ABCMeta, abstractmethod

import requests

BASE_URL = "https://api.liaufa.com/api/v1"
CONTENT_TYPE = {"Content-Type": "application/json"}
MAX_ITERATION = 50


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
        res = r.json()
    access_token = res.get("access")
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

    @property
    @abstractmethod
    def ordering_key(self):
        pass

    def __init__(self):
        pass

    def _get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        rows = []
        params = {
            "page_size": self.page_size,
            "page": 1,
        }
        headers = get_headers()
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
                        headers = get_headers()
                        continue
                    else:
                        res = r.json()
                rows.extend(res.get("results"))
                # print(len(rows))
                params["page"] += 1
        return rows

    @abstractmethod
    def _transform(self, rows):
        pass

    def load(self, rows):
        with open(f"export/{self.table}.json", "w") as f:
            json.dump(rows, f)

    def run(self):
        # start = time.time()
        # print(start)
        rows = self._get()
        rows = self._transform(rows)
        self.load(rows)
        # end = time.time()
        # print("total", end - start)


class LinkedinAccount(Liaufa):
    table = "LinkedinAccount"
    endpoint = "linkedin/accounts/"
    page_size = 1000
    ordering_key = None

    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
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
    ordering_key = None

    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "contact": row.get("contact"),
                "campaign_instance": row.get("campaign_instance"),
            }
            for row in rows
        ]


class CampaignInstances(Liaufa):
    table = "CampaignInstances"
    endpoint = "campaign-instances/"
    page_size = 1000
    ordering_key = None


    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "campaign": {
                    "id": row.get("campaign").get("id"),
                }
                if row.get("campaign")
                else {},
                "active": row.get("active"),
                "start_at": row.get("start_at"),
                "li_account": row.get("li_account"),
                "flag_connector": row.get("flag_connector"),
                "company": row.get("company"),
                "limit_requests_daily": row.get("limit_requests_daily"),
                "limit_follow_up_messages_daily": row.get("limit_follow_up_messages_daily"),
                "campaign_type": row.get("campaign_type"),
            }
            for row in rows
        ]


class LinkedinContacts(Liaufa):
    table = "LinkedinContacts"
    endpoint = "linkedin/contacts/"
    page_size = 100
    ordering_key = "created"


    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "company_name": row.get("company_name"),
                "profile_link": row.get("profile_link"),
                "email": row.get("email"),
            }
            for row in rows
        ]


class LinkedinSimpleMessenger(Liaufa):
    table = "LinkedinSimpleMessenger"
    endpoint = "linkedin/simple-messenger/"
    page_size = 100
    ordering_key = "created_at"

    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "contact_status": row.get("contact_status"),
                "conversation_status": row.get("conversion_status"),
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
                "connected_at": row.get("connected_at"),
                "invited_at": row.get("invited_at"),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ]


class Companies(Liaufa):
    table = "Companies"
    endpoint = "companies/"
    page_size = 100
    ordering_key = None


    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "agency": row.get("agency"),
                "li_accounts_count": row.get("li_accounts_count"),
            }
            for row in rows
        ]


class LinkedinCounts(Liaufa):
    table = "LinkedinCounts"
    endpoint = "linkedin/counts/"
    page_size = 1000
    ordering_key = None

    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "date": row.get("date"),
                "type": row.get("type"),
                "count": row.get("count"),
                "li_account": row.get("li_account"),
                "campaign_instance": row.get("campaign_instance"),
            }
            for row in rows
        ]


class LinkedinContactsTags(Liaufa):
    table = "LinkedinContactsTags"
    endpoint = "linkedin/contacts/tags/"
    page_size = 10
    ordering_key = None

    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "tag": row.get("tag"),
                "contact": row.get("contact"),
            }
            for row in rows
        ]


class Tags(Liaufa):
    table = "Tags"
    endpoint = "tags/"
    page_size = 100
    ordering_key = None

    def __init__(self):
        super().__init__()

    def _transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "company": row.get("company"),
            }
            for row in rows
        ]
