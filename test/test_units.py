import os

import pytest

from models import get_headers
from .utils import process

# HEADERS = get_headers()


@pytest.mark.parametrize(
    "resource",
    [
        "LinkedinAccounts",
        "CampaignContacts",
        "CampaignInstances",
        # "LinkedinContacts",
        # "LinkedinSimpleMessenger",
        "Companies",
        "LinkedinCounts",
        "LinkedinContactsTags",
        "Tags",
    ],
)
@pytest.mark.timeout(540)
def test_auto(resource):
    data = {
        "resource": resource,
        # "headers": HEADERS,
    }
    process(data)
