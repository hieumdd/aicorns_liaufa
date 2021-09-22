import pytest

from .utils import process



@pytest.mark.parametrize(
    "resource",
    [
        "LinkedinAccounts",
        "CampaignContacts",
        "CampaignInstances",
        "LinkedinContacts",
        "LinkedinSimpleMessenger",
        "Companies",
        "LinkedinCounts",
        "LinkedinContactsTags",
        "Tags",
    ],
)
def test_auto(resource):
    data = {
        "resource": resource,
    }
    process(data)
