import pytest

from .utils import process



@pytest.mark.parametrize(
    "resource",
    [
        # "LinkedinAccounts",
        # "CampaignContacts",
        # "CampaignInstances",
        # "LinkedinContacts",
        "LinkedinSimpleMessenger",
        # "Companies",
        # "LinkedinCounts",
        # "LinkedinContactsTags",
        # "Tags",
    ],
)
# @pytest.mark.timeout(540)
def test_auto(resource):
    data = {
        "resource": resource,
    }
    process(data)
