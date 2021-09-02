import os
import json

from google.cloud import pubsub_v1


PUBLISHER = pubsub_v1.PublisherClient()
TOPIC_PATH = PUBLISHER.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

RESOURCES = [
    "LinkedinAccounts",
    "CampaignContacts",
    "CampaignInstances",
    "LinkedinContacts",
    "LinkedinSimpleMessenger",
    "Companies",
    "LinkedinCounts",
    "LinkedinContactsTags",
    "Tags",
]


def broadcast():
    for resource in RESOURCES:
        data = {
            "resource": resource,
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        # PUBLISHER.publish(TOPIC_PATH, data=message_bytes).result()

    return {
        "message_sent": len(RESOURCES),
    }
