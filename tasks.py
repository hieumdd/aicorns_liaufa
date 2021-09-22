import os
import json
import uuid

from google.cloud import tasks_v2

from models.models import TABLES

TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = {
    "project": os.getenv("PROJECT_ID"),
    "location": os.getenv("REGION"),
    "queue": "liaufa",
}
PARENT = TASKS_CLIENT.queue_path(**CLOUD_TASKS_PATH)


def create_task():
    payloads = [
        {
            "table": table,
        }
        for table in [table for i in TABLES.values() for table in i]
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(
                **CLOUD_TASKS_PATH,
                task=f"{payload['table']}-{uuid.uuid4()}",
            ),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"https://{os.getenv('REGION')}-{os.getenv('PROJECT_ID')}.cloudfunctions.net/{os.getenv('FUNCTION_NAME')}",
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "tasks": len(responses),
    }
