import base64
import json

from models import Liaufa

# from broadcast import broadcast_email, broadcast_job


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "resource" in data:
        job = Liaufa.factory(
            data["resource"],
            # data["headers"],
        )
        results = job.run()
    else:
        raise NotImplementedError(data)

    response = {
        "pipelines": "GA",
        "results": results,
    }
    print(response)
    return response
