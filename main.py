import base64
import json

from models import Liaufa

from broadcast import broadcast


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        results = broadcast()
    elif "resource" in data:
        job = Liaufa.factory(
            data["resource"],
        )
        results = job.run()
    else:
        raise NotImplementedError(data)

    response = {
        "pipelines": "Liaufa",
        "results": results,
    }
    print(response)
    return response
