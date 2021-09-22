from models.models import Liaufa
from tasks import create_task


def main(request):
    data = request.get_json()
    print(data)

    if "tasks" in data:
        response = create_task()
    elif "resource" in data:
        response = Liaufa.factory(
            data["resource"],
        ).run()
    else:
        raise ValueError(data)

    print(response)
    return response
