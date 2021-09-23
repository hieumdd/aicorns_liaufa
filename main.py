from models.models import Liaufa
from tasks import create_task


def main(request):
    data = request.get_json()
    print(data)

    if "tasks" in data:
        response = create_task()
    elif "table" in data:
        response = Liaufa.factory(
            data["table"],
        ).run()
    else:
        raise ValueError(data)

    print(response)
    return response
