import pytest
from unittest.mock import Mock

from main import main
from models.models import TABLES


def process(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res


class TestPipelines:
    def assert_pipelines(self, res):
        assert res["num_processed"] >= 0
        if res["num_processed"] > 0:
            assert res["num_processed"] == res["output_rows"]

    @pytest.mark.parametrize(
        "table",
        TABLES["simple"],
    )
    def test_simple(self, table):
        data = {
            "table": table,
        }
        res = process(data)
        self.assert_pipelines(res)

    @pytest.mark.parametrize(
        "table",
        TABLES["reverse"],
    )
    def test_reverse(self, table):
        data = {
            "table": table,
        }
        res = process(data)
        self.assert_pipelines(res)


def test_tasks():
    data = {
        "tasks": "liaufa",
    }
    res = process(data)
    assert res["tasks"] > 0
