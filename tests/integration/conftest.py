import os

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client


@pytest.fixture(scope="session")
def ch() -> Client:
    return clickhouse_connect.get_client(
        host="localhost",
        port=int(os.environ["CLICKHOUSE_HTTP_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )
