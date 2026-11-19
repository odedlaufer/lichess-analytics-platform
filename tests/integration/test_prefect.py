"""Prefect server checks. Requires `make up PROFILES="orch"`.

Guards against Prefect's fallback to a temporary local server: the run must be
readable from the server at PREFECT_API_URL.
"""

import os

import pytest
from prefect import flow, get_client
from prefect.client.schemas.objects import StateType

pytestmark = pytest.mark.integration


@flow(name="smoke-flow", log_prints=True)
def smoke_flow(x: int) -> int:
    return x * 2


def test_api_url_points_at_compose_server() -> None:
    url = os.environ.get("PREFECT_API_URL", "")
    assert url == f"http://localhost:{os.environ['PREFECT_PORT']}/api", url


def test_server_healthy() -> None:
    with get_client(sync_client=True) as client:
        assert client.api_healthcheck() is None


def test_flow_run_recorded_on_server() -> None:
    state = smoke_flow(21, return_state=True)
    assert state.is_completed()
    assert state.result() == 42

    flow_run_id = state.state_details.flow_run_id
    assert flow_run_id is not None
    with get_client(sync_client=True) as client:
        run = client.read_flow_run(flow_run_id)
    assert run.state is not None
    assert run.state.type == StateType.COMPLETED
