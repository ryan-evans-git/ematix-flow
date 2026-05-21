"""``/api/dag`` — cross-pipeline depends_on graph."""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from ematix_flow import pipeline  # noqa: E402
from ematix_flow.web.server import create_app  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_registry():
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    yield
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()


def test_empty_registry_returns_empty_graph() -> None:
    client = TestClient(create_app())
    body = client.get("/api/dag").json()
    assert body == {"nodes": [], "edges": []}


def test_returns_nodes_and_edges() -> None:
    @pipeline.register(name="upstream", schedule="0 * * * *")
    def _u():
        return {}

    @pipeline.register(
        name="downstream", schedule="*/15 * * * *", depends_on=["upstream"],
    )
    def _d():
        return {}

    client = TestClient(create_app())
    body = client.get("/api/dag").json()
    names = {n["name"] for n in body["nodes"]}
    assert names == {"upstream", "downstream"}
    schedules = {n["name"]: n["schedule"] for n in body["nodes"]}
    assert schedules["upstream"] == "0 * * * *"
    assert schedules["downstream"] == "*/15 * * * *"
    assert body["edges"] == [{"from": "upstream", "to": "downstream"}]


def test_diamond_shape() -> None:
    @pipeline.register(name="root", schedule="0 0 * * *")
    def _r():
        return {}

    @pipeline.register(name="left", schedule="0 1 * * *", depends_on=["root"])
    def _l():
        return {}

    @pipeline.register(name="right", schedule="0 1 * * *", depends_on=["root"])
    def _rt():
        return {}

    @pipeline.register(
        name="sink", schedule="0 2 * * *", depends_on=["left", "right"],
    )
    def _s():
        return {}

    client = TestClient(create_app())
    body = client.get("/api/dag").json()
    assert len(body["nodes"]) == 4
    edges = {(e["from"], e["to"]) for e in body["edges"]}
    assert edges == {
        ("root", "left"),
        ("root", "right"),
        ("left", "sink"),
        ("right", "sink"),
    }


def test_includes_timezone_when_set() -> None:
    @pipeline.register(
        name="ny_daily",
        schedule="0 9 * * *",
        timezone="America/New_York",
    )
    def _fn():
        return {}

    client = TestClient(create_app())
    body = client.get("/api/dag").json()
    node = next(n for n in body["nodes"] if n["name"] == "ny_daily")
    assert node["timezone"] == "America/New_York"
