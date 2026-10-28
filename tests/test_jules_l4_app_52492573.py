"""Unit test: monkeypatch _orchestrator.discover_nodes and verify topology endpoint."""

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.l4_app import app, _orchestrator


def test_topology_endpoint_uses_patched_discover_nodes(monkeypatch):
    """Verify /api/federation/topology reflects a monkeypatched discover_nodes payload."""
    fake_discovery = {
        "node_count": 2,
        "online_count": 1,
        "mesh_digest": "deadbeefcafef00d",
        "nodes": [
            {
                "station_id": "mission_control",
                "name": "Mission Control",
                "online": True,
                "capabilities": ["orchestrate", "seal_evidence"],
            },
            {
                "station_id": "pe_reversing",
                "name": "PE Reversing Lab",
                "online": False,
                "capabilities": ["triage_binary", "seal_evidence"],
            },
        ],
    }

    calls = {"count": 0}

    def fake_discover_nodes():
        calls["count"] += 1
        return fake_discovery

    monkeypatch.setattr(_orchestrator, "discover_nodes", fake_discover_nodes)

    client = TestClient(app)
    response = client.get("/api/federation/topology")

    assert response.status_code == 200
    body = response.json()

    assert calls["count"] == 1
    assert body["ok"] is True
    assert body["node_count"] == 2
    assert body["online_count"] == 1
    assert body["mesh_digest"] == "deadbeefcafef00d"
    assert body["nodes"] == fake_discovery["nodes"]
    # capabilities are deduplicated while preserving first-seen order
    assert body["capabilities"] == ["orchestrate", "seal_evidence", "triage_binary"]
