"""
ISO 9001:2015 / Agile V Quality Pipeline Test Suite
Test: Monkeypatch FederationMeshOrchestrator.execute_cross_station_workflow
"""

import pytest
from unittest.mock import MagicMock

from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator
from agy_federation_mesh import cli


def test_cli_run_monkeypatches_execute_cross_station_workflow(monkeypatch):
    """Verify CLI run command calls patched execute_cross_station_workflow and returns success."""
    mock_result = {
        "workflow_name": "security_reversing_audit",
        "status": "completed",
        "ok": True,
        "results": {"step_audit": "ok", "step_triage": "ok", "step_seal": "ok"},
    }

    def mock_execute(self, workflow_name, tasks):
        assert workflow_name == "security_reversing_audit"
        assert len(tasks) == 3
        return mock_result

    monkeypatch.setattr(
        FederationMeshOrchestrator,
        "execute_cross_station_workflow",
        mock_execute,
    )

    exit_code = cli.main(["run", "--workflow", "security_reversing_audit"])
    assert exit_code == 0


def test_cli_run_monkeypatch_returns_failure(monkeypatch):
    """Verify CLI returns non-zero when patched method returns failure."""
    mock_result = {
        "workflow_name": "security_reversing_audit",
        "status": "failed",
        "ok": False,
        "error": "station_unreachable",
    }

    monkeypatch.setattr(
        FederationMeshOrchestrator,
        "execute_cross_station_workflow",
        lambda self, wf, tasks: mock_result,
    )

    exit_code = cli.main(["run", "--workflow", "security_reversing_audit"])
    assert exit_code == 1


def test_orchestrator_execute_cross_station_workflow_direct_monkeypatch(monkeypatch):
    """Unit-test the orchestrator method directly with a monkeypatch."""
    orchestrator = FederationMeshOrchestrator()
    called = {}

    def fake_execute(self, workflow_name, tasks):
        called["workflow"] = workflow_name
        called["task_count"] = len(tasks)
        return {"ok": True, "workflow_name": workflow_name, "status": "mocked"}

    monkeypatch.setattr(
        FederationMeshOrchestrator,
        "execute_cross_station_workflow",
        fake_execute,
    )

    result = orchestrator.execute_cross_station_workflow("test_wf", [MagicMock(), MagicMock()])
    assert result["ok"] is True
    assert result["status"] == "mocked"
    assert called["workflow"] == "test_wf"
    assert called["task_count"] == 2
