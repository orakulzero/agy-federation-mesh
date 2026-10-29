import pytest
from agy_federation_mesh.cli import main
from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator

def test_run_workflow_security_reversing_audit_failure(monkeypatch):
    """
    Test that calling main(["run", "--workflow", "security_reversing_audit", "--json"])
    when FederationMeshOrchestrator.execute_cross_station_workflow returns a failure result
    ({"ok": False, ...}) correctly exits with exit code 1.
    """
    def mock_execute(*args, **kwargs):
        return {"ok": False, "workflow_name": "security_reversing_audit", "status": "error"}

    monkeypatch.setattr(FederationMeshOrchestrator, "execute_cross_station_workflow", mock_execute)

    exit_code = main(["run", "--workflow", "security_reversing_audit", "--json"])
    
    assert exit_code == 1

def test_run_workflow_security_reversing_audit_success(monkeypatch):
    """Test successful workflow run to verify non-failure branch coverage."""
    def mock_execute(*args, **kwargs):
        return {"ok": True, "workflow_name": "security_reversing_audit", "status": "success"}

    monkeypatch.setattr(FederationMeshOrchestrator, "execute_cross_station_workflow", mock_execute)

    exit_code = main(["run", "--workflow", "security_reversing_audit", "--json"])
    
    assert exit_code == 0

def test_unknown_workflow():
    exit_code = main(["run", "--workflow", "unknown_workflow", "--json"])
    assert exit_code == 1

def test_topology_json(monkeypatch):
    def mock_discover_nodes(*args, **kwargs):
        return {"node_count": 5, "mesh_digest": "abcdef1234567890"}

    monkeypatch.setattr(FederationMeshOrchestrator, "discover_nodes", mock_discover_nodes)
    
    exit_code = main(["topology", "--json"])
    assert exit_code == 0

def test_topology_no_json(monkeypatch):
    def mock_discover_nodes(*args, **kwargs):
        return {"node_count": 5, "mesh_digest": "abcdef1234567890"}

    monkeypatch.setattr(FederationMeshOrchestrator, "discover_nodes", mock_discover_nodes)
    
    exit_code = main(["topology"])
    assert exit_code == 0

def test_run_workflow_no_json(monkeypatch):
    def mock_execute(*args, **kwargs):
        return {"ok": True, "workflow_name": "security_reversing_audit", "status": "success"}

    monkeypatch.setattr(FederationMeshOrchestrator, "execute_cross_station_workflow", mock_execute)

    exit_code = main(["run", "--workflow", "security_reversing_audit"])
    
    assert exit_code == 0

def test_invalid_args():
    exit_code = main(["invalid_command"])
    assert exit_code == 2 # argparse raises SystemExit(2)
