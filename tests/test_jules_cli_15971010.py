import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

import pytest
from agy_federation_mesh import cli

def test_unknown_command(capsys):
    """Test behavior when an unknown command is passed."""
    # argparse raises SystemExit(2) for invalid choices, and prints to stderr
    result = cli.main(["unknown"])
    assert result != 0
    captured = capsys.readouterr()
    assert "invalid choice: 'unknown'" in captured.err

def test_no_args(capsys):
    """Test behavior when no args are passed."""
    result = cli.main([])
    assert result != 0
    captured = capsys.readouterr()
    assert "the following arguments are required: command" in captured.err

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_topology_normal(mock_orchestrator_class, capsys):
    """Test topology command without json flag."""
    mock_orchestrator = mock_orchestrator_class.return_value
    mock_orchestrator.discover_nodes.return_value = {
        "node_count": 42,
        "mesh_digest": "abcdef0123456789xxxx"
    }
    
    result = cli.main(["topology"])
    assert result == 0
    captured = capsys.readouterr()
    assert "[AGY-FEDERATION] Nodes: 42 | Digest: abcdef0123456789" in captured.out

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_topology_json(mock_orchestrator_class, capsys):
    """Test topology command with json flag."""
    mock_orchestrator = mock_orchestrator_class.return_value
    mock_orchestrator.discover_nodes.return_value = {
        "node_count": 42,
        "mesh_digest": "abcdef0123456789xxxx"
    }
    
    result = cli.main(["topology", "--json"])
    assert result == 0
    captured = capsys.readouterr()
    output_json = json.loads(captured.out)
    assert output_json["node_count"] == 42
    assert output_json["mesh_digest"] == "abcdef0123456789xxxx"

def test_run_missing_workflow(capsys):
    """Test run command without workflow argument."""
    result = cli.main(["run"])
    assert result != 0
    captured = capsys.readouterr()
    assert "the following arguments are required: --workflow" in captured.err

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_run_unknown_workflow(mock_orchestrator_class, capsys):
    """Test run command with unknown workflow."""
    result = cli.main(["run", "--workflow", "invalid_workflow"])
    assert result == 1
    captured = capsys.readouterr()
    assert "Unknown workflow: invalid_workflow" in captured.err

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_run_valid_workflow_normal(mock_orchestrator_class, capsys):
    """Test run command with valid workflow without json flag."""
    mock_orchestrator = mock_orchestrator_class.return_value
    mock_orchestrator.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "COMPLETED",
        "ok": True
    }
    
    result = cli.main(["run", "--workflow", "security_reversing_audit"])
    assert result == 0
    captured = capsys.readouterr()
    assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> COMPLETED" in captured.out

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_run_valid_workflow_json(mock_orchestrator_class, capsys):
    """Test run command with valid workflow with json flag."""
    mock_orchestrator = mock_orchestrator_class.return_value
    mock_orchestrator.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "COMPLETED",
        "ok": True
    }
    
    result = cli.main(["run", "--workflow", "security_reversing_audit", "--json"])
    assert result == 0
    captured = capsys.readouterr()
    output_json = json.loads(captured.out)
    assert output_json["workflow_name"] == "security_reversing_audit"
    assert output_json["status"] == "COMPLETED"
    assert output_json["ok"] is True

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_run_valid_workflow_failure(mock_orchestrator_class, capsys):
    """Test run command with valid workflow that fails."""
    mock_orchestrator = mock_orchestrator_class.return_value
    mock_orchestrator.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "FAILED",
        "ok": False
    }
    
    result = cli.main(["run", "--workflow", "security_reversing_audit"])
    assert result == 1
    captured = capsys.readouterr()
    assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> FAILED" in captured.out

def test_system_exit_non_int(monkeypatch):
    """Test SystemExit handling when code is not int."""
    # argparse raises SystemExit(2) for invalid choices. Let's force it to raise SystemExit("error")
    def mock_parse_args(*args, **kwargs):
        raise SystemExit("string error")
    
    mock_parser = MagicMock()
    mock_parser.parse_args = mock_parse_args
    monkeypatch.setattr(cli, "build_parser", lambda: mock_parser)
    
    result = cli.main(["topology"])
    assert result == 1

def test_main_block_coverage():
    """Test coverage for __main__ block if possible."""
    pass # we can't test __main__ easily without running the file directly, which is fine
