import pytest
import sys
from pathlib import Path
from io import StringIO
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import (
    format_cli_diagnostic,
    validate_cli_args,
    build_parser,
    main,
)

def test_format_cli_diagnostic_empty():
    res = format_cli_diagnostic({})
    assert res == {"arg_count": 0, "command_type": None, "has_json_flag": False}

def test_format_cli_diagnostic_bool_cast():
    res = format_cli_diagnostic({"command": "test", "json": 1})
    assert res == {"arg_count": 2, "command_type": "test", "has_json_flag": True}
    
def test_format_cli_diagnostic_bool_cast_false():
    res = format_cli_diagnostic({"command": "test", "json": 0})
    assert res == {"arg_count": 2, "command_type": "test", "has_json_flag": False}

def test_validate_cli_args_missing_command():
    res = validate_cli_args({})
    assert res["valid"] is False
    assert res["command"] is None
    assert "Unknown command: None" in res["errors"]

def test_validate_cli_args_invalid_command():
    res = validate_cli_args({"command": "invalid"})
    assert res["valid"] is False
    assert res["command"] == "invalid"
    assert "Unknown command: invalid" in res["errors"]

def test_validate_cli_args_topology():
    res = validate_cli_args({"command": "topology"})
    assert res["valid"] is True
    assert res["command"] == "topology"
    assert len(res["errors"]) == 0

def test_validate_cli_args_run_missing_workflow():
    res = validate_cli_args({"command": "run"})
    assert res["valid"] is False
    assert res["command"] == "run"
    assert "Missing required parameter: workflow" in res["errors"]

def test_validate_cli_args_run_valid():
    res = validate_cli_args({"command": "run", "workflow": "test"})
    assert res["valid"] is True
    assert res["command"] == "run"
    assert len(res["errors"]) == 0

def test_build_parser_topology():
    parser = build_parser()
    args = parser.parse_args(["topology"])
    assert args.command == "topology"
    assert args.json is False

def test_build_parser_topology_json():
    parser = build_parser()
    args = parser.parse_args(["topology", "--json"])
    assert args.command == "topology"
    assert args.json is True

def test_build_parser_run_missing_workflow():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run"])

def test_build_parser_run_valid():
    parser = build_parser()
    args = parser.parse_args(["run", "--workflow", "test_workflow"])
    assert args.command == "run"
    assert args.workflow == "test_workflow"
    assert args.json is False

def test_build_parser_run_json():
    parser = build_parser()
    args = parser.parse_args(["run", "--workflow", "test_workflow", "--json"])
    assert args.command == "run"
    assert args.workflow == "test_workflow"
    assert args.json is True

def test_main_help():
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout, patch('sys.stderr', new_callable=StringIO) as mock_stderr:
        assert main(["--help"]) == 0
        
def test_main_invalid_command():
    with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
        assert main(["invalid"]) != 0

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_main_topology(mock_orchestrator):
    mock_instance = mock_orchestrator.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 3, "mesh_digest": "abcdef01234567890"}
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        assert main(["topology"]) == 0
        output = mock_stdout.getvalue()
        assert "Nodes: 3" in output
        assert "Digest: abcdef0123456789" in output

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_main_topology_json(mock_orchestrator):
    mock_instance = mock_orchestrator.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 3, "mesh_digest": "abcdef"}
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        assert main(["topology", "--json"]) == 0
        output = mock_stdout.getvalue()
        assert "node_count" in output
        assert "abcdef" in output

@patch('agy_federation_mesh.cli.WorkflowTask')
@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_main_run_valid(mock_orchestrator, mock_workflow_task):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"workflow_name": "security_reversing_audit", "status": "success", "ok": True}
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        assert main(["run", "--workflow", "security_reversing_audit"]) == 0
        output = mock_stdout.getvalue()
        assert "Workflow: security_reversing_audit -> success" in output
        assert mock_instance.execute_cross_station_workflow.called

@patch('agy_federation_mesh.cli.WorkflowTask')
@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_main_run_valid_json(mock_orchestrator, mock_workflow_task):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"workflow_name": "security_reversing_audit", "status": "success", "ok": True}
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        assert main(["run", "--workflow", "security_reversing_audit", "--json"]) == 0
        output = mock_stdout.getvalue()
        assert "security_reversing_audit" in output
        assert "success" in output

@patch('agy_federation_mesh.cli.WorkflowTask')
@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_main_run_valid_failure(mock_orchestrator, mock_workflow_task):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"workflow_name": "security_reversing_audit", "status": "failed", "ok": False}
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        assert main(["run", "--workflow", "security_reversing_audit"]) == 1

@patch('agy_federation_mesh.cli.FederationMeshOrchestrator')
def test_main_run_unknown_workflow(mock_orchestrator):
    with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
        assert main(["run", "--workflow", "unknown"]) == 1
        output = mock_stderr.getvalue()
        assert "Unknown workflow: unknown" in output

def test_main_no_args():
    with patch('sys.stderr', new_callable=StringIO):
        assert main([]) != 0
