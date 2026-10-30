"""Test suite for agy_federation_mesh CLI."""

import argparse
import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import validate_cli_args, format_cli_diagnostic, build_parser, main


def test_format_cli_diagnostic_empty():
    res = format_cli_diagnostic({})
    assert res == {"arg_count": 0, "command_type": None, "has_json_flag": False}

def test_format_cli_diagnostic_full():
    res = format_cli_diagnostic({"command": "run", "json": True, "extra": "val"})
    assert res == {"arg_count": 3, "command_type": "run", "has_json_flag": True}

def test_format_cli_diagnostic_json_truthy():
    res = format_cli_diagnostic({"command": "run", "json": 1})
    assert res == {"arg_count": 2, "command_type": "run", "has_json_flag": True}


def test_validate_cli_args_valid_run():
    res = validate_cli_args({"command": "run", "workflow": "test"})
    assert res["valid"] is True
    assert res["command"] == "run"
    assert res["errors"] == []

def test_validate_cli_args_invalid_command():
    res = validate_cli_args({"command": "unknown"})
    assert res["valid"] is False
    assert "Unknown command: unknown" in res["errors"][0]

def test_validate_cli_args_missing_command():
    res = validate_cli_args({})
    assert res["valid"] is False
    assert "Unknown command: None" in res["errors"][0]

def test_validate_cli_args_missing_workflow():
    res = validate_cli_args({"command": "run"})
    assert res["valid"] is False
    assert "Missing required parameter: workflow" in res["errors"][0]

def test_validate_cli_args_valid_topology():
    res = validate_cli_args({"command": "topology"})
    assert res["valid"] is True
    
def test_build_parser():
    parser = build_parser()
    assert isinstance(parser, argparse.ArgumentParser)
    
def test_main_sys_exit():
    # Calling main with missing args causes SystemExit
    assert main([]) == 2

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology(mock_orchestrator_cls, capsys):
    mock_instance = mock_orchestrator_cls.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 3, "mesh_digest": "12345678901234567890"}
    
    assert main(["topology"]) == 0
    captured = capsys.readouterr()
    assert "Nodes: 3 | Digest: 1234567890123456" in captured.out

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology_json(mock_orchestrator_cls, capsys):
    mock_instance = mock_orchestrator_cls.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 3, "mesh_digest": "12345678901234567890"}
    
    assert main(["topology", "--json"]) == 0
    captured = capsys.readouterr()
    assert '"node_count": 3' in captured.out

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_valid_workflow(mock_orchestrator_cls, capsys):
    mock_instance = mock_orchestrator_cls.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"ok": True, "workflow_name": "security_reversing_audit", "status": "completed"}
    
    assert main(["run", "--workflow", "security_reversing_audit"]) == 0
    captured = capsys.readouterr()
    assert "Workflow: security_reversing_audit -> completed" in captured.out
    
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_valid_workflow_json(mock_orchestrator_cls, capsys):
    mock_instance = mock_orchestrator_cls.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"ok": True, "workflow_name": "security_reversing_audit", "status": "completed"}
    
    assert main(["run", "--workflow", "security_reversing_audit", "--json"]) == 0
    captured = capsys.readouterr()
    assert '"ok": true' in captured.out

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_valid_workflow_fails(mock_orchestrator_cls, capsys):
    mock_instance = mock_orchestrator_cls.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"ok": False, "workflow_name": "security_reversing_audit", "status": "failed"}
    
    assert main(["run", "--workflow", "security_reversing_audit"]) == 1

def test_main_run_unknown_workflow(capsys):
    assert main(["run", "--workflow", "unknown_workflow"]) == 1
    captured = capsys.readouterr()
    assert "Unknown workflow: unknown_workflow" in captured.err

def test_main_invalid_command():
    assert main(["invalid_command"]) == 2

def test_main_system_exit_custom_code():
    with patch("argparse.ArgumentParser.parse_args", side_effect=SystemExit("some error")):
        assert main(["topology"]) == 1
