"""Test suite for agy_federation_mesh CLI covering edge cases, boundary inputs, and invariant checks."""

import json
import pytest
import sys
import argparse
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import (
    format_cli_diagnostic,
    validate_cli_args,
    build_parser,
    main,
)

# --- Tests for format_cli_diagnostic ---

def test_format_cli_diagnostic_empty():
    assert format_cli_diagnostic({}) == {
        "arg_count": 0,
        "command_type": None,
        "has_json_flag": False
    }

def test_format_cli_diagnostic_basic():
    args = {"command": "topology"}
    assert format_cli_diagnostic(args) == {
        "arg_count": 1,
        "command_type": "topology",
        "has_json_flag": False
    }

def test_format_cli_diagnostic_with_json_true():
    args = {"command": "run", "json": True, "workflow": "test"}
    assert format_cli_diagnostic(args) == {
        "arg_count": 3,
        "command_type": "run",
        "has_json_flag": True
    }

def test_format_cli_diagnostic_with_json_false():
    args = {"command": "run", "json": False, "workflow": "test"}
    assert format_cli_diagnostic(args) == {
        "arg_count": 3,
        "command_type": "run",
        "has_json_flag": False
    }

def test_format_cli_diagnostic_other_keys():
    args = {"other": 1, "keys": 2}
    assert format_cli_diagnostic(args) == {
        "arg_count": 2,
        "command_type": None,
        "has_json_flag": False
    }

# --- Tests for validate_cli_args ---

def test_validate_cli_args_valid_topology():
    res = validate_cli_args({"command": "topology"})
    assert res["valid"] is True
    assert res["command"] == "topology"
    assert res["errors"] == []

def test_validate_cli_args_valid_run():
    res = validate_cli_args({"command": "run", "workflow": "test"})
    assert res["valid"] is True
    assert res["command"] == "run"
    assert res["errors"] == []

def test_validate_cli_args_run_missing_workflow():
    res = validate_cli_args({"command": "run"})
    assert res["valid"] is False
    assert res["command"] == "run"
    assert "Missing required parameter: workflow" in res["errors"]

def test_validate_cli_args_unknown_command():
    res = validate_cli_args({"command": "invalid_cmd"})
    assert res["valid"] is False
    assert res["command"] == "invalid_cmd"
    assert "Unknown command: invalid_cmd" in res["errors"]

def test_validate_cli_args_none_command():
    res = validate_cli_args({})
    assert res["valid"] is False
    assert res["command"] is None
    assert "Unknown command: None" in res["errors"]

# --- Tests for build_parser ---

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

def test_build_parser_run():
    parser = build_parser()
    args = parser.parse_args(["run", "--workflow", "w1"])
    assert args.command == "run"
    assert args.workflow == "w1"
    assert args.json is False

def test_build_parser_run_missing_workflow():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run"])

def test_build_parser_missing_command():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])

# --- Tests for main ---

@patch("sys.stdout", new_callable=StringIO)
@patch("sys.stderr", new_callable=StringIO)
def test_main_help(mock_stderr, mock_stdout):
    # argparse prints help and exits with 0
    assert main(["--help"]) == 0

@patch("sys.stdout", new_callable=StringIO)
@patch("sys.stderr", new_callable=StringIO)
def test_main_invalid_args(mock_stderr, mock_stdout):
    # argparse invalid argument exits with 2
    assert main([]) == 2
    assert main(["invalid_cmd"]) == 2

@patch("sys.stdout", new_callable=StringIO)
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology(mock_orchestrator, mock_stdout):
    mock_instance = mock_orchestrator.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 5, "mesh_digest": "a"*32}
    
    assert main(["topology"]) == 0
    mock_instance.discover_nodes.assert_called_once()
    
    output = mock_stdout.getvalue()
    assert "[AGY-FEDERATION] Nodes: 5 | Digest: aaaaaaaaaaaaaaaa" in output

@patch("sys.stdout", new_callable=StringIO)
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology_json(mock_orchestrator, mock_stdout):
    mock_instance = mock_orchestrator.return_value
    ret_val = {"node_count": 5, "mesh_digest": "a"*32}
    mock_instance.discover_nodes.return_value = ret_val
    
    assert main(["topology", "--json"]) == 0
    mock_instance.discover_nodes.assert_called_once()
    
    output = mock_stdout.getvalue()
    parsed_json = json.loads(output)
    assert parsed_json == ret_val

@patch("sys.stdout", new_callable=StringIO)
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_valid_workflow(mock_orchestrator, mock_stdout):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "COMPLETED",
        "ok": True
    }
    
    assert main(["run", "--workflow", "security_reversing_audit"]) == 0
    mock_instance.execute_cross_station_workflow.assert_called_once()
    
    output = mock_stdout.getvalue()
    assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> COMPLETED" in output

@patch("sys.stdout", new_callable=StringIO)
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_valid_workflow_json(mock_orchestrator, mock_stdout):
    mock_instance = mock_orchestrator.return_value
    ret_val = {
        "workflow_name": "security_reversing_audit",
        "status": "COMPLETED",
        "ok": True
    }
    mock_instance.execute_cross_station_workflow.return_value = ret_val
    
    assert main(["run", "--workflow", "security_reversing_audit", "--json"]) == 0
    mock_instance.execute_cross_station_workflow.assert_called_once()
    
    output = mock_stdout.getvalue()
    parsed_json = json.loads(output)
    assert parsed_json == ret_val

@patch("sys.stdout", new_callable=StringIO)
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_valid_workflow_not_ok(mock_orchestrator, mock_stdout):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "FAILED",
        "ok": False
    }
    
    assert main(["run", "--workflow", "security_reversing_audit"]) == 1
    output = mock_stdout.getvalue()
    assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> FAILED" in output

@patch("sys.stderr", new_callable=StringIO)
@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_unknown_workflow(mock_orchestrator, mock_stderr):
    assert main(["run", "--workflow", "unknown_workflow"]) == 1
    mock_orchestrator.return_value.execute_cross_station_workflow.assert_not_called()
    
    output = mock_stderr.getvalue()
    assert "Unknown workflow: unknown_workflow" in output

def test_main_sys_argv_none():
    # If main() is called with no args, it should use sys.argv
    # We patch sys.argv to simulate a call without args
    with patch("sys.argv", ["agy-federation-mesh", "topology"]):
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with patch("agy_federation_mesh.cli.FederationMeshOrchestrator") as mock_orchestrator:
                mock_instance = mock_orchestrator.return_value
                mock_instance.discover_nodes.return_value = {"node_count": 1, "mesh_digest": "b"*32}
                
                assert main(None) == 0
                mock_instance.discover_nodes.assert_called_once()
                assert "Digest: bbbbbbbbbbbbbbbb" in mock_stdout.getvalue()

def test_main_unhandled_command():
    # This shouldn't happen normally because argparse validates commands,
    # but we can mock parse_args to return an unhandled command.
    with patch("agy_federation_mesh.cli.build_parser") as mock_build_parser:
        mock_parser = MagicMock()
        mock_args = MagicMock()
        mock_args.command = "unhandled_command"
        mock_parser.parse_args.return_value = mock_args
        mock_build_parser.return_value = mock_parser
        
        assert main(["unhandled"]) == 1

def test_main_systemexit_string():
    # If argparse raises SystemExit with a string message instead of int
    with patch("agy_federation_mesh.cli.build_parser") as mock_build_parser:
        mock_parser = MagicMock()
        mock_parser.parse_args.side_effect = SystemExit("Error string")
        mock_build_parser.return_value = mock_parser
        
        assert main(["some_args"]) == 1
