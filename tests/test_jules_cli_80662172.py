"""Tests for agy_federation_mesh CLI boundary and error paths."""

import json
import sys
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import (
    format_cli_diagnostic,
    validate_cli_args,
    build_parser,
    main,
)

# --- format_cli_diagnostic ---

def test_format_cli_diagnostic_empty():
    assert format_cli_diagnostic({}) == {
        "arg_count": 0,
        "command_type": None,
        "has_json_flag": False
    }

def test_format_cli_diagnostic_missing_json():
    assert format_cli_diagnostic({"command": "test"}) == {
        "arg_count": 1,
        "command_type": "test",
        "has_json_flag": False
    }

def test_format_cli_diagnostic_json_truthy():
    assert format_cli_diagnostic({"command": "test", "json": 1}) == {
        "arg_count": 2,
        "command_type": "test",
        "has_json_flag": True
    }

def test_format_cli_diagnostic_json_falsy():
    assert format_cli_diagnostic({"command": "test", "json": ""}) == {
        "arg_count": 2,
        "command_type": "test",
        "has_json_flag": False
    }


# --- validate_cli_args ---

def test_validate_cli_args_missing_command():
    res = validate_cli_args({})
    assert res["valid"] is False
    assert res["command"] is None
    assert "Unknown command: None" in res["errors"]

def test_validate_cli_args_unknown_command():
    res = validate_cli_args({"command": "fake"})
    assert res["valid"] is False
    assert res["command"] == "fake"
    assert "Unknown command: fake" in res["errors"]

def test_validate_cli_args_topology():
    res = validate_cli_args({"command": "topology"})
    assert res["valid"] is True
    assert res["command"] == "topology"
    assert res["errors"] == []

def test_validate_cli_args_run_missing_workflow():
    res = validate_cli_args({"command": "run"})
    assert res["valid"] is False
    assert res["command"] == "run"
    assert "Missing required parameter: workflow" in res["errors"]

def test_validate_cli_args_run_with_workflow():
    res = validate_cli_args({"command": "run", "workflow": "test"})
    assert res["valid"] is True
    assert res["command"] == "run"
    assert res["errors"] == []


# --- build_parser ---

def test_build_parser_help(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["-h"])
    assert exc.value.code == 0
    out, err = capsys.readouterr()
    assert "AGY Federation Mesh" in out

def test_build_parser_missing_command(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args([])
    assert exc.value.code == 2

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
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["run"])
    assert exc.value.code == 2

def test_build_parser_run():
    parser = build_parser()
    args = parser.parse_args(["run", "--workflow", "w1"])
    assert args.command == "run"
    assert args.workflow == "w1"
    assert args.json is False


# --- main ---

def test_main_no_args():
    # Should exit with code 2 because of missing required sub-command
    assert main([]) == 2

def test_main_invalid_args():
    assert main(["invalid_command"]) == 2

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology(mock_orchestrator, capsys):
    mock_instance = MagicMock()
    mock_instance.discover_nodes.return_value = {"node_count": 5, "mesh_digest": "abcdef0123456789xxxx"}
    mock_orchestrator.return_value = mock_instance

    assert main(["topology"]) == 0
    out, err = capsys.readouterr()
    assert "[AGY-FEDERATION] Nodes: 5 | Digest: abcdef0123456789" in out

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology_json(mock_orchestrator, capsys):
    mock_instance = MagicMock()
    mock_instance.discover_nodes.return_value = {"node_count": 5, "mesh_digest": "abcdef0123456789xxxx"}
    mock_orchestrator.return_value = mock_instance

    assert main(["topology", "--json"]) == 0
    out, err = capsys.readouterr()
    data = json.loads(out)
    assert data["node_count"] == 5
    assert data["mesh_digest"] == "abcdef0123456789xxxx"

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_unknown_workflow(mock_orchestrator, capsys):
    assert main(["run", "--workflow", "unknown_wf"]) == 1
    out, err = capsys.readouterr()
    assert "Unknown workflow: unknown_wf" in err

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_known_workflow(mock_orchestrator, capsys):
    mock_instance = MagicMock()
    mock_instance.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "completed",
        "ok": True
    }
    mock_orchestrator.return_value = mock_instance

    assert main(["run", "--workflow", "security_reversing_audit"]) == 0
    out, err = capsys.readouterr()
    assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> completed" in out

@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_known_workflow_json(mock_orchestrator, capsys):
    mock_instance = MagicMock()
    mock_instance.execute_cross_station_workflow.return_value = {
        "workflow_name": "security_reversing_audit",
        "status": "failed",
        "ok": False
    }
    mock_orchestrator.return_value = mock_instance

    # Should return 1 since ok is False
    assert main(["run", "--workflow", "security_reversing_audit", "--json"]) == 1
    out, err = capsys.readouterr()
    data = json.loads(out)
    assert data["workflow_name"] == "security_reversing_audit"
    assert data["status"] == "failed"
    assert data["ok"] is False

def test_main_system_exit_custom_code():
    with patch("argparse.ArgumentParser.parse_args", side_effect=SystemExit(42)):
        assert main([]) == 42

def test_main_system_exit_no_code():
    with patch("argparse.ArgumentParser.parse_args", side_effect=SystemExit(None)):
        assert main([]) == 1

def test_main_system_exit_string_code():
    with patch("argparse.ArgumentParser.parse_args", side_effect=SystemExit("error")):
        assert main([]) == 1
