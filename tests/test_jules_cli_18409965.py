"""Tests for agy_federation_mesh.cli."""

import json
import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import (
    format_cli_diagnostic,
    validate_cli_args,
    build_parser,
    main,
)


def test_format_cli_diagnostic_empty_args():
    res = format_cli_diagnostic({})
    assert res == {"arg_count": 0, "command_type": None, "has_json_flag": False}

def test_format_cli_diagnostic_full_args():
    res = format_cli_diagnostic({"command": "run", "json": True, "workflow": "test"})
    assert res == {"arg_count": 3, "command_type": "run", "has_json_flag": True}

def test_validate_cli_args_unknown_command():
    res = validate_cli_args({"command": "invalid_cmd"})
    assert res["valid"] is False
    assert "Unknown command: invalid_cmd" in res["errors"][0]

def test_validate_cli_args_missing_workflow():
    res = validate_cli_args({"command": "run"})
    assert res["valid"] is False
    assert "Missing required parameter: workflow" in res["errors"][0]

def test_validate_cli_args_valid_run():
    res = validate_cli_args({"command": "run", "workflow": "audit"})
    assert res["valid"] is True
    assert not res["errors"]

def test_validate_cli_args_valid_topology():
    res = validate_cli_args({"command": "topology"})
    assert res["valid"] is True
    assert not res["errors"]

def test_build_parser_missing_command():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])

def test_build_parser_valid_commands():
    parser = build_parser()
    args = parser.parse_args(["topology", "--json"])
    assert args.command == "topology"
    assert args.json is True

    args = parser.parse_args(["run", "--workflow", "w1"])
    assert args.command == "run"
    assert args.workflow == "w1"
    assert args.json is False

def test_main_no_args():
    # Calling main with no args should trigger SystemExit and return code > 0
    assert main([]) != 0

def test_main_topology_stdout(capsys):
    with patch("agy_federation_mesh.cli.FederationMeshOrchestrator") as mock_orch:
        instance = mock_orch.return_value
        instance.discover_nodes.return_value = {"node_count": 5, "mesh_digest": "abcdef01234567890"}
        
        assert main(["topology"]) == 0
        out, _ = capsys.readouterr()
        assert "[AGY-FEDERATION] Nodes: 5 | Digest: abcdef0123456789" in out

def test_main_topology_json(capsys):
    with patch("agy_federation_mesh.cli.FederationMeshOrchestrator") as mock_orch:
        instance = mock_orch.return_value
        instance.discover_nodes.return_value = {"node_count": 3, "mesh_digest": "123"}
        
        assert main(["topology", "--json"]) == 0
        out, _ = capsys.readouterr()
        data = json.loads(out)
        assert data["node_count"] == 3

def test_main_run_success(capsys):
    with patch("agy_federation_mesh.cli.FederationMeshOrchestrator") as mock_orch:
        instance = mock_orch.return_value
        instance.execute_cross_station_workflow.return_value = {
            "workflow_name": "security_reversing_audit",
            "status": "completed",
            "ok": True
        }
        
        assert main(["run", "--workflow", "security_reversing_audit"]) == 0
        out, _ = capsys.readouterr()
        assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> completed" in out

def test_main_run_json(capsys):
    with patch("agy_federation_mesh.cli.FederationMeshOrchestrator") as mock_orch:
        instance = mock_orch.return_value
        instance.execute_cross_station_workflow.return_value = {
            "workflow_name": "security_reversing_audit",
            "status": "completed",
            "ok": True
        }
        
        assert main(["run", "--workflow", "security_reversing_audit", "--json"]) == 0
        out, _ = capsys.readouterr()
        data = json.loads(out)
        assert data["ok"] is True

def test_main_run_failure():
    with patch("agy_federation_mesh.cli.FederationMeshOrchestrator") as mock_orch:
        instance = mock_orch.return_value
        instance.execute_cross_station_workflow.return_value = {
            "workflow_name": "security_reversing_audit",
            "status": "failed",
            "ok": False
        }
        
        assert main(["run", "--workflow", "security_reversing_audit"]) == 1

def test_main_run_unknown_workflow(capsys):
    assert main(["run", "--workflow", "invalid_workflow"]) == 1
    _, err = capsys.readouterr()
    assert "Unknown workflow: invalid_workflow" in err

def test_main_sys_exit_non_int():
    # If parse_args raises SystemExit with non-int, main returns 1
    parser_mock = MagicMock()
    parser_mock.parse_args.side_effect = SystemExit("string error")
    with patch("agy_federation_mesh.cli.build_parser", return_value=parser_mock):
        assert main(["topology"]) == 1

def test_main_sys_exit_int():
    # If parse_args raises SystemExit with int, main returns it
    parser_mock = MagicMock()
    parser_mock.parse_args.side_effect = SystemExit(42)
    with patch("agy_federation_mesh.cli.build_parser", return_value=parser_mock):
        assert main(["topology"]) == 42
