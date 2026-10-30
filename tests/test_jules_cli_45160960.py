import pytest
import sys
import json
import argparse
from pathlib import Path
from unittest.mock import patch, MagicMock

# Assuming standard path insertion is needed if testing directly via pytest in root
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


def test_format_cli_diagnostic_full():
    res = format_cli_diagnostic({"command": "run", "json": True, "extra": 1})
    assert res == {"arg_count": 3, "command_type": "run", "has_json_flag": True}


def test_format_cli_diagnostic_false_json():
    res = format_cli_diagnostic({"command": "topology", "json": False})
    assert res == {"arg_count": 2, "command_type": "topology", "has_json_flag": False}


def test_validate_cli_args_missing_command():
    res = validate_cli_args({"json": True})
    assert res["valid"] is False
    assert res["command"] is None
    assert "Unknown command: None" in res["errors"]


def test_validate_cli_args_unknown_command():
    res = validate_cli_args({"command": "invalid"})
    assert res["valid"] is False
    assert res["command"] == "invalid"
    assert "Unknown command: invalid" in res["errors"]


def test_validate_cli_args_valid_topology():
    res = validate_cli_args({"command": "topology", "json": True})
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
    args = parser.parse_args(["topology", "--json"])
    assert args.command == "topology"
    assert args.json is True


def test_build_parser_run():
    parser = build_parser()
    args = parser.parse_args(["run", "--workflow", "sec_audit", "--json"])
    assert args.command == "run"
    assert args.workflow == "sec_audit"
    assert args.json is True


def test_build_parser_invalid_command(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["unknown"])
    out, err = capsys.readouterr()
    assert "invalid choice: 'unknown'" in err


def test_build_parser_run_missing_workflow(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run"])
    out, err = capsys.readouterr()
    assert "the following arguments are required: --workflow" in err


@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology_no_json(mock_orchestrator, capsys):
    mock_instance = mock_orchestrator.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 5, "mesh_digest": "1234567890abcdef1234"}
    
    assert main(["topology"]) == 0
    out, err = capsys.readouterr()
    assert "[AGY-FEDERATION] Nodes: 5 | Digest: 1234567890abcdef" in out


@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_topology_with_json(mock_orchestrator, capsys):
    mock_instance = mock_orchestrator.return_value
    mock_instance.discover_nodes.return_value = {"node_count": 5, "mesh_digest": "1234"}
    
    assert main(["topology", "--json"]) == 0
    out, err = capsys.readouterr()
    parsed_out = json.loads(out)
    assert parsed_out["node_count"] == 5
    assert parsed_out["mesh_digest"] == "1234"


@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_known_workflow_no_json(mock_orchestrator, capsys):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"workflow_name": "security_reversing_audit", "status": "COMPLETED", "ok": True}
    
    assert main(["run", "--workflow", "security_reversing_audit"]) == 0
    out, err = capsys.readouterr()
    assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> COMPLETED" in out


@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_known_workflow_with_json(mock_orchestrator, capsys):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"workflow_name": "security_reversing_audit", "status": "COMPLETED", "ok": True}
    
    assert main(["run", "--workflow", "security_reversing_audit", "--json"]) == 0
    out, err = capsys.readouterr()
    parsed_out = json.loads(out)
    assert parsed_out["workflow_name"] == "security_reversing_audit"
    assert parsed_out["status"] == "COMPLETED"


@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_known_workflow_fails(mock_orchestrator, capsys):
    mock_instance = mock_orchestrator.return_value
    mock_instance.execute_cross_station_workflow.return_value = {"workflow_name": "security_reversing_audit", "status": "FAILED", "ok": False}
    
    assert main(["run", "--workflow", "security_reversing_audit"]) == 1


@patch("agy_federation_mesh.cli.FederationMeshOrchestrator")
def test_main_run_unknown_workflow(mock_orchestrator, capsys):
    assert main(["run", "--workflow", "unknown_workflow"]) == 1
    out, err = capsys.readouterr()
    assert "Unknown workflow: unknown_workflow" in err


def test_main_invalid_args():
    # Calling without enough arguments triggers argparse's SystemExit
    assert main([]) == 2 # argparse typically exits with code 2
