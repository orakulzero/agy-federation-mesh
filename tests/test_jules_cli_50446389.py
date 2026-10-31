"""Test suite for agy_federation_mesh.cli module."""
import json
import pytest
import sys
from unittest.mock import patch, MagicMock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import (
    format_cli_diagnostic,
    validate_cli_args,
    build_parser,
    main
)
from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator


def test_format_cli_diagnostic_empty():
    """Test format_cli_diagnostic with empty args."""
    args = {}
    res = format_cli_diagnostic(args)
    assert res == {
        "arg_count": 0,
        "command_type": None,
        "has_json_flag": False
    }


def test_format_cli_diagnostic_valid():
    """Test format_cli_diagnostic with typical args."""
    args = {"command": "run", "json": True, "workflow": "test"}
    res = format_cli_diagnostic(args)
    assert res == {
        "arg_count": 3,
        "command_type": "run",
        "has_json_flag": True
    }


def test_validate_cli_args_topology():
    """Test validate_cli_args with topology command."""
    args = {"command": "topology"}
    res = validate_cli_args(args)
    assert res["valid"] is True
    assert res["command"] == "topology"
    assert res["errors"] == []


def test_validate_cli_args_run_missing_workflow():
    """Test validate_cli_args with run command missing workflow."""
    args = {"command": "run"}
    res = validate_cli_args(args)
    assert res["valid"] is False
    assert "Missing required parameter: workflow" in res["errors"]


def test_validate_cli_args_run_valid():
    """Test validate_cli_args with valid run command."""
    args = {"command": "run", "workflow": "security_reversing_audit"}
    res = validate_cli_args(args)
    assert res["valid"] is True
    assert res["errors"] == []


def test_validate_cli_args_unknown():
    """Test validate_cli_args with unknown command."""
    args = {"command": "unknown_cmd"}
    res = validate_cli_args(args)
    assert res["valid"] is False
    assert "Unknown command: unknown_cmd" in res["errors"]


def test_build_parser():
    """Test that build_parser produces a valid parser."""
    parser = build_parser()
    
    # Test topology parse
    args = parser.parse_args(["topology", "--json"])
    assert args.command == "topology"
    assert args.json is True

    # Test run parse
    args = parser.parse_args(["run", "--workflow", "security_reversing_audit"])
    assert args.command == "run"
    assert args.workflow == "security_reversing_audit"
    assert args.json is False


@patch("sys.stdout")
def test_main_topology(mock_stdout):
    """Test main with topology command."""
    with patch.object(FederationMeshOrchestrator, "discover_nodes", return_value={"node_count": 42, "mesh_digest": "abcdef1234567890123"}):
        res = main(["topology"])
        assert res == 0
        output = mock_stdout.write.call_args_list[0][0][0]
        assert "[AGY-FEDERATION] Nodes: 42" in output
        assert "Digest: abcdef1234567890" in output


@patch("sys.stdout")
def test_main_topology_json(mock_stdout):
    """Test main with topology command and json flag."""
    with patch.object(FederationMeshOrchestrator, "discover_nodes", return_value={"node_count": 42, "mesh_digest": "abcdef1234567890123"}):
        res = main(["topology", "--json"])
        assert res == 0
        output = mock_stdout.write.call_args_list[0][0][0]
        data = json.loads(output)
        assert data["node_count"] == 42
        assert data["mesh_digest"] == "abcdef1234567890123"


@patch("sys.stdout")
def test_main_run(mock_stdout):
    """Test main with run command."""
    with patch.object(FederationMeshOrchestrator, "execute_cross_station_workflow", return_value={"workflow_name": "security_reversing_audit", "status": "COMPLETED", "ok": True}):
        res = main(["run", "--workflow", "security_reversing_audit"])
        assert res == 0
        output = mock_stdout.write.call_args_list[0][0][0]
        assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> COMPLETED" in output


@patch("sys.stdout")
def test_main_run_json(mock_stdout):
    """Test main with run command and json flag."""
    with patch.object(FederationMeshOrchestrator, "execute_cross_station_workflow", return_value={"workflow_name": "security_reversing_audit", "status": "COMPLETED", "ok": True}):
        res = main(["run", "--workflow", "security_reversing_audit", "--json"])
        assert res == 0
        output = "".join([call[0][0] for call in mock_stdout.write.call_args_list])
        data = json.loads(output)
        assert data["workflow_name"] == "security_reversing_audit"
        assert data["status"] == "COMPLETED"
        assert data["ok"] is True


@patch("sys.stderr")
def test_main_unknown_workflow(mock_stderr):
    """Test main with run command and unknown workflow."""
    res = main(["run", "--workflow", "unknown_workflow"])
    assert res == 1
    output = mock_stderr.write.call_args_list[0][0][0]
    assert "Unknown workflow: unknown_workflow" in output


def test_main_missing_args():
    """Test main with missing arguments."""
    res = main([])
    assert res == 2  # argparse exits with code 2 on error


def test_main_sys_argv():
    """Test main without arguments (uses sys.argv)."""
    with patch.object(sys, "argv", ["prog", "topology"]):
        with patch.object(FederationMeshOrchestrator, "discover_nodes", return_value={"node_count": 1, "mesh_digest": "abcdef0123456789"}):
            with patch("sys.stdout"):
                res = main()
                assert res == 0
