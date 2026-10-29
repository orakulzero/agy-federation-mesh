"""Test suite for validate_cli_args helper function."""

import pytest
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import validate_cli_args


def test_validate_cli_args_topology_command():
    """Verify validate_cli_args accepts valid topology command arguments."""
    args = {"command": "topology", "json": False}
    result = validate_cli_args(args)
    assert result["valid"] is True
    assert result["command"] == "topology"
    assert "errors" not in result or len(result["errors"]) == 0


def test_validate_cli_args_run_command_missing_workflow():
    """Verify validate_cli_args detects missing workflow parameter."""
    args = {"command": "run", "json": True}
    result = validate_cli_args(args)
    assert result["valid"] is False
    assert "errors" in result
    assert any("workflow" in err.lower() for err in result["errors"])


def test_validate_cli_args_unknown_command():
    """Verify validate_cli_args rejects unrecognized command."""
    args = {"command": "invalid_cmd", "json": False}
    result = validate_cli_args(args)
    assert result["valid"] is False
    assert "errors" in result
    assert any("command" in err.lower() for err in result["errors"])
