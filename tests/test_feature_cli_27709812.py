"""Test suite for format_cli_diagnostic helper function."""

import pytest
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.cli import format_cli_diagnostic


def test_format_cli_diagnostic_returns_structured_dict():
    """Verify format_cli_diagnostic returns a dictionary with required keys."""
    args = {"command": "topology", "json": False}
    diagnostic = format_cli_diagnostic(args)
    
    assert isinstance(diagnostic, dict)
    assert "arg_count" in diagnostic
    assert "command_type" in diagnostic
    assert "has_json_flag" in diagnostic


def test_format_cli_diagnostic_counts_arguments_correctly():
    """Verify format_cli_diagnostic accurately counts provided arguments."""
    args = {"command": "run", "workflow": "security_reversing_audit", "json": True}
    diagnostic = format_cli_diagnostic(args)
    
    assert diagnostic["arg_count"] == 3
    assert diagnostic["command_type"] == "run"
    assert diagnostic["has_json_flag"] is True


def test_format_cli_diagnostic_handles_minimal_input():
    """Verify format_cli_diagnostic gracefully handles minimal argument sets."""
    args = {"command": "topology"}
    diagnostic = format_cli_diagnostic(args)
    
    assert diagnostic["arg_count"] == 1
    assert diagnostic["command_type"] == "topology"
    assert diagnostic["has_json_flag"] is False
