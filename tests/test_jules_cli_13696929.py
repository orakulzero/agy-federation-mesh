"""Unit test for CLI unknown workflow handling (REQ-CLI-002)."""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh import cli


def test_cli_run_unknown_workflow_returns_error(capsys):
    """Calling main with an unknown workflow must return exit code 1 and report on stderr."""
    exit_code = cli.main(["run", "--workflow", "unknown_workflow"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Unknown workflow" in captured.err
    assert "unknown_workflow" in captured.err
    assert captured.out == ""


def test_cli_run_unknown_workflow_does_not_raise(capsys):
    """Unknown workflow must be handled gracefully without raising exceptions."""
    try:
        result = cli.main(["run", "--workflow", "does_not_exist"])
    except SystemExit as exc:  # pragma: no cover - defensive
        pytest.fail(f"main() raised SystemExit unexpectedly: {exc}")

    assert result == 1
    err = capsys.readouterr().err
    assert "Unknown workflow: does_not_exist" in err


def test_cli_build_parser_run_subcommand_requires_workflow():
    """The 'run' subcommand must require the --workflow argument."""
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["run"])
