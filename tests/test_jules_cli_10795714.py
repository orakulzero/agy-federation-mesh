import pytest
from unittest.mock import patch
from io import StringIO
import sys
import json
import argparse

from agy_federation_mesh.cli import main, build_parser
from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator

def test_build_parser():
    parser = build_parser()
    assert isinstance(parser, argparse.ArgumentParser)
    assert parser.prog == "agy-federation-mesh"

def test_main_no_args():
    # Should exit with code 2 due to missing required arguments
    exit_code = main([])
    assert exit_code == 2

def test_main_help(capsys):
    # Should exit with code 0 for --help
    exit_code = main(["--help"])
    assert exit_code == 0

def test_topology_no_json(capsys):
    mock_result = {"node_count": 4, "mesh_digest": "1234567890abcdef123456"}
    with patch.object(FederationMeshOrchestrator, "discover_nodes", return_value=mock_result):
        exit_code = main(["topology"])
        assert exit_code == 0
        captured = capsys.readouterr()
        assert "[AGY-FEDERATION] Nodes: 4 | Digest: 1234567890abcdef" in captured.out

def test_topology_with_json(capsys):
    mock_result = {"node_count": 4, "mesh_digest": "1234567890abcdef123456"}
    with patch.object(FederationMeshOrchestrator, "discover_nodes", return_value=mock_result):
        exit_code = main(["topology", "--json"])
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["node_count"] == 4
        assert data["mesh_digest"] == "1234567890abcdef123456"

def test_run_workflow_success_no_json(capsys):
    mock_result = {"ok": True, "workflow_name": "security_reversing_audit", "status": "SUCCESS"}
    with patch.object(FederationMeshOrchestrator, "execute_cross_station_workflow", return_value=mock_result):
        exit_code = main(["run", "--workflow", "security_reversing_audit"])
        assert exit_code == 0
        captured = capsys.readouterr()
        assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> SUCCESS" in captured.out

def test_run_workflow_success_json(capsys):
    mock_result = {"ok": True, "workflow_name": "security_reversing_audit", "status": "SUCCESS"}
    with patch.object(FederationMeshOrchestrator, "execute_cross_station_workflow", return_value=mock_result):
        exit_code = main(["run", "--workflow", "security_reversing_audit", "--json"])
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["ok"] is True
        assert data["status"] == "SUCCESS"

def test_run_workflow_failure_no_json(capsys):
    mock_result = {"ok": False, "workflow_name": "security_reversing_audit", "status": "FAILURE"}
    with patch.object(FederationMeshOrchestrator, "execute_cross_station_workflow", return_value=mock_result):
        exit_code = main(["run", "--workflow", "security_reversing_audit"])
        assert exit_code == 1
        captured = capsys.readouterr()
        assert "[AGY-FEDERATION] Workflow: security_reversing_audit -> FAILURE" in captured.out

def test_run_workflow_failure_json(capsys):
    mock_result = {"ok": False, "workflow_name": "security_reversing_audit", "status": "FAILURE"}
    with patch.object(FederationMeshOrchestrator, "execute_cross_station_workflow", return_value=mock_result):
        exit_code = main(["run", "--workflow", "security_reversing_audit", "--json"])
        assert exit_code == 1
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["ok"] is False
        assert data["status"] == "FAILURE"

def test_run_unknown_workflow(capsys):
    exit_code = main(["run", "--workflow", "unknown_workflow"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Unknown workflow: unknown_workflow" in captured.err

def test_main_systemexit_non_int():
    # If parse_args raises SystemExit with non-int, return 1
    class NonIntExit(SystemExit):
        def __init__(self, code="error"):
            self.code = code
    
    with patch("argparse.ArgumentParser.parse_args", side_effect=NonIntExit()):
        exit_code = main(["something"])
        assert exit_code == 1
