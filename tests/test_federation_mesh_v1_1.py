"""
ISO 9001:2015 / Agile V Quality Pipeline Test Suite v1.1.0
Component: AGY Federation Mesh v1.1.0 Feature Expansion & Tour Dossier Validation
"""

import json
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any

import pytest

from agy_federation_mesh.l1_primitives import (
    DAGPipelineEngine,
    StationNode,
    WorkflowTask,
    compute_mesh_digest,
)
from agy_federation_mesh.l3_pipeline import (
    REVIEW_DIMENSIONS,
    VALID_EPISTEMIC_BASES,
    FederationMeshOrchestrator,
    drop_revealing_asks,
    synthesize_diff_tour_dossier,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_v1_1_compute_execution_waves_diamond_dag():
    """Verify compute_execution_waves partitions diamond DAG into distinct execution waves."""
    t_init = WorkflowTask(task_id="t0", station_id="mission_control", action="init", payload={})
    t_sec = WorkflowTask(task_id="t1_sec", station_id="secret_shield", action="scan", payload={}, depends_on=["t0"])
    t_pe = WorkflowTask(task_id="t1_pe", station_id="pe_reversing", action="triage", payload={}, depends_on=["t0"])
    t_join = WorkflowTask(task_id="t2_join", station_id="flow_studio", action="render", payload={}, depends_on=["t1_sec", "t1_pe"])

    waves = DAGPipelineEngine.compute_execution_waves([t_join, t_sec, t_pe, t_init])

    assert len(waves) == 3
    assert waves[0] == ["t0"]
    assert sorted(waves[1]) == ["t1_pe", "t1_sec"]
    assert waves[2] == ["t2_join"]


def test_v1_1_compute_execution_waves_multi_branch_and_cycles():
    """Verify compute_execution_waves correctly catches cycles and missing dependencies."""
    # Independent parallel tasks in wave 0
    t_a = WorkflowTask(task_id="ta", station_id="s1", action="a", payload={})
    t_b = WorkflowTask(task_id="tb", station_id="s2", action="b", payload={})
    t_c = WorkflowTask(task_id="tc", station_id="s3", action="c", payload={}, depends_on=["ta"])

    waves = DAGPipelineEngine.compute_execution_waves([t_a, t_b, t_c])
    assert waves == [["ta", "tb"], ["tc"]]

    # Cycle detection
    c1 = WorkflowTask(task_id="c1", station_id="s1", action="a", payload={}, depends_on=["c2"])
    c2 = WorkflowTask(task_id="c2", station_id="s2", action="b", payload={}, depends_on=["c1"])
    with pytest.raises(ValueError, match="Cyclic dependency detected"):
        DAGPipelineEngine.compute_execution_waves([c1, c2])

    # Unknown dependency
    u1 = WorkflowTask(task_id="u1", station_id="s1", action="a", payload={}, depends_on=["missing"])
    with pytest.raises(ValueError, match="references unknown dependency"):
        DAGPipelineEngine.compute_execution_waves([u1])


def test_v1_1_parallel_wave_execution_concurrency_and_provenance():
    """Verify execute_workflow_parallel_waves executes tasks wave-by-wave with ThreadPoolExecutor."""
    orchestrator = FederationMeshOrchestrator()

    t0 = WorkflowTask(task_id="t0", station_id="mission_control", action="init", payload={})
    t1a = WorkflowTask(task_id="t1a", station_id="pe_reversing", action="parse", payload={}, depends_on=["t0"])
    t1b = WorkflowTask(task_id="t1b", station_id="secret_shield", action="scan", payload={}, depends_on=["t0"])
    t2 = WorkflowTask(task_id="t2", station_id="flow_studio", action="synthesize", payload={}, depends_on=["t1a", "t1b"])

    tasks = [t2, t1b, t1a, t0]
    res = orchestrator.execute_workflow_parallel_waves("parallel_test_wf", tasks, max_workers=2)

    assert res["ok"] is True
    assert res["status"] == "COMPLETED"
    assert res["tasks_executed"] == 4
    assert len(res["results"]) == 4
    assert all(r["ok"] is True for r in res["results"].values())

    # Check provenance trace
    prov = res["prov_trace"]
    assert len(prov) == 4
    wave_map = {item["task_id"]: item["wave"] for item in prov}
    assert wave_map["t0"] == 0
    assert wave_map["t1a"] == 1
    assert wave_map["t1b"] == 1
    assert wave_map["t2"] == 2


def test_v1_1_parallel_wave_circuit_breaker_halting():
    """Verify Circuit Breaker halts execution immediately when a station fails in Wave 1."""
    orchestrator = FederationMeshOrchestrator()

    t0 = WorkflowTask(task_id="t0", station_id="mission_control", action="init", payload={})
    # t1a fails, t1b succeeds
    t1a = WorkflowTask(task_id="t1a", station_id="pe_reversing", action="raise_error_action", payload={}, depends_on=["t0"])
    t1b = WorkflowTask(task_id="t1b", station_id="secret_shield", action="scan", payload={}, depends_on=["t0"])
    # t2 should NEVER be executed
    t2 = WorkflowTask(task_id="t2", station_id="flow_studio", action="render", payload={}, depends_on=["t1a", "t1b"])

    tasks = [t0, t1a, t1b, t2]
    res = orchestrator.execute_workflow_parallel_waves("fail_wf", tasks, max_workers=2)

    assert res["ok"] is False
    assert res["status"] == "FAILED"
    assert "Execution halted at wave 1" in res["error"]
    assert res["tasks_executed"] == 3  # t0 + (t1a, t1b); t2 was skipped!
    assert "t2" not in res["results"]
    assert res["results"]["t1a"]["ok"] is False


def test_v1_1_drop_revealing_asks_algorithm():
    """Verify drop_revealing_asks strips asks from question steps and immediately preceding steps."""
    tour = {
        "title": "Review Tour",
        "steps": [
            {"title": "Step 0", "asks": ["Ask 0"]},
            {"title": "Step 1", "asks": ["Ask 1"]},
            {"title": "Step 2", "question": True, "asks": ["Ask 2"]},
            {"title": "Step 3", "asks": ["Ask 3"]},
        ],
        "chapters": [
            {
                "title": "Ch 1",
                "steps": [
                    {"title": "Ch Step 0", "asks": ["Ch Ask 0"]},
                    {"title": "Ch Step 1", "question": True, "asks": ["Ch Ask 1"]},
                ],
            }
        ],
    }

    sanitized = drop_revealing_asks(tour)

    # In top-level steps: Step 2 is question, so Step 1 and 2 must have asks stripped
    assert "asks" in sanitized["steps"][0]
    assert "asks" not in sanitized["steps"][1]
    assert "asks" not in sanitized["steps"][2]
    assert "asks" in sanitized["steps"][3]

    # In chapter steps: Step 1 is question, so Step 0 and 1 must have asks stripped
    assert "asks" not in sanitized["chapters"][0]["steps"][0]
    assert "asks" not in sanitized["chapters"][0]["steps"][1]


def test_v1_1_synthesize_diff_tour_dossier_native_tour_cli(tmp_path):
    """Verify synthesize_diff_tour_dossier produces valid diff-tour v1 tested against tour.exe."""
    sec_result = {"clean": True, "quarantined_count": 0}
    pe_result = {"exports": ["VirtualAlloc", "CreateThread"]}
    eskd_result = {"total_sheets": 3, "violations": []}
    telemetry = {"mesh_digest": "0123456789abcdef" * 4}

    dossier = synthesize_diff_tour_dossier(
        repo_path=str(REPO_ROOT),
        secret_shield_result=sec_result,
        pe_reversing_result=pe_result,
        eskd_vision_result=eskd_result,
        mesh_telemetry=telemetry,
        title="Federated Mission Dossier Test",
    )

    assert dossier["version"] == 1
    assert dossier["title"] == "Federated Mission Dossier Test"
    assert "chapters" in dossier and len(dossier["chapters"]) >= 3
    assert "steps" in dossier and len(dossier["steps"]) == 8

    # Verify all 8 Senior Review Dimensions are present in steps
    dims = {s["dimension"] for s in dossier["steps"]}
    assert dims == REVIEW_DIMENSIONS

    # Verify all epistemic bases are valid
    for s in dossier["steps"]:
        assert s["epistemic_basis"] in VALID_EPISTEMIC_BASES
        assert s["line_start"] >= 1
        assert s["line_end"] >= s["line_start"]

    # Write out and execute with native tour.exe if installed
    tour_file = tmp_path / "mission_dossier.tour.json"
    tour_file.write_text(json.dumps(dossier, indent=2), encoding="utf-8")

    tour_bin = shutil.which("tour") or "D:/Go/bin/tour.exe"
    if Path(tour_bin).is_file():
        cmd = [
            str(tour_bin),
            "--repo",
            str(REPO_ROOT),
            "--dump",
            "--cols",
            "110",
            "--rows",
            "32",
            "--plain",
            str(tour_file),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        assert res.returncode == 0, f"tour.exe returned error code {res.returncode}: {res.stderr}"
        assert "Federated Mission Dossier Test" in res.stdout
