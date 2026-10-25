"""
ISO 9001:2015 / Agile V Quality Pipeline Test Suite — Tier L5 Mesh v1.2.0
Baseline: BL-20261025-FEDERATION-L5-V120
Component: AGY Federation Mesh v1.2.0 Expansion & Property Test Matrix
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, strategies as st

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.l1_primitives import (
    DAGPipelineEngine,
    StationLoadBalancer,
    StationReplica,
    WorkflowTask,
    export_dag_dot,
    export_dag_mermaid,
)
from agy_federation_mesh.l3_pipeline import (
    FederationMeshOrchestrator,
    execute_workflow_with_retry,
)
from agy_federation_mesh.l4_app import app


# ============================================================================
# 1. StationReplica & StationLoadBalancer Primitives Tests
# ============================================================================

def test_v1_2_station_replica_model_and_defaults():
    """Verify StationReplica dataclass initialization, default values, and serialization."""
    replica = StationReplica(
        base_url="http://127.0.0.1:8770",
        station_id="secret_shield",
        capabilities=["secret_scan", "dpapi_seal"],
    )
    assert replica.base_url == "http://127.0.0.1:8770"
    assert replica.station_id == "secret_shield"
    assert replica.latency_ema_ms == 10.0
    assert replica.inflight_count == 0
    assert replica.weight == 1.0
    assert replica.status == "ONLINE"
    assert replica.consecutive_failures == 0
    assert "secret_scan" in replica.capabilities

    data = replica.to_dict()
    assert isinstance(data, dict)
    assert data["base_url"] == "http://127.0.0.1:8770"
    assert data["status"] == "ONLINE"


def test_v1_2_load_balancer_registration_and_filtering():
    """Verify StationLoadBalancer adds, updates, and filters replicas."""
    lb = StationLoadBalancer()
    assert lb.get_replicas() == []

    r1 = StationReplica(base_url="http://127.0.0.1:8001", station_id="pe_reversing")
    r2 = StationReplica(base_url="http://127.0.0.1:8002", station_id="secret_shield")
    lb.add_replica(r1)
    lb.add_replica(r2)

    assert len(lb.get_replicas()) == 2
    assert len(lb.get_replicas("pe_reversing")) == 1
    assert lb.get_replicas("pe_reversing")[0].base_url == "http://127.0.0.1:8001"

    # Update existing replica by base_url
    r1_updated = StationReplica(
        base_url="http://127.0.0.1:8001",
        station_id="pe_reversing",
        weight=2.0,
    )
    lb.add_replica(r1_updated)
    assert len(lb.get_replicas()) == 2
    assert lb.get_replicas("pe_reversing")[0].weight == 2.0


def test_v1_2_load_balancer_selection_scoring_formula():
    """Verify select_replica strictly minimizes (latency_ema_ms * (1 + inflight)) / weight."""
    lb = StationLoadBalancer()
    # Replica 1: latency=20, inflight=0, weight=1 -> score = 20 * 1 / 1 = 20
    r1 = StationReplica(
        base_url="http://127.0.0.1:9001",
        station_id="worker_station",
        latency_ema_ms=20.0,
        inflight_count=0,
        weight=1.0,
    )
    # Replica 2: latency=30, inflight=0, weight=2 -> score = 30 * 1 / 2 = 15 (winner)
    r2 = StationReplica(
        base_url="http://127.0.0.1:9002",
        station_id="worker_station",
        latency_ema_ms=30.0,
        inflight_count=0,
        weight=2.0,
    )
    # Replica 3: latency=10, inflight=3, weight=1 -> score = 10 * 4 / 1 = 40
    r3 = StationReplica(
        base_url="http://127.0.0.1:9003",
        station_id="worker_station",
        latency_ema_ms=10.0,
        inflight_count=3,
        weight=1.0,
    )
    lb.add_replica(r1)
    lb.add_replica(r2)
    lb.add_replica(r3)

    selected = lb.select_replica("worker_station")
    assert selected.base_url == "http://127.0.0.1:9002"


def test_v1_2_load_balancer_capability_requirement():
    """Verify select_replica enforces required_capability or raises ValueError."""
    lb = StationLoadBalancer()
    r1 = StationReplica(
        base_url="http://127.0.0.1:9101",
        station_id="pe_lab",
        capabilities=["iat_triage"],
    )
    r2 = StationReplica(
        base_url="http://127.0.0.1:9102",
        station_id="pe_lab",
        capabilities=["iat_triage", "entropy_scan"],
    )
    lb.add_replica(r1)
    lb.add_replica(r2)

    # Both have iat_triage
    selected = lb.select_replica("pe_lab", required_capability="iat_triage")
    assert selected in [r1, r2]

    # Only r2 has entropy_scan
    selected_entropy = lb.select_replica("pe_lab", required_capability="entropy_scan")
    assert selected_entropy.base_url == "http://127.0.0.1:9102"

    # Unsupported capability raises ValueError
    with pytest.raises(ValueError, match="support capability 'yara_synth'"):
        lb.select_replica("pe_lab", required_capability="yara_synth")


def test_v1_2_load_balancer_graceful_fallback_and_offline_filtering():
    """Verify load balancer prefers ONLINE over DEGRADED, falls back gracefully, and excludes OFFLINE."""
    lb = StationLoadBalancer()
    r_online = StationReplica(
        base_url="http://127.0.0.1:9201",
        station_id="data_station",
        status="ONLINE",
        latency_ema_ms=50.0,
    )
    r_degraded = StationReplica(
        base_url="http://127.0.0.1:9202",
        station_id="data_station",
        status="DEGRADED",
        latency_ema_ms=10.0,
    )
    r_offline = StationReplica(
        base_url="http://127.0.0.1:9203",
        station_id="data_station",
        status="OFFLINE",
        latency_ema_ms=5.0,
    )
    lb.add_replica(r_online)
    lb.add_replica(r_degraded)
    lb.add_replica(r_offline)

    # When ONLINE exists, ONLINE is selected even if degraded has lower latency
    assert lb.select_replica("data_station").base_url == "http://127.0.0.1:9201"

    # When ONLINE is removed or becomes OFFLINE, fallback to DEGRADED
    r_online.status = "OFFLINE"
    assert lb.select_replica("data_station").base_url == "http://127.0.0.1:9202"

    # When all become OFFLINE, raises ValueError
    r_degraded.status = "OFFLINE"
    with pytest.raises(ValueError, match="are OFFLINE"):
        lb.select_replica("data_station")


def test_v1_2_load_balancer_telemetry_ema_and_health_transitions():
    """Verify record_telemetry updates EMA latency and manages health status transitions."""
    lb = StationLoadBalancer()
    r = StationReplica(
        base_url="http://127.0.0.1:9301",
        station_id="node_a",
        latency_ema_ms=10.0,
        status="ONLINE",
    )
    lb.add_replica(r)

    # Successful call: EMA with alpha=0.5 -> 0.5 * 20.0 + 0.5 * 10.0 = 15.0
    lb.record_telemetry("http://127.0.0.1:9301", duration_ms=20.0, success=True, alpha=0.5)
    assert pytest.approx(r.latency_ema_ms, rel=1e-3) == 15.0
    assert r.status == "ONLINE"
    assert r.consecutive_failures == 0

    # Failure 1: consecutive_failures=1, stays ONLINE
    lb.record_telemetry("http://127.0.0.1:9301", duration_ms=100.0, success=False)
    assert r.consecutive_failures == 1
    assert r.status == "ONLINE"

    # Failure 2: consecutive_failures=2, demoted to DEGRADED
    lb.record_telemetry("http://127.0.0.1:9301", duration_ms=100.0, success=False)
    assert r.consecutive_failures == 2
    assert r.status == "DEGRADED"

    # Success: restores DEGRADED to ONLINE and resets consecutive_failures
    lb.record_telemetry("http://127.0.0.1:9301", duration_ms=12.0, success=True)
    assert r.status == "ONLINE"
    assert r.consecutive_failures == 0

    # 5 consecutive failures: transitions to OFFLINE
    for _ in range(5):
        lb.record_telemetry("http://127.0.0.1:9301", duration_ms=50.0, success=False)
    assert r.consecutive_failures == 5
    assert r.status == "OFFLINE"


# ============================================================================
# 2. Mermaid & DOT DAG Exporter Tests
# ============================================================================

def test_v1_2_export_dag_mermaid_syntax_and_subgraphs():
    """Verify export_dag_mermaid produces valid Mermaid syntax with wave subgraphs and edges."""
    tasks = [
        WorkflowTask(task_id="t_scan", station_id="secret_shield", action="scan", payload={}),
        WorkflowTask(
            task_id="t_parse",
            station_id="pe_reversing",
            action="parse",
            payload={},
            depends_on=["t_scan"],
        ),
        WorkflowTask(
            task_id="t_yara",
            station_id="pe_reversing",
            action="yara",
            payload={},
            depends_on=["t_scan"],
        ),
        WorkflowTask(
            task_id="t_report",
            station_id="mission_control",
            action="report",
            payload={},
            depends_on=["t_parse", "t_yara"],
        ),
    ]

    mermaid = export_dag_mermaid(tasks)
    assert mermaid.startswith("flowchart TD")
    assert "subgraph Wave_0" in mermaid
    assert "subgraph Wave_1" in mermaid
    assert "subgraph Wave_2" in mermaid
    assert "t_scan --> t_parse" in mermaid
    assert "t_scan --> t_yara" in mermaid
    assert "t_parse --> t_report" in mermaid
    assert "t_yara --> t_report" in mermaid

    # Empty task list returns clean flowchart
    assert export_dag_mermaid([]) == "flowchart TD\n"


def test_v1_2_export_dag_mermaid_with_execution_results():
    """Verify export_dag_mermaid embeds COMPLETED/FAILED execution results in node labels."""
    tasks = [
        WorkflowTask(task_id="t1", station_id="secret_shield", action="scan", payload={}),
        WorkflowTask(task_id="t2", station_id="pe_reversing", action="triage", payload={}, depends_on=["t1"]),
    ]
    results = {
        "t1": {"ok": True, "output": "clean"},
        "t2": {"ok": False, "error": "binary corrupt"},
    }

    mermaid = export_dag_mermaid(tasks, execution_results=results)
    assert "t1: secret_shield/scan [COMPLETED]" in mermaid
    assert "t2: pe_reversing/triage [FAILED]" in mermaid


def test_v1_2_export_dag_dot_syntax_and_clusters():
    """Verify export_dag_dot produces valid Graphviz DOT with cluster subgraphs and edges."""
    tasks = [
        WorkflowTask(task_id="step_a", station_id="station_a", action="act_a", payload={}),
        WorkflowTask(
            task_id="step_b",
            station_id="station_b",
            action="act_b",
            payload={},
            depends_on=["step_a"],
        ),
    ]
    results = {
        "step_a": {"ok": True},
        "step_b": {"ok": True},
    }

    dot = export_dag_dot(tasks, execution_results=results)
    assert dot.startswith("digraph FederationDAG {")
    assert "rankdir=TB;" in dot
    assert "subgraph cluster_wave_0 {" in dot
    assert "subgraph cluster_wave_1 {" in dot
    assert '"step_a" -> "step_b";' in dot
    assert "status: COMPLETED" in dot
    assert dot.strip().endswith("}")

    # Empty list produces empty valid digraph
    empty_dot = export_dag_dot([])
    assert "digraph FederationDAG {" in empty_dot
    assert empty_dot.strip().endswith("}")


# ============================================================================
# 3. Exponential Backoff Retry Pipeline Tests
# ============================================================================

def test_v1_2_execute_workflow_with_retry_transient_recovery():
    """Verify execute_workflow_with_retry recovers from transient station failure via retry."""
    orchestrator = FederationMeshOrchestrator()
    tasks = [
        WorkflowTask(
            task_id="task_transient",
            station_id="secret_shield",
            action="scan_with_retry",
            payload={"fail_attempts": 2},  # Fails attempts 1 & 2, succeeds on attempt 3
        ),
        WorkflowTask(
            task_id="task_downstream",
            station_id="mission_control",
            action="seal_status",
            payload={},
            depends_on=["task_transient"],
        ),
    ]

    res = orchestrator.execute_workflow_with_retry(
        workflow_name="test_retry_recovery",
        tasks=tasks,
        max_retries=3,
        base_delay_ms=2.0,
        backoff_factor=1.5,
    )

    assert res["ok"] is True
    assert res["status"] == "COMPLETED"
    assert res["tasks_executed"] == 2

    prov = res["prov_trace"]
    t_trace = next(p for p in prov if p["task_id"] == "task_transient")
    assert t_trace["attempt_count"] == 3
    assert len(t_trace["retry_history"]) == 2
    assert t_trace["retry_history"][0]["attempt"] == 1
    assert t_trace["retry_history"][1]["attempt"] == 2


def test_v1_2_execute_workflow_with_retry_circuit_breaker_exhaustion():
    """Verify circuit breaker trips when retries are exhausted, halting execution."""
    orchestrator = FederationMeshOrchestrator()
    tasks = [
        WorkflowTask(
            task_id="task_persist_fail",
            station_id="pe_reversing",
            action="crash_action",
            payload={"fail_attempts": 10},  # Fails 10 times, max_retries is only 2
        ),
        WorkflowTask(
            task_id="task_blocked",
            station_id="mission_control",
            action="should_not_run",
            payload={},
            depends_on=["task_persist_fail"],
        ),
    ]

    res = orchestrator.execute_workflow_with_retry(
        workflow_name="test_circuit_exhaustion",
        tasks=tasks,
        max_retries=2,
        base_delay_ms=1.0,
        backoff_factor=1.0,
    )

    assert res["ok"] is False
    assert res["status"] == "FAILED"
    assert "Execution halted at wave 0" in res["error"]
    assert "task_blocked" not in res["results"]

    prov = res["prov_trace"]
    fail_trace = next(p for p in prov if p["task_id"] == "task_persist_fail")
    # Initial attempt + 2 retries = 3 attempts total
    assert fail_trace["attempt_count"] == 3
    assert len(fail_trace["retry_history"]) == 2


# ============================================================================
# 4. FastAPI L4 Endpoints Tests
# ============================================================================

def test_v1_2_fastapi_dag_export_endpoint():
    """Verify POST /api/federation/dag/export for mermaid and dot formats."""
    client = TestClient(app)
    tasks_payload = [
        {"task_id": "step_1", "station_id": "secret_shield", "action": "scan", "depends_on": []},
        {"task_id": "step_2", "station_id": "pe_reversing", "action": "triage", "depends_on": ["step_1"]},
    ]

    # Mermaid export
    resp_mermaid = client.post(
        "/api/federation/dag/export",
        json={"tasks": tasks_payload, "format": "mermaid"},
    )
    assert resp_mermaid.status_code == 200
    data_mermaid = resp_mermaid.json()
    assert data_mermaid["ok"] is True
    assert data_mermaid["format"] == "mermaid"
    assert "flowchart TD" in data_mermaid["content"]
    assert "step_1 --> step_2" in data_mermaid["content"]

    # DOT export
    resp_dot = client.post(
        "/api/federation/dag/export",
        json={"tasks": tasks_payload, "format": "dot"},
    )
    assert resp_dot.status_code == 200
    data_dot = resp_dot.json()
    assert data_dot["ok"] is True
    assert data_dot["format"] == "dot"
    assert "digraph FederationDAG {" in data_dot["content"]
    assert '"step_1" -> "step_2";' in data_dot["content"]

    # Unsupported format rejection
    resp_invalid = client.post(
        "/api/federation/dag/export",
        json={"tasks": tasks_payload, "format": "unsupported_svg"},
    )
    assert resp_invalid.status_code == 400
    assert "Unsupported format" in resp_invalid.json()["detail"]


def test_v1_2_fastapi_workflow_retry_run_endpoint():
    """Verify POST /api/federation/workflow/retry-run endpoint."""
    client = TestClient(app)

    # Standard security_reversing_audit workflow
    resp = client.post(
        "/api/federation/workflow/retry-run",
        json={
            "workflow_name": "security_reversing_audit",
            "payload": {"target": "sample.bin"},
            "max_retries": 2,
            "base_delay_ms": 2.0,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["status"] == "COMPLETED"
    assert len(data["results"]) == 3

    # Custom tasks with transient retry
    custom_tasks = [
        {
            "task_id": "custom_1",
            "station_id": "secret_shield",
            "action": "custom_scan",
            "payload": {"fail_attempts": 1},
            "depends_on": [],
        }
    ]
    resp_custom = client.post(
        "/api/federation/workflow/retry-run",
        json={
            "workflow_name": "custom_retry_wf",
            "tasks": custom_tasks,
            "max_retries": 2,
            "base_delay_ms": 1.0,
        },
    )
    assert resp_custom.status_code == 200
    data_custom = resp_custom.json()
    assert data_custom["ok"] is True
    assert data_custom["prov_trace"][0]["attempt_count"] == 2


# ============================================================================
# 5. Hypothesis Property-Based Invariant Tests
# ============================================================================

@given(
    latencies=st.lists(st.floats(min_value=1.0, max_value=500.0), min_size=1, max_size=10),
    inflights=st.lists(st.integers(min_value=0, max_value=20), min_size=1, max_size=10),
    weights=st.lists(st.floats(min_value=0.5, max_value=10.0), min_size=1, max_size=10),
)
def test_v1_2_hypothesis_load_balancer_selection_invariants(latencies, inflights, weights):
    """Property test: select_replica strictly minimizes score across generated parameters."""
    n = min(len(latencies), len(inflights), len(weights))
    lb = StationLoadBalancer()
    replicas = []
    for i in range(n):
        r = StationReplica(
            base_url=f"http://127.0.0.1:{9500 + i}",
            station_id="hypo_station",
            latency_ema_ms=latencies[i],
            inflight_count=inflights[i],
            weight=weights[i],
            status="ONLINE",
        )
        lb.add_replica(r)
        replicas.append(r)

    selected = lb.select_replica("hypo_station")

    # Calculate expected minimal score
    scores = [(r.latency_ema_ms * (1 + r.inflight_count)) / r.weight for r in replicas]
    min_score = min(scores)
    selected_score = (selected.latency_ema_ms * (1 + selected.inflight_count)) / selected.weight

    assert pytest.approx(selected_score, rel=1e-5) == min_score


# ============================================================================
# 6. Remediation Regression Tests (Defects 1, 2, 3)
# ============================================================================

def test_v1_2_remediation_offline_recovery_to_online_on_success():
    """Verify that success=True restores OFFLINE and DEGRADED replicas to ONLINE."""
    lb = StationLoadBalancer()
    r_offline = StationReplica(
        base_url="http://127.0.0.1:9601",
        station_id="test_station",
        status="OFFLINE",
        consecutive_failures=5,
    )
    r_degraded = StationReplica(
        base_url="http://127.0.0.1:9602",
        station_id="test_station",
        status="DEGRADED",
        consecutive_failures=3,
    )
    lb.add_replica(r_offline)
    lb.add_replica(r_degraded)

    # Success on OFFLINE replica restores to ONLINE
    lb.record_telemetry("http://127.0.0.1:9601", duration_ms=15.0, success=True)
    assert r_offline.status == "ONLINE"
    assert r_offline.consecutive_failures == 0

    # Success on DEGRADED replica restores to ONLINE
    lb.record_telemetry("http://127.0.0.1:9602", duration_ms=15.0, success=True)
    assert r_degraded.status == "ONLINE"
    assert r_degraded.consecutive_failures == 0


def test_v1_2_remediation_failure_while_offline_never_promotes_to_degraded():
    """Verify that failures while OFFLINE never promote or transition a replica to DEGRADED."""
    lb = StationLoadBalancer()
    # Replica starting OFFLINE with 0 consecutive failures
    r = StationReplica(
        base_url="http://127.0.0.1:9603",
        station_id="test_station",
        status="OFFLINE",
        consecutive_failures=0,
    )
    lb.add_replica(r)

    # 1st failure: remains OFFLINE
    lb.record_telemetry(r.base_url, duration_ms=25.0, success=False)
    assert r.status == "OFFLINE"
    assert r.consecutive_failures == 1

    # 2nd failure: MUST remain OFFLINE (never promoted to DEGRADED)
    lb.record_telemetry(r.base_url, duration_ms=25.0, success=False)
    assert r.status == "OFFLINE"
    assert r.consecutive_failures == 2

    # 3rd and 4th failures: remain OFFLINE
    lb.record_telemetry(r.base_url, duration_ms=25.0, success=False)
    lb.record_telemetry(r.base_url, duration_ms=25.0, success=False)
    assert r.status == "OFFLINE"
    assert r.consecutive_failures == 4

    # 5th failure: remains OFFLINE
    lb.record_telemetry(r.base_url, duration_ms=25.0, success=False)
    assert r.status == "OFFLINE"
    assert r.consecutive_failures == 5


def test_v1_2_remediation_dag_exporters_handle_none_and_non_dict_results():
    """Verify export_dag_mermaid and export_dag_dot safely handle None, bool, and non-dict execution_results."""
    tasks = [
        WorkflowTask(task_id="step_none", station_id="st1", action="act1", payload={}),
        WorkflowTask(task_id="step_bool_t", station_id="st2", action="act2", payload={}),
        WorkflowTask(task_id="step_bool_f", station_id="st3", action="act3", payload={}),
        WorkflowTask(task_id="step_custom", station_id="st4", action="act4", payload={}),
    ]
    results = {
        "step_none": None,
        "step_bool_t": True,
        "step_bool_f": False,
        "step_custom": "SKIPPED",
    }

    # Mermaid export
    mermaid = export_dag_mermaid(tasks, execution_results=results)
    assert "step_none: st1/act1 [UNKNOWN]" in mermaid
    assert "step_bool_t: st2/act2 [COMPLETED]" in mermaid
    assert "step_bool_f: st3/act3 [FAILED]" in mermaid
    assert "step_custom: st4/act4 [SKIPPED]" in mermaid

    # DOT export
    dot = export_dag_dot(tasks, execution_results=results)
    assert "status: UNKNOWN" in dot
    assert "status: COMPLETED" in dot
    assert "status: FAILED" in dot
    assert "status: SKIPPED" in dot

    # FastAPI endpoint test client validation
    client = TestClient(app)
    resp_mermaid = client.post(
        "/api/federation/dag/export",
        json={
            "tasks": [{"task_id": "t1", "station_id": "st", "action": "act", "depends_on": []}],
            "format": "mermaid",
            "execution_results": {"t1": None},
        },
    )
    assert resp_mermaid.status_code == 200
    assert "[UNKNOWN]" in resp_mermaid.json()["content"]

    resp_dot = client.post(
        "/api/federation/dag/export",
        json={
            "tasks": [{"task_id": "t1", "station_id": "st", "action": "act", "depends_on": []}],
            "format": "dot",
            "execution_results": {"t1": None},
        },
    )
    assert resp_dot.status_code == 200
    assert "status: UNKNOWN" in resp_dot.json()["content"]


def test_v1_2_remediation_export_dag_dot_escapes_quotes_and_backslashes():
    """Verify export_dag_dot properly escapes double quotes and backslashes in node labels."""
    tasks = [
        WorkflowTask(
            task_id="task_quotes",
            station_id="station_test",
            action='deep "nested" scan',
            payload={},
        ),
    ]

    dot = export_dag_dot(tasks)
    matching_lines = [l.strip() for l in dot.splitlines() if "task_quotes" in l and "label=" in l]
    assert len(matching_lines) == 1
    label_line = matching_lines[0]

    # Double quotes inside label must be escaped as \"
    assert r'\"nested\"' in label_line
    # Emitted line must have balanced outer label="..." quotes
    assert label_line.endswith('"];')
