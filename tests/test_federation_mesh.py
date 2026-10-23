"""
ISO 9001:2015 / Agile V Quality Pipeline Test Suite
Baseline: BL-20261023-FEDERATION-L5 (L1 -> L2 -> L3 -> L4 -> L5)
Component: AGY Federation Mesh Test Matrix
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.l1_primitives import (
    STATION_CATALOG,
    StationNode,
    WorkflowTask,
    DAGPipelineEngine,
    compute_mesh_digest,
)
from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator
from agy_federation_mesh.l4_app import app
from agy_federation_mesh import cli


# ============================================================================
# Tier L1: Atomic Primitives & DAG Engine Tests (REQ-001)
# ============================================================================

def test_l1_station_catalog_metadata():
    """Verify STATION_CATALOG contains required stations and structured metadata."""
    assert isinstance(STATION_CATALOG, dict)
    required_stations = {
        "mission_control",
        "flow_studio",
        "pe_reversing",
        "secret_shield",
    }
    assert required_stations.issubset(set(STATION_CATALOG.keys()))
    for station_id in required_stations:
        meta = STATION_CATALOG[station_id]
        assert isinstance(meta, dict)
        assert "name" in meta and isinstance(meta["name"], str) and len(meta["name"]) > 0
        assert "default_port" in meta and isinstance(meta["default_port"], int)
        assert 1024 <= meta["default_port"] <= 65535
        assert "tier" in meta and isinstance(meta["tier"], str)
        assert "capabilities" in meta and isinstance(meta["capabilities"], list)
        assert len(meta["capabilities"]) >= 1
        assert all(isinstance(c, str) for c in meta["capabilities"])


def test_l1_station_node_model_and_serialization():
    """Verify StationNode initialization, fields, and to_dict serialization."""
    node = StationNode(
        station_id="pe_reversing",
        name="PE Reversing Lab",
        base_url="http://127.0.0.1:8003",
        capabilities=["pe_triage", "entropy_analysis"],
        tier="L4",
        status="ONLINE",
    )
    assert node.station_id == "pe_reversing"
    assert node.name == "PE Reversing Lab"
    assert node.base_url == "http://127.0.0.1:8003"
    assert node.capabilities == ["pe_triage", "entropy_analysis"]
    assert node.tier == "L4"
    assert node.status == "ONLINE"

    data = node.to_dict()
    assert isinstance(data, dict)
    assert data["station_id"] == "pe_reversing"
    assert data["name"] == "PE Reversing Lab"
    assert data["base_url"] == "http://127.0.0.1:8003"
    assert data["capabilities"] == ["pe_triage", "entropy_analysis"]
    assert data["tier"] == "L4"
    assert data["status"] == "ONLINE"

    # Verify defaults
    default_node = StationNode(
        station_id="mission_control",
        name="Mission Control Station",
        base_url="http://127.0.0.1:8000",
        capabilities=["orchestration"],
    )
    assert default_node.tier == "L4"
    assert default_node.status == "ONLINE"


def test_l1_workflow_task_model():
    """Verify WorkflowTask field values and default depends_on list."""
    task = WorkflowTask(
        task_id="task_audit",
        station_id="secret_shield",
        action="scan_entropy",
        payload={"target": "repo"},
        depends_on=["task_init"],
    )
    assert task.task_id == "task_audit"
    assert task.station_id == "secret_shield"
    assert task.action == "scan_entropy"
    assert task.payload == {"target": "repo"}
    assert task.depends_on == ["task_init"]

    default_task = WorkflowTask(
        task_id="task_default",
        station_id="flow_studio",
        action="render",
        payload={},
    )
    assert default_task.depends_on == [] or default_task.depends_on is None or len(default_task.depends_on) == 0


def test_l1_dag_engine_topological_sort_success():
    """Verify DAGPipelineEngine correctly produces linear topological execution order."""
    t1 = WorkflowTask(task_id="t1", station_id="secret_shield", action="scan", payload={})
    t2 = WorkflowTask(task_id="t2", station_id="pe_reversing", action="analyze", payload={}, depends_on=["t1"])
    t3 = WorkflowTask(task_id="t3", station_id="mission_control", action="seal", payload={}, depends_on=["t2"])

    order = DAGPipelineEngine.validate_dag([t3, t1, t2])
    assert order == ["t1", "t2", "t3"]


def test_l1_dag_engine_branching_execution_order():
    """Verify DAGPipelineEngine handles diamond and branching task dependencies."""
    t_init = WorkflowTask(task_id="t_init", station_id="mission_control", action="init", payload={})
    t_sec = WorkflowTask(task_id="t_sec", station_id="secret_shield", action="sec", payload={}, depends_on=["t_init"])
    t_pe = WorkflowTask(task_id="t_pe", station_id="pe_reversing", action="pe", payload={}, depends_on=["t_init"])
    t_join = WorkflowTask(task_id="t_join", station_id="flow_studio", action="join", payload={}, depends_on=["t_sec", "t_pe"])

    order = DAGPipelineEngine.validate_dag([t_join, t_sec, t_pe, t_init])
    assert len(order) == 4
    assert order[0] == "t_init"
    assert order[-1] == "t_join"
    assert order.index("t_sec") < order.index("t_join")
    assert order.index("t_pe") < order.index("t_join")


def test_l1_dag_engine_cycle_detection():
    """Verify DAGPipelineEngine detects direct and indirect dependency cycles."""
    # Direct cycle
    t1 = WorkflowTask(task_id="t1", station_id="s1", action="a1", payload={}, depends_on=["t2"])
    t2 = WorkflowTask(task_id="t2", station_id="s2", action="a2", payload={}, depends_on=["t1"])
    with pytest.raises(ValueError):
        DAGPipelineEngine.validate_dag([t1, t2])

    # 3-node cycle
    t_a = WorkflowTask(task_id="ta", station_id="s1", action="a", payload={}, depends_on=["tc"])
    t_b = WorkflowTask(task_id="tb", station_id="s2", action="b", payload={}, depends_on=["ta"])
    t_c = WorkflowTask(task_id="tc", station_id="s3", action="c", payload={}, depends_on=["tb"])
    with pytest.raises(ValueError):
        DAGPipelineEngine.validate_dag([t_a, t_b, t_c])


def test_l1_dag_engine_unknown_dependency_detection():
    """Verify DAGPipelineEngine raises ValueError for missing dependency IDs."""
    t1 = WorkflowTask(task_id="t1", station_id="s1", action="a1", payload={}, depends_on=["ghost_task"])
    with pytest.raises(ValueError):
        DAGPipelineEngine.validate_dag([t1])


def test_l1_compute_mesh_digest():
    """Verify compute_mesh_digest generates deterministic 64-char hex SHA-256 digest."""
    n1 = StationNode("s1", "Station 1", "http://127.0.0.1:8001", ["cap1"])
    n2 = StationNode("s2", "Station 2", "http://127.0.0.1:8002", ["cap2"])

    digest1 = compute_mesh_digest([n1, n2])
    digest2 = compute_mesh_digest([n2, n1])

    assert isinstance(digest1, str)
    assert len(digest1) == 64
    assert digest1 == digest2
    assert all(c in "0123456789abcdefABCDEF" for c in digest1)


# ============================================================================
# Tier L2: Native Plugin Bundle Tests (REQ-002)
# ============================================================================

def test_l2_plugin_json_schema_and_allowed_keys():
    """Verify plugin.json follows strict allowed keys and valid identity."""
    plugin_path = REPO_ROOT / "plugins" / "agy-federation-mesh" / "plugin.json"
    assert plugin_path.exists(), f"Missing plugin.json at {plugin_path}"

    with open(plugin_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    allowed_keys = {
        "name",
        "displayName",
        "description",
        "version",
        "logo",
        "suggestedPrompts",
    }
    assert set(data.keys()).issubset(allowed_keys)
    assert data.get("name") == "agy-federation-mesh"
    assert isinstance(data.get("version"), str) and len(data["version"]) > 0


def test_l2_hooks_consolidation_into_golden_stack():
    """Verify hooks.json declares consolidated execution into agy-golden-stack."""
    hooks_path = REPO_ROOT / "plugins" / "agy-federation-mesh" / "hooks.json"
    assert hooks_path.exists(), f"Missing hooks.json at {hooks_path}"

    with open(hooks_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_text = json.dumps(data)
    assert "agy-golden-stack" in raw_text


def test_l2_mcp_config_schema_and_length_shield():
    """Verify mcp_config.json server prefixes and < 64 chars tool identifier limit."""
    mcp_path = REPO_ROOT / "plugins" / "agy-federation-mesh" / "mcp_config.json"
    assert mcp_path.exists(), f"Missing mcp_config.json at {mcp_path}"

    with open(mcp_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    mcp_servers = data.get("mcpServers", data)
    assert isinstance(mcp_servers, dict)
    assert len(mcp_servers) >= 1

    for srv_name, srv_def in mcp_servers.items():
        assert srv_name.startswith("agy-federation-mesh"), f"Server key {srv_name} must start with 'agy-federation-mesh'"
        assert len(srv_name) < 64, f"Server key {srv_name} exceeds 64 characters"
        if isinstance(srv_def, dict) and "tools" in srv_def:
            for tool in srv_def["tools"]:
                tool_name = tool if isinstance(tool, str) else tool.get("name", "")
                assert len(tool_name) < 64, f"Tool name {tool_name} exceeds 64 characters"


def test_l2_plugin_bundle_file_structure():
    """Verify required bundle assets, markdown rules without frontmatter, and skill paths."""
    plugin_root = REPO_ROOT / "plugins" / "agy-federation-mesh"

    logo_path = plugin_root / "assets" / "logo.svg"
    assert logo_path.exists(), f"Missing logo asset at {logo_path}"
    assert "<svg" in logo_path.read_text(encoding="utf-8")

    agents_rule = plugin_root / "rules" / "AGENTS.md"
    assert agents_rule.exists(), f"Missing AGENTS.md rule at {agents_rule}"
    rule_content = agents_rule.read_text(encoding="utf-8").strip()
    assert not rule_content.startswith("---"), "AGENTS.md must NOT start with YAML frontmatter (---)"

    skill_path = plugin_root / "skills" / "federation-mesh" / "SKILL.md"
    assert skill_path.exists(), f"Missing SKILL.md at {skill_path}"

    agent_path = plugin_root / "agents" / "agy-federation-agent.md"
    assert agent_path.exists(), f"Missing agent manifest at {agent_path}"


# ============================================================================
# Tier L3: Federated Multi-Station Orchestrator Tests (REQ-003)
# ============================================================================

def test_l3_orchestrator_initialization_and_discovery():
    """Verify FederationMeshOrchestrator discovers default catalog station nodes."""
    orchestrator = FederationMeshOrchestrator()
    discovery = orchestrator.discover_nodes()

    assert isinstance(discovery, dict)
    assert "node_count" in discovery and discovery["node_count"] >= 4
    assert "online_count" in discovery and discovery["online_count"] >= 0
    assert "nodes" in discovery and isinstance(discovery["nodes"], list)
    assert "mesh_digest" in discovery and len(discovery["mesh_digest"]) == 64


def test_l3_orchestrator_execute_composite_security_reversing_workflow():
    """Verify full end-to-end composite cross-station workflow execution."""
    t1 = WorkflowTask(
        task_id="step_audit",
        station_id="secret_shield",
        action="audit_secrets",
        payload={"scan_scope": "repo"},
    )
    t2 = WorkflowTask(
        task_id="step_triage",
        station_id="pe_reversing",
        action="triage_binary",
        payload={"sample": "target.exe"},
        depends_on=["step_audit"],
    )
    t3 = WorkflowTask(
        task_id="step_seal",
        station_id="mission_control",
        action="seal_evidence",
        payload={"level": "L5"},
        depends_on=["step_triage"],
    )

    orchestrator = FederationMeshOrchestrator()
    res = orchestrator.execute_cross_station_workflow("security_reversing_audit", [t1, t2, t3])

    assert res.get("ok") is True
    assert res.get("status") == "COMPLETED"
    assert res.get("workflow_name") == "security_reversing_audit"
    assert res.get("tasks_executed") == 3
    assert isinstance(res.get("results"), dict)
    assert isinstance(res.get("prov_trace"), list)
    assert len(res.get("prov_trace")) == 3
    assert len(res.get("mesh_digest", "")) == 64


def test_l3_orchestrator_failure_halting():
    """Verify task failure halts downstream execution and marks status FAILED."""
    t1 = WorkflowTask(
        task_id="failing_task",
        station_id="secret_shield",
        action="raise_error_action",
        payload={"force_failure": True},
    )
    t2 = WorkflowTask(
        task_id="downstream_task",
        station_id="mission_control",
        action="should_not_run",
        payload={},
        depends_on=["failing_task"],
    )

    orchestrator = FederationMeshOrchestrator()
    res = orchestrator.execute_cross_station_workflow("failing_flow", [t1, t2])

    assert res.get("ok") is False
    assert res.get("status") == "FAILED"
    assert res.get("tasks_executed", 0) < 2
    assert "error" in res or any("error" in str(v).lower() for v in res.get("results", {}).values())


def test_l3_orchestrator_validation_guards():
    """Verify orchestrator rejects empty task sets and cyclic graphs."""
    orchestrator = FederationMeshOrchestrator()

    with pytest.raises(ValueError):
        orchestrator.execute_cross_station_workflow("empty_flow", [])

    t1 = WorkflowTask(task_id="loop_a", station_id="mission_control", action="a", payload={}, depends_on=["loop_b"])
    t2 = WorkflowTask(task_id="loop_b", station_id="flow_studio", action="b", payload={}, depends_on=["loop_a"])

    with pytest.raises(ValueError):
        orchestrator.execute_cross_station_workflow("cyclic_flow", [t1, t2])


# ============================================================================
# Tier L4/L5: Web UI Cockpit & FastAPI Application Tests (REQ-004)
# ============================================================================

def test_l4_fastapi_cockpit_html_and_test_ids():
    """Verify GET / returns HTML with all required Cockpit data-testid elements."""
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")

    html_content = response.text
    required_test_ids = [
        'data-testid="federation-l5-cockpit"',
        'data-testid="mesh-topology-view"',
        'data-testid="station-node-card"',
        'data-testid="execute-workflow-btn"',
        'data-testid="mesh-prov-trace"',
    ]
    for test_id in required_test_ids:
        assert test_id in html_content, f"Missing {test_id} in GET / HTML output"


def test_l4_fastapi_api_topology():
    """Verify GET /api/federation/topology returns full mesh metadata."""
    client = TestClient(app)
    response = client.get("/api/federation/topology")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, dict)
    assert data.get("node_count", 0) >= 4
    assert isinstance(data.get("nodes"), list)
    assert len(data.get("nodes")) >= 4
    assert len(data.get("mesh_digest", "")) == 64
    assert "capabilities" in data or all("capabilities" in n for n in data["nodes"])


def test_l4_fastapi_workflow_run_endpoint_success_and_400():
    """Verify POST /api/federation/workflow/run executes valid flows and rejects unknown flows."""
    client = TestClient(app)

    valid_payload = {
        "workflow_name": "security_reversing_audit",
        "payload": {"target": "sample.bin"},
    }
    resp_ok = client.post("/api/federation/workflow/run", json=valid_payload)
    assert resp_ok.status_code == 200
    data_ok = resp_ok.json()
    assert data_ok.get("ok") is True
    assert data_ok.get("status") == "COMPLETED"

    bad_payload = {
        "workflow_name": "unknown_workflow_999",
        "payload": {},
    }
    resp_bad = client.post("/api/federation/workflow/run", json=bad_payload)
    assert resp_bad.status_code == 400


def test_l4_fastapi_report_to_root_endpoint(tmp_path):
    """Verify POST /api/federation/report-to-root writes jsonl and markdown reports."""
    client = TestClient(app)
    payload = {
        "target_dir": str(tmp_path),
        "summary": "Multi-station integration completed under ISO 9001",
        "status": "COMPLETED",
        "metrics": {"nodes_online": 4, "tasks_executed": 3},
    }
    resp = client.post("/api/federation/report-to-root", json=payload)
    assert resp.status_code == 200

    jsonl_target = tmp_path / "child_session_reports.jsonl"
    report_target = tmp_path / "FEDERATION_MESH_HANDOFF_REPORT.md"

    assert jsonl_target.exists(), f"Missing {jsonl_target}"
    assert report_target.exists(), f"Missing {report_target}"

    jsonl_text = jsonl_target.read_text(encoding="utf-8")
    assert "child_session_reports" in jsonl_text or "COMPLETED" in jsonl_text or "summary" in jsonl_text

    report_text = report_target.read_text(encoding="utf-8")
    assert "FEDERATION" in report_text or "COMPLETED" in report_text


# ============================================================================
# Tier L5: CLI Interface & 5-Gate OSS Hygiene Tests (REQ-005)
# ============================================================================

def test_l5_cli_topology_command(capsys):
    """Verify CLI topology command outputs valid JSON summary."""
    ret = cli.main(["topology", "--json"])
    assert ret == 0
    captured = capsys.readouterr()
    assert "node_count" in captured.out or "nodes" in captured.out


def test_l5_cli_run_workflow_command(capsys):
    """Verify CLI run command executes composite workflow successfully."""
    ret = cli.main(["run", "--workflow", "security_reversing_audit", "--json"])
    assert ret == 0
    captured = capsys.readouterr()
    assert "COMPLETED" in captured.out or "results" in captured.out


def test_l5_cli_invalid_command():
    """Verify CLI returns non-zero error status for invalid arguments."""
    ret = cli.main(["unknown_subcommand_xyz"])
    assert ret != 0


def test_l5_oss_repository_hygiene_and_anti_leak():
    """Verify 5-gate OSS repository hygiene, MIT license, and zero leak policy."""
    # 1. License check
    license_path = REPO_ROOT / "LICENSE"
    assert license_path.exists(), f"Missing LICENSE at {license_path}"
    assert "MIT License" in license_path.read_text(encoding="utf-8")

    # 2. Skill check in .agents
    agents_skill = REPO_ROOT / ".agents" / "skills" / "federation-mesh" / "SKILL.md"
    assert agents_skill.exists(), f"Missing .agents skill at {agents_skill}"

    # 3. Dynamic forbidden needles construction (zero literal forbidden strings in this test file)
    needle_ora = "".join([chr(79), chr(114), chr(97)])
    needle_kul = "".join([chr(107), chr(117), chr(108)])
    needle_user = needle_ora + needle_kul

    needle_grey = "".join([chr(71), chr(114), chr(101), chr(121)])
    needle_full_name = f"{needle_user} {needle_grey}"

    needle_anti = "".join([chr(97), chr(110), chr(116), chr(105)])
    needle_grav = "".join([chr(103), chr(114), chr(97), chr(118), chr(105), chr(116), chr(121)])
    needle_framework = needle_anti + needle_grav

    needle_kuro = "kuro" + needle_user.lower() + needle_grey.lower()

    cyrillic_pattern = re.compile("[" + chr(0x0400) + "-" + chr(0x04FF) + "]")

    # Scan directories
    scan_dirs = [
        REPO_ROOT / "src",
        REPO_ROOT / "tests",
        REPO_ROOT / "plugins",
        REPO_ROOT / ".agents",
    ]

    for scan_dir in scan_dirs:
        if not scan_dir.exists():
            continue
        for file_path in scan_dir.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() in {".pyc", ".png", ".ico", ".jpg", ".bin", ".exe", ".pyd"}:
                continue
            if ".git" in file_path.parts or "__pycache__" in file_path.parts:
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue

            # Verify zero Cyrillic characters across repository
            cyrillic_matches = cyrillic_pattern.findall(content)
            assert not cyrillic_matches, f"Cyrillic characters detected in {file_path}"

            # Verify zero personal identifiers
            assert needle_full_name not in content, f"Forbidden full name found in {file_path}"
            assert needle_kuro not in content, f"Forbidden handle found in {file_path}"

            # In implementation and config files, verify zero framework or personal user tokens
            if "tests" not in file_path.parts:
                assert needle_user.lower() not in content.lower(), f"Forbidden name token found in {file_path}"
                assert needle_framework.lower() not in content.lower(), f"Forbidden framework token found in {file_path}"
