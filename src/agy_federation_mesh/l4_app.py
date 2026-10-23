"""Tier L4/L5 Federation Command Matrix Web UI & FastAPI Application.

Surfaces real-time station topology, visual DAG pipeline status, cross-station
workflow execution, and automated child session telemetry reporting.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .l1_primitives import (
    STATION_CATALOG,
    StationNode,
    WorkflowTask,
    compute_mesh_digest,
)
from .l3_pipeline import FederationMeshOrchestrator

app = FastAPI(
    title="AGY Federation Mesh Command Matrix",
    version="1.0.0",
    description="Tier L5 Federated Cross-Station Mesh Controller & Cockpit",
)

_orchestrator = FederationMeshOrchestrator()


class WorkflowRunRequest(BaseModel):
    workflow_name: str
    payload: Optional[dict[str, Any]] = None


class ReportToRootRequest(BaseModel):
    target_dir: str
    summary: str
    status: str = "COMPLETED"
    metrics: Optional[dict[str, Any]] = None


COCKPIT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>AGY Federation Mesh — L5 Command Matrix</title>
  <style>
    body { background: #080c16; color: #f1f5f9; font-family: system-ui, sans-serif; margin: 0; padding: 24px; }
    .cockpit { max-width: 1100px; margin: 0 auto; background: #0f172a; border: 1px solid #1e293b; border-radius: 14px; padding: 24px; }
    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 16px; margin-bottom: 20px; }
    .badge { background: #0369a1; color: #e0f2fe; padding: 4px 12px; border-radius: 9999px; font-size: 13px; font-weight: 600; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; }
    .card h3 { margin: 0 0 8px 0; font-size: 16px; color: #38bdf8; }
    .actions { display: flex; gap: 12px; margin-bottom: 20px; }
    button { background: #2563eb; color: #fff; border: 0; padding: 10px 18px; border-radius: 8px; font-weight: 600; cursor: pointer; }
    button:hover { background: #1d4ed8; }
    pre { background: #020617; border: 1px solid #1e293b; border-radius: 8px; padding: 14px; overflow-x: auto; font-size: 13px; }
  </style>
</head>
<body>
  <div class="cockpit" data-testid="federation-l5-cockpit">
    <div class="header">
      <div>
        <h2 style="margin:0 0 6px 0;">AGY Federation Mesh — Tier L5 Cockpit</h2>
        <span style="color:#94a3b8; font-size:14px;">Cross-Station Swarm Orchestrator &amp; Distributed DAG Mesh</span>
      </div>
      <span class="badge">Tier L5 Federation Active</span>
    </div>

    <div class="grid" data-testid="mesh-topology-view">
      <div class="card" data-testid="station-node-card">
        <h3>Mission Control (L4)</h3>
        <p style="font-size:13px; color:#cbd5e1; margin:0;">Port 8000 &bull; Status: ONLINE</p>
      </div>
      <div class="card" data-testid="station-node-card">
        <h3>Flow Video Studio (L4)</h3>
        <p style="font-size:13px; color:#cbd5e1; margin:0;">Port 8765 &bull; Status: ONLINE</p>
      </div>
      <div class="card" data-testid="station-node-card">
        <h3>PE Reversing Lab (L4)</h3>
        <p style="font-size:13px; color:#cbd5e1; margin:0;">Port 8768 &bull; Status: ONLINE</p>
      </div>
      <div class="card" data-testid="station-node-card">
        <h3>Secret Shield (L4)</h3>
        <p style="font-size:13px; color:#cbd5e1; margin:0;">Port 8770 &bull; Status: ONLINE</p>
      </div>
    </div>

    <div class="actions">
      <button data-testid="execute-workflow-btn" onclick="runWorkflow()">Run Security &amp; Reversing Pipeline</button>
    </div>

    <h4 style="margin:0 0 10px 0;">Provenance &amp; Workflow Trace</h4>
    <pre id="traceBox" data-testid="mesh-prov-trace">Ready for federated cross-station execution...</pre>
  </div>

  <script>
    async function runWorkflow() {
      const box = document.getElementById('traceBox');
      box.textContent = 'Executing cross-station workflow...';
      const res = await fetch('/api/federation/workflow/run', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({workflow_name: 'security_reversing_audit', payload: {target: 'sample.exe'}})
      });
      const data = await res.json();
      box.textContent = JSON.stringify(data, null, 2);
    }
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def get_cockpit_view() -> HTMLResponse:
    return HTMLResponse(content=COCKPIT_HTML, status_code=200)


@app.get("/api/federation/topology")
def get_mesh_topology() -> dict[str, Any]:
    discovery = _orchestrator.discover_nodes()
    all_caps: list[str] = []
    for n in discovery["nodes"]:
        for cap in n.get("capabilities", []):
            if cap not in all_caps:
                all_caps.append(cap)

    return {
        "ok": True,
        "node_count": discovery["node_count"],
        "online_count": discovery["online_count"],
        "nodes": discovery["nodes"],
        "mesh_digest": discovery["mesh_digest"],
        "capabilities": all_caps,
    }


@app.post("/api/federation/workflow/run")
def post_workflow_run(req: WorkflowRunRequest) -> dict[str, Any]:
    if req.workflow_name == "security_reversing_audit":
        t1 = WorkflowTask(
            task_id="step_audit",
            station_id="secret_shield",
            action="audit_secrets",
            payload=req.payload or {"scope": "input"},
        )
        t2 = WorkflowTask(
            task_id="step_triage",
            station_id="pe_reversing",
            action="triage_binary",
            payload=req.payload or {"sample": "sample.exe"},
            depends_on=["step_audit"],
        )
        t3 = WorkflowTask(
            task_id="step_seal",
            station_id="mission_control",
            action="seal_evidence",
            payload={"level": "L5"},
            depends_on=["step_triage"],
        )
        return _orchestrator.execute_cross_station_workflow(
            workflow_name=req.workflow_name,
            tasks=[t1, t2, t3],
            initial_context=req.payload,
        )
    raise HTTPException(status_code=400, detail=f"Unsupported workflow: {req.workflow_name}")


@app.post("/api/federation/report-to-root")
def post_report_to_root(req: ReportToRootRequest) -> dict[str, Any]:
    target_path = Path(req.target_dir).resolve()
    target_path.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).isoformat()
    record = {
        "timestamp": ts,
        "project": "agy-federation-mesh",
        "tier": "L5",
        "status": req.status,
        "summary": req.summary,
        "metrics": req.metrics or {},
    }

    jsonl_path = target_path / "child_session_reports.jsonl"
    with jsonl_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    report_path = target_path / "FEDERATION_MESH_HANDOFF_REPORT.md"
    content = (
        "# AGY Federation Mesh — Tier L5 Telemetry Handoff Report\n\n"
        f"- **Timestamp**: `{ts}`\n"
        f"- **Status**: `{req.status}`\n"
        f"- **Summary**: {req.summary}\n"
        f"- **Metrics**: `{json.dumps(req.metrics or {})}`\n"
    )
    report_path.write_text(content, encoding="utf-8")

    return {
        "ok": True,
        "jsonl_path": str(jsonl_path),
        "report_path": str(report_path),
        "record": record,
    }
