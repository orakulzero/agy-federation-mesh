"""CLI Entrypoint for AGY Federation Mesh (Tier L5)."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from .l1_primitives import (
    WorkflowTask,
)
from .l3_pipeline import FederationMeshOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agy-federation-mesh",
        description="AGY Federation Mesh — Cross-Station Orchestrator & Swarm Matrix CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    topo_cmd = sub.add_parser("topology", help="Enumerate active mesh topology")
    topo_cmd.add_argument("--json", action="store_true", help="Emit JSON output")

    run_cmd = sub.add_parser("run", help="Execute a cross-station workflow")
    run_cmd.add_argument("--workflow", required=True, help="Workflow name")
    run_cmd.add_argument("--json", action="store_true", help="Emit JSON output")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1

    orchestrator = FederationMeshOrchestrator()

    if args.command == "topology":
        res = orchestrator.discover_nodes()
        if args.json:
            print(json.dumps(res, indent=2))
        else:
            print(f"[AGY-FEDERATION] Nodes: {res['node_count']} | Digest: {res['mesh_digest'][:16]}")
        return 0

    if args.command == "run":
        if args.workflow == "security_reversing_audit":
            t1 = WorkflowTask("step_audit", "secret_shield", "audit_secrets", {})
            t2 = WorkflowTask("step_triage", "pe_reversing", "triage_binary", {}, depends_on=["step_audit"])
            t3 = WorkflowTask("step_seal", "mission_control", "seal_evidence", {}, depends_on=["step_triage"])
            res = orchestrator.execute_cross_station_workflow(args.workflow, [t1, t2, t3])
            if args.json:
                print(json.dumps(res, indent=2))
            else:
                print(f"[AGY-FEDERATION] Workflow: {res['workflow_name']} -> {res['status']}")
            return 0 if res["ok"] else 1
        print(f"Unknown workflow: {args.workflow}", file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
