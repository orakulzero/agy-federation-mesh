"""Tier L1 Atomic Primitives & DAG Execution Engine for AGY Federation Mesh.

Provides station catalog taxonomy, station node models, workflow task definitions,
directed acyclic graph (DAG) cycle validation and topological resolution,
and deterministic topology digest hashing.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Any, Dict, List, Optional

STATION_CATALOG: dict[str, dict[str, Any]] = {
    "mission_control": {
        "name": "AGY Mission Control Station",
        "default_port": 8000,
        "tier": "L4",
        "capabilities": ["overview", "plugin_toggle", "golden_bus", "brain_memory", "seal_evidence"],
    },
    "flow_studio": {
        "name": "Flow Video Studio",
        "default_port": 8765,
        "tier": "L4",
        "capabilities": ["storyboard", "voiceover", "video_assembly", "batch_render"],
    },
    "pe_reversing": {
        "name": "PE Reversing Lab",
        "default_port": 8768,
        "tier": "L4",
        "capabilities": ["binary_parse", "entropy_scan", "iat_triage", "yara_synth", "pe_triage"],
    },
    "secret_shield": {
        "name": "AGY Secret Shield",
        "default_port": 8770,
        "tier": "L4",
        "capabilities": ["secret_scan", "dpapi_seal", "entropy_analysis", "clean_cert", "audit_secrets"],
    },
}


@dataclass
class StationNode:
    """Represents an active or registered Tier L4 Station Node in the Mesh."""

    station_id: str
    name: str
    base_url: str
    capabilities: list[str]
    tier: str = "L4"
    status: str = "ONLINE"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkflowTask:
    """Discrete executable task belonging to a federated cross-station workflow."""

    task_id: str
    station_id: str
    action: str
    payload: dict[str, Any]
    depends_on: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DAGPipelineEngine:
    """Directed Acyclic Graph (DAG) validator and topological execution resolver."""

    @staticmethod
    def validate_dag(tasks: list[WorkflowTask]) -> list[str]:
        """Validate task graph dependencies and return topological execution sequence.

        Raises:
            ValueError: If a task references an unknown dependency ID.
            ValueError: If a cyclic dependency (deadlock loop) is detected.
        """
        task_map: dict[str, WorkflowTask] = {t.task_id: t for t in tasks}

        for t in tasks:
            for dep in t.depends_on:
                if dep not in task_map:
                    raise ValueError(f"Task '{t.task_id}' references unknown dependency: '{dep}'")

        in_degree: dict[str, int] = {t.task_id: 0 for t in tasks}
        adjacency: dict[str, list[str]] = {t.task_id: [] for t in tasks}

        for t in tasks:
            for dep in t.depends_on:
                adjacency[dep].append(t.task_id)
                in_degree[t.task_id] += 1

        queue: list[str] = [t_id for t_id, deg in in_degree.items() if deg == 0]
        topological_order: list[str] = []

        while queue:
            queue.sort()
            curr = queue.pop(0)
            topological_order.append(curr)

            for neighbor in adjacency[curr]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(topological_order) != len(tasks):
            raise ValueError("Cyclic dependency detected in task graph")

        return topological_order

    @staticmethod
    def compute_execution_waves(tasks: list[WorkflowTask]) -> list[list[str]]:
        """Partition tasks into independent topological execution waves.

        Tasks within each wave have all dependencies satisfied by preceding waves
        and can be executed concurrently in parallel worker pools.

        Raises:
            ValueError: If a task references an unknown dependency ID.
            ValueError: If a cyclic dependency is detected in the graph.
        """
        task_map: dict[str, WorkflowTask] = {t.task_id: t for t in tasks}

        for t in tasks:
            for dep in t.depends_on:
                if dep not in task_map:
                    raise ValueError(f"Task '{t.task_id}' references unknown dependency: '{dep}'")

        in_degree: dict[str, int] = {t.task_id: 0 for t in tasks}
        adjacency: dict[str, list[str]] = {t.task_id: [] for t in tasks}

        for t in tasks:
            for dep in t.depends_on:
                adjacency[dep].append(t.task_id)
                in_degree[t.task_id] += 1

        current_wave: list[str] = [t_id for t_id, deg in in_degree.items() if deg == 0]
        waves: list[list[str]] = []
        visited_count = 0

        while current_wave:
            current_wave.sort()
            waves.append(list(current_wave))
            visited_count += len(current_wave)
            next_wave: list[str] = []

            for task_id in current_wave:
                for neighbor in adjacency[task_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_wave.append(neighbor)

            current_wave = next_wave

        if visited_count != len(tasks):
            raise ValueError("Cyclic dependency detected in task graph")

        return waves


def compute_mesh_digest(nodes: list[StationNode]) -> str:
    """Compute a deterministic 64-char SHA-256 fingerprint for active mesh topology."""
    sorted_nodes = sorted(nodes, key=lambda n: n.station_id)
    canonical_repr = [
        {
            "station_id": n.station_id,
            "name": n.name,
            "base_url": n.base_url,
            "capabilities": sorted(n.capabilities),
            "tier": n.tier,
            "status": n.status,
        }
        for n in sorted_nodes
    ]
    raw_json = json.dumps(canonical_repr, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw_json).hexdigest()


@dataclass
class StationReplica:
    """Represents a specific replica endpoint for an L4 Station Node."""

    base_url: str
    station_id: str
    latency_ema_ms: float = 10.0
    inflight_count: int = 0
    weight: float = 1.0
    status: str = "ONLINE"  # "ONLINE" | "DEGRADED" | "OFFLINE"
    capabilities: list[str] = field(default_factory=list)
    consecutive_failures: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StationLoadBalancer:
    """Adaptive load balancer with latency EMA, inflight tracking, and health failover."""

    def __init__(self, replicas: Optional[list[StationReplica]] = None) -> None:
        self.replicas: list[StationReplica] = list(replicas) if replicas else []

    def add_replica(self, replica: StationReplica) -> None:
        """Register a new station replica or update existing replica by base_url."""
        for idx, existing in enumerate(self.replicas):
            if existing.base_url == replica.base_url:
                self.replicas[idx] = replica
                return
        self.replicas.append(replica)

    def get_replicas(self, station_id: Optional[str] = None) -> list[StationReplica]:
        """Return all replicas, optionally filtered by station_id."""
        if station_id is None:
            return list(self.replicas)
        return [r for r in self.replicas if r.station_id == station_id]

    def select_replica(
        self, station_id: str, required_capability: Optional[str] = None
    ) -> StationReplica:
        """Select optimal replica minimizing (latency_ema_ms * (1 + inflight_count)) / weight.

        Filters by required_capability if specified.
        Prefers ONLINE replicas, falling back gracefully to DEGRADED replicas if no ONLINE exist.
        Excludes OFFLINE replicas.

        Raises:
            ValueError: If no replicas are configured or all matching replicas are OFFLINE.
        """
        matching = [r for r in self.replicas if r.station_id == station_id]
        if not matching:
            raise ValueError(f"No replicas registered for station '{station_id}'")

        if required_capability is not None:
            matching = [r for r in matching if required_capability in r.capabilities]
            if not matching:
                raise ValueError(
                    f"No replicas for station '{station_id}' support capability '{required_capability}'"
                )

        eligible = [r for r in matching if r.status != "OFFLINE"]
        if not eligible:
            raise ValueError(f"All replicas for station '{station_id}' are OFFLINE")

        online_candidates = [r for r in eligible if r.status == "ONLINE"]
        candidates = online_candidates if online_candidates else [r for r in eligible if r.status == "DEGRADED"]

        def _cost(r: StationReplica) -> tuple[float, str]:
            effective_weight = r.weight if r.weight > 0.0 else 1.0
            score = (r.latency_ema_ms * (1 + max(0, r.inflight_count))) / effective_weight
            return (score, r.base_url)

        return min(candidates, key=_cost)

    def record_telemetry(
        self, base_url: str, duration_ms: float, success: bool, alpha: float = 0.3
    ) -> None:
        """Update EMA latency and health status for the replica matching base_url.

        latency_ema_ms = alpha * duration_ms + (1 - alpha) * latency_ema_ms
        On success: reset consecutive failures; if DEGRADED, restore to ONLINE.
        On failure: increment consecutive failures; transition to DEGRADED (>=2) or OFFLINE (>=5).
        """
        target: Optional[StationReplica] = None
        for r in self.replicas:
            if r.base_url == base_url:
                target = r
                break
        if target is None:
            return

        clamped_alpha = max(0.01, min(1.0, alpha))
        target.latency_ema_ms = clamped_alpha * float(duration_ms) + (1.0 - clamped_alpha) * target.latency_ema_ms

        if success:
            target.consecutive_failures = 0
            if target.status in ("DEGRADED", "OFFLINE"):
                target.status = "ONLINE"
        else:
            target.consecutive_failures += 1
            if target.consecutive_failures >= 5:
                target.status = "OFFLINE"
            elif target.consecutive_failures >= 2:
                if target.status != "OFFLINE":
                    target.status = "DEGRADED"


def export_dag_mermaid(
    tasks: list[WorkflowTask],
    execution_results: Optional[dict[str, Any]] = None,
) -> str:
    """Generate valid Mermaid flowchart TD grouping tasks into wave subgraphs.

    Emits directed dependency edges `dep --> task_id` and execution status annotations.
    """
    if not tasks:
        return "flowchart TD\n"

    waves = DAGPipelineEngine.compute_execution_waves(tasks)
    task_map = {t.task_id: t for t in tasks}

    lines: list[str] = ["flowchart TD"]

    for w_idx, wave in enumerate(waves):
        lines.append(f"  subgraph Wave_{w_idx} [\"Wave {w_idx}\"]")
        for tid in wave:
            t = task_map[tid]
            if execution_results and tid in execution_results:
                res = execution_results[tid]
                if isinstance(res, dict):
                    status = "COMPLETED" if res.get("ok", True) else "FAILED"
                elif isinstance(res, bool):
                    status = "COMPLETED" if res else "FAILED"
                elif res is None:
                    status = "UNKNOWN"
                else:
                    status = str(res)
                node_label = f"{t.task_id}: {t.station_id}/{t.action} [{status}]"
            else:
                node_label = f"{t.task_id}: {t.station_id}/{t.action}"
            lines.append(f"    {tid}[\"{node_label}\"]")
        lines.append("  end")

    edge_pairs: list[tuple[str, str]] = []
    for t in tasks:
        for dep in t.depends_on:
            edge_pairs.append((dep, t.task_id))

    edge_pairs.sort()
    for dep, tid in edge_pairs:
        lines.append(f"  {dep} --> {tid}")

    return "\n".join(lines) + "\n"


def export_dag_dot(
    tasks: list[WorkflowTask],
    execution_results: Optional[dict[str, Any]] = None,
) -> str:
    """Generate valid Graphviz DOT digraph grouping tasks into wave clusters.

    Emits directed dependency edges `"{dep}" -> "{task_id}";` and execution status annotations.
    """
    lines: list[str] = [
        "digraph FederationDAG {",
        "  rankdir=TB;",
        "  node [shape=box, fontname=\"Helvetica\"];",
    ]

    if not tasks:
        lines.append("}")
        return "\n".join(lines) + "\n"

    waves = DAGPipelineEngine.compute_execution_waves(tasks)
    task_map = {t.task_id: t for t in tasks}

    for w_idx, wave in enumerate(waves):
        lines.append(f"  subgraph cluster_wave_{w_idx} {{")
        lines.append(f"    label=\"Wave {w_idx}\";")
        for tid in wave:
            t = task_map[tid]
            if execution_results and tid in execution_results:
                res = execution_results[tid]
                if isinstance(res, dict):
                    status = "COMPLETED" if res.get("ok", True) else "FAILED"
                elif isinstance(res, bool):
                    status = "COMPLETED" if res else "FAILED"
                elif res is None:
                    status = "UNKNOWN"
                else:
                    status = str(res)
                node_label = f"{t.task_id}\\nstation: {t.station_id}\\naction: {t.action}\\nstatus: {status}"
            else:
                node_label = f"{t.task_id}\\nstation: {t.station_id}\\naction: {t.action}"
            safe_label = node_label.replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f"    \"{tid}\" [label=\"{safe_label}\"];")
        lines.append("  }")

    edge_pairs: list[tuple[str, str]] = []
    for t in tasks:
        for dep in t.depends_on:
            edge_pairs.append((dep, t.task_id))

    edge_pairs.sort()
    for dep, tid in edge_pairs:
        lines.append(f"  \"{dep}\" -> \"{tid}\";")

    lines.append("}")
    return "\n".join(lines) + "\n"

