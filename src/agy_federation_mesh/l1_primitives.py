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
