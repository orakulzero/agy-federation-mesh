"""AGY Federation Mesh — Tier L5 Cross-Station Swarm & DAG Mesh."""

from .l1_primitives import (
    DAGPipelineEngine,
    STATION_CATALOG,
    StationNode,
    WorkflowTask,
    compute_mesh_digest,
)
from .l3_pipeline import FederationMeshOrchestrator

__all__ = [
    "DAGPipelineEngine",
    "FederationMeshOrchestrator",
    "STATION_CATALOG",
    "StationNode",
    "WorkflowTask",
    "compute_mesh_digest",
]

__version__ = "1.0.0"
