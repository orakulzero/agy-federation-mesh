"""AGY Federation Mesh — Tier L5 Cross-Station Swarm & DAG Mesh."""

from .l1_primitives import (
    DAGPipelineEngine,
    STATION_CATALOG,
    StationLoadBalancer,
    StationNode,
    StationReplica,
    WorkflowTask,
    compute_mesh_digest,
    export_dag_dot,
    export_dag_mermaid,
)
from .l3_pipeline import FederationMeshOrchestrator, execute_workflow_with_retry
from .engine import (
    SwarmWorkerProfile,
    SpeculativeSwarmRouter,
    compute_pareto_optimal_workers,
    ByzantineQuorumVerifier,
    WorkStealingDAGScheduler,
)

__all__ = [
    "DAGPipelineEngine",
    "FederationMeshOrchestrator",
    "STATION_CATALOG",
    "StationLoadBalancer",
    "StationNode",
    "StationReplica",
    "WorkflowTask",
    "compute_mesh_digest",
    "execute_workflow_with_retry",
    "export_dag_dot",
    "export_dag_mermaid",
    "SwarmWorkerProfile",
    "SpeculativeSwarmRouter",
    "compute_pareto_optimal_workers",
    "ByzantineQuorumVerifier",
    "WorkStealingDAGScheduler",
]

__version__ = "2.0.0"
