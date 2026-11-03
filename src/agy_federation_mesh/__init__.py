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
from .verification_loop import (
    build_three_wave_verification_dag,
    execute_autonomous_verification_loop,
)
from .webrtc_swarm_federation import (
    FederatedPeerNode,
    SubSwarmSession,
    WebRTCSwarmFederation,
)
from .thread_subscription_fanout import (
    ReviewThreadEvent,
    ThreadSubscriptionHub,
    TicketRecord,
)
from .multi_repo_dependency_repair import (
    BreakingSignatureChange,
    MultiRepoDependencyRepair,
)
from .wasm_sandbox_executor import (
    WasmEnvelopeValidation,
    WasmExecutionResult,
    WasmSandboxConfig,
    WasmSandboxExecutor,
)
from .line_span_lock_oracle import LineSpanLockOracle

__all__ = [
    "DAGPipelineEngine",
    "FederatedPeerNode",
    "FederationMeshOrchestrator",
    "LineSpanLockOracle",
    "ReviewThreadEvent",
    "STATION_CATALOG",
    "StationLoadBalancer",
    "StationNode",
    "StationReplica",
    "SubSwarmSession",
    "ThreadSubscriptionHub",
    "TicketRecord",
    "WasmEnvelopeValidation",
    "WasmExecutionResult",
    "WasmSandboxConfig",
    "WasmSandboxExecutor",
    "WebRTCSwarmFederation",
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
    "build_three_wave_verification_dag",
    "execute_autonomous_verification_loop",
]

__version__ = "2.0.0"
