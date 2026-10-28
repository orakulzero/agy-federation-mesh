import sys
from pathlib import Path
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.l1_primitives import (
    compute_mesh_digest, 
    StationNode, 
    WorkflowTask, 
    DAGPipelineEngine, 
    StationReplica, 
    StationLoadBalancer,
    export_dag_mermaid,
    export_dag_dot
)

def test_compute_mesh_digest_empty():
    nodes = []
    digest = compute_mesh_digest(nodes)
    assert len(digest) == 64
    assert isinstance(digest, str)
    
    # Check deterministic value for empty list
    import hashlib, json
    expected = hashlib.sha256(json.dumps([]).encode("utf-8")).hexdigest()
    assert digest == expected

def test_dag_pipeline_engine_validate_dag_empty():
    assert DAGPipelineEngine.validate_dag([]) == []

def test_dag_pipeline_engine_validate_dag_unknown_dependency():
    t1 = WorkflowTask(task_id="t1", station_id="s1", action="a1", payload={}, depends_on=["t2"])
    with pytest.raises(ValueError, match="unknown dependency"):
        DAGPipelineEngine.validate_dag([t1])

def test_dag_pipeline_engine_validate_dag_cycle():
    t1 = WorkflowTask(task_id="t1", station_id="s1", action="a1", payload={}, depends_on=["t1"])
    with pytest.raises(ValueError, match="Cyclic dependency detected"):
        DAGPipelineEngine.validate_dag([t1])
        
def test_station_load_balancer_empty():
    lb = StationLoadBalancer()
    assert lb.get_replicas() == []
    with pytest.raises(ValueError, match="No replicas registered"):
        lb.select_replica("nonexistent")

def test_export_dag_mermaid_empty():
    assert "flowchart TD" in export_dag_mermaid([])
    
def test_export_dag_dot_empty():
    assert "digraph FederationDAG {" in export_dag_dot([])
