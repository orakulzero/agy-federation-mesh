import sys
from pathlib import Path
import time
import pytest
import concurrent.futures

# Add src to sys.path so we can import agy_federation_mesh
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from agy_federation_mesh.engine import SwarmWorkerProfile, SpeculativeSwarmRouter


def test_swarm_worker_profile_extreme_values_serialization():
    """Test serialization/deserialization of a profile with extreme values."""
    profile = SwarmWorkerProfile(
        id="extreme_worker",
        name="Extreme Worker",
        latency_p50_ms=-50.0,         # Negative latency
        latency_p95_ms=-10.0,
        latency_ema_ms=-30.0,
        reliability_score=0.0,        # Zero reliability
        concurrency_limit=1000000,    # Very high limit
        current_inflight=999999,      # Very high inflight count
        cost_per_1k_tokens=0.0,       # Zero cost
    )
    
    # Serialize
    data = profile.to_dict()
    assert data["id"] == "extreme_worker"
    assert data["latency_p50_ms"] == -50.0
    assert data["latency_p95_ms"] == -10.0
    assert data["reliability_score"] == 0.0
    assert data["current_inflight"] == 999999
    
    # Deserialize
    recreated = SwarmWorkerProfile.from_dict(data)
    assert recreated.id == "extreme_worker"
    assert recreated.latency_p50_ms == -50.0
    assert recreated.latency_p95_ms == -10.0
    assert recreated.reliability_score == 0.0
    assert recreated.current_inflight == 999999
    assert recreated.concurrency_limit == 1000000


def test_worker_aliases():
    """Test property aliases for worker profile."""
    profile = SwarmWorkerProfile.from_dict({
        "worker_id": "alias_id", 
        "token_cost_per_1k": 0.05, 
        "inflight_count": 5
    })
    
    assert profile.id == "alias_id"
    assert profile.worker_id == "alias_id"
    assert profile.cost_per_1k_tokens == 0.05
    assert profile.token_cost_per_1k == 0.05
    assert profile.current_inflight == 5
    assert profile.inflight_count == 5
    
    # Test setters
    profile.token_cost_per_1k = 0.1
    assert profile.cost_per_1k_tokens == 0.1
    profile.inflight_count = 10
    assert profile.current_inflight == 10


def test_speculative_swarm_router_extreme_values():
    """Test router logic with extreme profile values and constraints."""
    router = SpeculativeSwarmRouter()
    
    normal_worker = SwarmWorkerProfile(
        id="normal_worker",
        latency_p50_ms=20.0,
        cost_per_1k_tokens=0.002,
        reliability_score=0.99,
        concurrency_limit=10,
        current_inflight=0
    )
    
    extreme_worker = SwarmWorkerProfile(
        id="extreme_worker",
        latency_p50_ms=-10.0,
        cost_per_1k_tokens=0.0,
        reliability_score=0.0,
        concurrency_limit=1000,
        current_inflight=10000  # Inflight exceeds concurrency limit
    )
    
    router.register_worker(normal_worker)
    router.register_worker(extreme_worker)
    
    # 1. Enforce concurrency should exclude the extreme worker
    frontier_cap = router.compute_pareto_frontier(task_requirements={"enforce_concurrency": True})
    assert len(frontier_cap) == 1
    assert frontier_cap[0].id == "normal_worker"
    
    # 2. Min reliability should exclude the extreme worker
    frontier_rel = router.compute_pareto_frontier(task_requirements={"min_reliability": 0.5})
    assert len(frontier_rel) == 1
    assert frontier_rel[0].id == "normal_worker"
    
    # 3. High reliability policy should definitely pick normal_worker
    best = router.select_pareto_worker(task_requirements={"policy": "HIGH_RELIABILITY"})
    assert best == "normal_worker"


def test_router_select_pareto_worker_no_valid_workers():
    """Test error handling when no workers satisfy constraints."""
    router = SpeculativeSwarmRouter()
    router.register_worker(SwarmWorkerProfile(id="w1", reliability_score=0.1))
    
    with pytest.raises(ValueError, match="No eligible worker satisfies strict constraints"):
        router.select_pareto_worker(task_requirements={"min_reliability": 0.9})


def test_execute_with_hedging():
    """Test the speculative hedging execution logic."""
    router = SpeculativeSwarmRouter()
    
    def fast_task():
        return {"tokens": 100, "cost": 0.01}
        
    def slow_task():
        time.sleep(0.2)
        return {"tokens": 200, "cost": 0.02}
        
    def error_task():
        raise RuntimeError("Task failed")

    # 1. Non-critical path
    res1 = router.execute_with_hedging("t1", primary_fn=fast_task, critical_path=False)
    assert res1["success"] is True
    assert res1["hedged"] is False
    assert res1["winning_channel"] == "primary"
    
    # 2. Critical path, primary finishes fast
    res2 = router.execute_with_hedging("t2", primary_fn=fast_task, critical_path=True, hedging_delay_ms=100)
    assert res2["success"] is True
    assert res2["hedged"] is False
    assert res2["winning_channel"] == "primary"
    
    # 3. Critical path, primary is slow, fallback is fast
    res3 = router.execute_with_hedging(
        "t3", 
        primary_fn=slow_task, 
        fallback_fn=fast_task, 
        critical_path=True, 
        hedging_delay_ms=50
    )
    assert res3["success"] is True
    assert res3["hedged"] is True
    assert res3["winning_channel"] == "fallback"

    # 4. Critical path, both error out (primary fails immediately)
    # If critical path is false, it fails and returns error
    res4 = router.execute_with_hedging("t4", primary_fn=error_task, critical_path=False)
    assert res4["success"] is False
    assert "Task failed" in res4["error"]

    # 5. execute_with_hedging without primary_fn or task_fn
    with pytest.raises(ValueError, match="execute_with_hedging requires primary_fn or task_fn"):
        router.execute_with_hedging("t5")
