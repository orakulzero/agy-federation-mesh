"""
ISO 9001:2015 / Agile V Verification Suite for AGY Federation Mesh v1.3.0 Features.

Covers:
1. SwarmWorkerProfile schema, property aliases, and serialization/deserialization.
2. Multi-objective Pareto frontier worker computation:
   - Non-dominated Pareto frontier extraction across Latency, Token Cost, and Reliability
   - In-flight load penalty: L_eff = L_p50 * (1.0 + 0.2 * inflight)
   - Strict constraint filtering: capability, station_id, min_reliability, max_cost, max_latency, concurrency limit
3. Scalarized multi-objective utility loss worker selection:
   - Presets: MIN_COST, MIN_LATENCY, HIGH_RELIABILITY, BALANCED
   - Dynamic weights (weight_latency, weight_cost, weight_reliability)
4. Critical-path speculative dual-execution hedging engine:
   - Non-critical execution path direct dispatch
   - Speculative fallback execution on delayed primary worker
   - Fast primary completion avoiding speculative hedging
"""

from __future__ import annotations

from pathlib import Path
import sys
import time
from typing import Any, Dict, List

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from agy_federation_mesh.engine import (
        SwarmWorkerProfile,
        SpeculativeSwarmRouter,
        compute_pareto_optimal_workers,
    )
except ImportError:
    try:
        from agy_federation_mesh import (
            SwarmWorkerProfile,
            SpeculativeSwarmRouter,
            compute_pareto_optimal_workers,
        )
    except ImportError:
        SwarmWorkerProfile = None  # type: ignore
        SpeculativeSwarmRouter = None  # type: ignore
        compute_pareto_optimal_workers = None  # type: ignore


def _require_engine() -> None:
    if SwarmWorkerProfile is None or SpeculativeSwarmRouter is None:
        pytest.skip("agy_federation_mesh.engine module is not yet implemented (pending implementer phase)")


# ============================================================================
# Test 1: SwarmWorkerProfile Schema, Aliases & Serialization
# ============================================================================

def test_v1_3_swarm_worker_profile_properties_and_aliases() -> None:
    _require_engine()

    profile = SwarmWorkerProfile(
        id="worker-rev-01",
        name="PE Reversing Worker",
        capabilities=["pe_cfg", "imphash", "rich_header"],
        latency_p50_ms=25.0,
        latency_p95_ms=90.0,
        cost_per_1k_tokens=0.003,
        reliability_score=0.98,
        concurrency_limit=8,
        current_inflight=2,
        station_id="pe-reversing-lab",
    )

    # Aliases
    assert profile.worker_id == "worker-rev-01"
    assert profile.token_cost_per_1k == 0.003
    assert profile.inflight_count == 2

    # Serialization
    data = profile.to_dict()
    assert isinstance(data, dict)
    assert data["id"] == "worker-rev-01"
    assert data["worker_id"] == "worker-rev-01"
    assert data["cost_per_1k_tokens"] == 0.003
    assert data["token_cost_per_1k"] == 0.003

    # Deserialization from dictionary
    reconstructed = SwarmWorkerProfile.from_dict(data)
    assert reconstructed.id == profile.id
    assert reconstructed.latency_p50_ms == profile.latency_p50_ms
    assert reconstructed.cost_per_1k_tokens == profile.cost_per_1k_tokens
    assert reconstructed.reliability_score == profile.reliability_score


# ============================================================================
# Test 2: Strict Capability and Station ID Filtering
# ============================================================================

def test_v1_3_pareto_strict_capability_and_station_filtering() -> None:
    _require_engine()

    w1 = SwarmWorkerProfile(id="w1", capabilities=["pe_cfg"], station_id="pe_station")
    w2 = SwarmWorkerProfile(id="w2", capabilities=["vector_bom"], station_id="eskd_station")
    w3 = SwarmWorkerProfile(id="w3", capabilities=["vector_bom"], station_id="pe_station")

    router = SpeculativeSwarmRouter(workers=[w1, w2, w3])

    # Filter by required_capability
    frontier_bom = router.compute_pareto_frontier(task_requirements={"required_capability": "vector_bom"})
    ids_bom = [w.id for w in frontier_bom]
    assert "w1" not in ids_bom
    assert "w2" in ids_bom
    assert "w3" in ids_bom

    # Filter by station_id
    frontier_station = router.compute_pareto_frontier(task_requirements={"station_id": "pe_station"})
    ids_station = [w.id for w in frontier_station]
    assert "w1" in ids_station
    assert "w2" not in ids_station
    assert "w3" in ids_station


# ============================================================================
# Test 3: Strict Quantitative SLA Constraints (Latency, Cost, Reliability)
# ============================================================================

def test_v1_3_pareto_strict_constraints_filtering() -> None:
    _require_engine()

    w_fast = SwarmWorkerProfile(id="w_fast", latency_p95_ms=30.0, cost_per_1k_tokens=0.005, reliability_score=0.99)
    w_cheap = SwarmWorkerProfile(id="w_cheap", latency_p95_ms=120.0, cost_per_1k_tokens=0.001, reliability_score=0.95)
    w_unreliable = SwarmWorkerProfile(id="w_unrel", latency_p95_ms=40.0, cost_per_1k_tokens=0.002, reliability_score=0.85)

    router = SpeculativeSwarmRouter(workers=[w_fast, w_cheap, w_unreliable])

    # Filter with min_reliability=0.90
    f_rel = router.compute_pareto_frontier(task_requirements={"min_reliability": 0.90})
    ids_rel = [w.id for w in f_rel]
    assert "w_unrel" not in ids_rel
    assert "w_fast" in ids_rel
    assert "w_cheap" in ids_rel

    # Filter with max_latency_ms=50.0
    f_lat = router.compute_pareto_frontier(task_requirements={"max_latency_ms": 50.0})
    ids_lat = [w.id for w in f_lat]
    assert "w_cheap" not in ids_lat
    assert "w_fast" in ids_lat

    # Filter with max_cost=0.003
    f_cost = router.compute_pareto_frontier(task_requirements={"max_cost": 0.003})
    ids_cost = [w.id for w in f_cost]
    assert "w_fast" not in ids_cost
    assert "w_cheap" in ids_cost


# ============================================================================
# Test 4: Concurrency Limit Enforcement
# ============================================================================

def test_v1_3_pareto_concurrency_limit_enforcement() -> None:
    _require_engine()

    w_busy = SwarmWorkerProfile(id="w_busy", concurrency_limit=4, current_inflight=4)
    w_free = SwarmWorkerProfile(id="w_free", concurrency_limit=4, current_inflight=1)

    router = SpeculativeSwarmRouter(workers=[w_busy, w_free])

    # When enforce_concurrency is True, saturated worker is excluded
    frontier = router.compute_pareto_frontier(task_requirements={"enforce_concurrency": True})
    ids = [w.id for w in frontier]
    assert "w_busy" not in ids
    assert "w_free" in ids


# ============================================================================
# Test 5: Multi-Objective Pareto Dominance Elimination
# ============================================================================

def test_v1_3_pareto_dominance_elimination() -> None:
    _require_engine()

    # Worker A: fast, cheap, reliable
    w_a = SwarmWorkerProfile(id="w_a", latency_p50_ms=20.0, cost_per_1k_tokens=0.002, reliability_score=0.99)
    # Worker B: strictly dominated by A in latency, cost, and reliability
    w_b = SwarmWorkerProfile(id="w_b", latency_p50_ms=40.0, cost_per_1k_tokens=0.004, reliability_score=0.95)
    # Worker C: faster than A, but more expensive (non-dominated trade-off)
    w_c = SwarmWorkerProfile(id="w_c", latency_p50_ms=10.0, cost_per_1k_tokens=0.006, reliability_score=0.98)

    router = SpeculativeSwarmRouter(workers=[w_a, w_b, w_c])

    frontier = router.compute_pareto_frontier()
    ids = [w.id for w in frontier]

    assert "w_a" in ids
    assert "w_c" in ids
    assert "w_b" not in ids  # Dominated candidate must be pruned


# ============================================================================
# Test 6: In-Flight Load Penalty on Effective Latency
# ============================================================================

def test_v1_3_pareto_inflight_load_penalty() -> None:
    _require_engine()

    # Identical baseline p50 latency, but w_loaded has 5 in-flight tasks
    # L_eff(w_idle) = 20.0
    # L_eff(w_loaded) = 20.0 * (1.0 + 0.2 * 5) = 40.0
    w_idle = SwarmWorkerProfile(id="w_idle", latency_p50_ms=20.0, cost_per_1k_tokens=0.002, reliability_score=0.99, current_inflight=0)
    w_loaded = SwarmWorkerProfile(id="w_loaded", latency_p50_ms=20.0, cost_per_1k_tokens=0.002, reliability_score=0.99, current_inflight=5)

    router = SpeculativeSwarmRouter(workers=[w_idle, w_loaded])

    frontier = router.compute_pareto_frontier()
    ids = [w.id for w in frontier]

    # Because w_idle has identical cost/reliability but lower effective latency, it dominates w_loaded
    assert "w_idle" in ids
    assert "w_loaded" not in ids


# ============================================================================
# Test 7: Selection Policy MIN_COST
# ============================================================================

def test_v1_3_select_pareto_worker_min_cost_policy() -> None:
    _require_engine()

    w_fast = SwarmWorkerProfile(id="w_fast", latency_p50_ms=10.0, cost_per_1k_tokens=0.008, reliability_score=0.99)
    w_mid = SwarmWorkerProfile(id="w_mid", latency_p50_ms=25.0, cost_per_1k_tokens=0.003, reliability_score=0.98)
    w_cheap = SwarmWorkerProfile(id="w_cheap", latency_p50_ms=50.0, cost_per_1k_tokens=0.001, reliability_score=0.97)

    router = SpeculativeSwarmRouter(workers=[w_fast, w_mid, w_cheap])

    selected_id = router.select_pareto_worker({"policy": "MIN_COST"})
    assert selected_id == "w_cheap"

    selected_cost_focused = router.select_pareto_worker({"cost_focused": True})
    assert selected_cost_focused == "w_cheap"


# ============================================================================
# Test 8: Selection Policy MIN_LATENCY
# ============================================================================

def test_v1_3_select_pareto_worker_min_latency_policy() -> None:
    _require_engine()

    w_fast = SwarmWorkerProfile(id="w_fast", latency_p50_ms=8.0, cost_per_1k_tokens=0.010, reliability_score=0.99)
    w_mid = SwarmWorkerProfile(id="w_mid", latency_p50_ms=30.0, cost_per_1k_tokens=0.002, reliability_score=0.98)

    router = SpeculativeSwarmRouter(workers=[w_fast, w_mid])

    selected_id = router.select_pareto_worker({"policy": "MIN_LATENCY"})
    assert selected_id == "w_fast"

    selected_lat_focused = router.select_pareto_worker({"latency_focused": True})
    assert selected_lat_focused == "w_fast"


# ============================================================================
# Test 9: Selection Policy HIGH_RELIABILITY
# ============================================================================

def test_v1_3_select_pareto_worker_high_reliability_policy() -> None:
    _require_engine()

    w1 = SwarmWorkerProfile(id="w1", latency_p50_ms=15.0, cost_per_1k_tokens=0.002, reliability_score=0.94)
    w2 = SwarmWorkerProfile(id="w2", latency_p50_ms=30.0, cost_per_1k_tokens=0.005, reliability_score=0.999)

    router = SpeculativeSwarmRouter(workers=[w1, w2])

    selected_id = router.select_pareto_worker({"policy": "HIGH_RELIABILITY"})
    assert selected_id == "w2"


# ============================================================================
# Test 10: Balanced Multi-Objective Utility Loss Selection
# ============================================================================

def test_v1_3_select_pareto_worker_balanced_utility() -> None:
    _require_engine()

    w1 = SwarmWorkerProfile(id="w1", latency_p50_ms=10.0, cost_per_1k_tokens=0.010, reliability_score=0.98)
    w2 = SwarmWorkerProfile(id="w2", latency_p50_ms=20.0, cost_per_1k_tokens=0.002, reliability_score=0.99)
    w3 = SwarmWorkerProfile(id="w3", latency_p50_ms=80.0, cost_per_1k_tokens=0.001, reliability_score=0.96)

    router = SpeculativeSwarmRouter(workers=[w1, w2, w3])

    # Balanced weights favoring moderate latency and cost
    selected_id = router.select_pareto_worker({
        "policy": "BALANCED",
        "weight_latency": 0.5,
        "weight_cost": 0.3,
        "weight_reliability": 0.2,
    })
    assert isinstance(selected_id, str)
    assert selected_id in ("w1", "w2", "w3")


# ============================================================================
# Test 11: Non-Critical Path Hedging Bypass
# ============================================================================

def test_v1_3_execute_with_hedging_non_critical_path() -> None:
    _require_engine()

    router = SpeculativeSwarmRouter()

    def sample_task() -> Dict[str, Any]:
        return {"output": "ok", "tokens": 150, "cost": 0.0003}

    result = router.execute_with_hedging(
        task_id="task-nc-01",
        primary_fn=sample_task,
        critical_path=False,
    )

    assert isinstance(result, dict)
    assert result["task_id"] == "task-nc-01"
    assert result["success"] is True
    assert result["hedged"] is False
    assert result["winning_channel"] == "primary"
    assert result["result"] == {"output": "ok", "tokens": 150, "cost": 0.0003}


# ============================================================================
# Test 12: Critical-Path Speculative Hedging Activation & Fast Primary Win
# ============================================================================

def test_v1_3_execute_with_hedging_speculative_channels() -> None:
    _require_engine()

    router = SpeculativeSwarmRouter(hedging_delay_ms=20.0)

    # 1. Delayed primary triggers speculative hedging
    def slow_primary() -> Dict[str, str]:
        time.sleep(0.15)
        return {"channel": "primary"}

    def fast_fallback() -> Dict[str, str]:
        return {"channel": "fallback"}

    res_hedged = router.execute_with_hedging(
        task_id="task-crit-01",
        primary_fn=slow_primary,
        fallback_fn=fast_fallback,
        hedging_delay_ms=20.0,
        critical_path=True,
    )

    assert isinstance(res_hedged, dict)
    assert res_hedged["success"] is True
    assert res_hedged["hedged"] is True
    assert res_hedged["winning_channel"] == "fallback"
    assert res_hedged["result"] == {"channel": "fallback"}

    # 2. Fast primary completes before hedging delay expires
    def fast_primary() -> Dict[str, str]:
        return {"channel": "primary"}

    res_fast = router.execute_with_hedging(
        task_id="task-crit-02",
        primary_fn=fast_primary,
        fallback_fn=fast_fallback,
        hedging_delay_ms=50.0,
        critical_path=True,
    )

    assert res_fast["success"] is True
    assert res_fast["hedged"] is False
    assert res_fast["winning_channel"] == "primary"
    assert res_fast["result"] == {"channel": "primary"}
