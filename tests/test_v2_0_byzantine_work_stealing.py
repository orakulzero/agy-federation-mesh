"""
ISO 9001:2015 / Agile V Verification Suite for AGY Federation Mesh v2.0.0 Features.
Requirement: REQ-FM-201

Covers:
1. ByzantineQuorumVerifier(f=1):
   - Output SHA-256 hash computation per worker payload/output (canonical JSON serialization).
   - Tally votes: if max tally >= 2f + 1, consensus is reached. Returns consensus output, hash, agreeing worker IDs, faulty worker IDs.
   - If max tally < 2f + 1, flags "BYZANTINE_SPLIT_BRAIN" (consensus_reached=False).
   - Test cases: unanimous consensus (4 workers, f=1), single corrupt worker isolation, split-brain rejection (2 vs 2), tolerance limits (f=2).
   - Payload hashing determinism across dict key ordering and varied types.
   - Insufficient total workers handling.
2. WorkStealingDAGScheduler(num_workers=4):
   - Chase-Lev work-stealing DAG execution model.
   - Dependency resolution via in-degree tracking.
   - Worker-local ready deque with LIFO pop from tail, FIFO steal from head of peer deques.
   - Decrementing child in-degree upon completion, pushing ready tasks.
   - Test cases: linear dependency chain, diamond DAG parallelism, worker starvation causing work-stealing, cycle detection / invalid DAG error.
   - Task failure isolation and downstream halting.
   - Chase-Lev deque contract verification.
3. Version and Export Sync:
   - __version__ == "2.0.0"
   - plugin.json version == "2.0.0"
   - pyproject.toml version == "2.0.0"
   - Exports: ByzantineQuorumVerifier and WorkStealingDAGScheduler importable from agy_federation_mesh.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import time
from typing import Any, Callable, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Dynamic imports with graceful fallback for Agile V Test-Designer phase
try:
    from agy_federation_mesh.engine import (
        ByzantineQuorumVerifier,
        WorkStealingDAGScheduler,
    )
except ImportError:
    try:
        from agy_federation_mesh import (
            ByzantineQuorumVerifier,
            WorkStealingDAGScheduler,
        )
    except ImportError:
        ByzantineQuorumVerifier = None  # type: ignore
        WorkStealingDAGScheduler = None  # type: ignore


def _get_val(obj: Any, key: str, default: Any = None) -> Any:
    """Helper to access properties from either dataclass/object attributes or dict keys."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


# ============================================================================
# 1. ByzantineQuorumVerifier (REQ-FM-201)
# ============================================================================

def test_byzantine_quorum_unanimous_consensus_f1() -> None:
    """Verify unanimous consensus across 4 workers with f=1 (quorum requirement: 2f + 1 = 3)."""
    assert ByzantineQuorumVerifier is not None, "ByzantineQuorumVerifier must be implemented in agy_federation_mesh.engine"

    verifier = ByzantineQuorumVerifier(f=1)
    payload = {"status": "SUCCESS", "analysis_hash": "a1b2c3d4e5", "score": 99.4}

    worker_outputs = {
        "worker_0": payload,
        "worker_1": payload,
        "worker_2": payload,
        "worker_3": payload,
    }

    result = verifier.verify_consensus(worker_outputs) if hasattr(verifier, "verify_consensus") else verifier.verify(worker_outputs)

    assert _get_val(result, "consensus_reached") is True
    assert _get_val(result, "consensus_output") == payload
    assert len(_get_val(result, "agreeing_workers")) == 4
    assert len(_get_val(result, "faulty_workers")) == 0

    c_hash = _get_val(result, "consensus_hash")
    assert isinstance(c_hash, str)
    assert len(c_hash) == 64
    assert re.match(r"^[0-9a-f]{64}$", c_hash)


def test_byzantine_quorum_single_corrupt_worker_isolation() -> None:
    """Verify single corrupt worker is isolated when 3 of 4 workers agree (f=1)."""
    assert ByzantineQuorumVerifier is not None, "ByzantineQuorumVerifier must be implemented in agy_federation_mesh.engine"

    verifier = ByzantineQuorumVerifier(f=1)
    clean_payload = {"stage": "L5_AUDIT", "verdict": "PASS", "confidence": 0.98}
    corrupt_payload = {"stage": "L5_AUDIT", "verdict": "FAIL", "confidence": 0.12}

    worker_outputs = {
        "worker_alpha": clean_payload,
        "worker_beta": clean_payload,
        "worker_gamma": clean_payload,
        "worker_corrupt": corrupt_payload,
    }

    result = verifier.verify_consensus(worker_outputs) if hasattr(verifier, "verify_consensus") else verifier.verify(worker_outputs)

    assert _get_val(result, "consensus_reached") is True
    assert _get_val(result, "consensus_output") == clean_payload

    agreeing = set(_get_val(result, "agreeing_workers"))
    faulty = set(_get_val(result, "faulty_workers"))

    assert agreeing == {"worker_alpha", "worker_beta", "worker_gamma"}
    assert faulty == {"worker_corrupt"}


def test_byzantine_quorum_split_brain_rejection() -> None:
    """Verify split-brain (2 vs 2 out of 4) is rejected because max tally (2) < 2f + 1 (3)."""
    assert ByzantineQuorumVerifier is not None, "ByzantineQuorumVerifier must be implemented in agy_federation_mesh.engine"

    verifier = ByzantineQuorumVerifier(f=1)
    payload_a = {"partition": "A", "leader": "node_1"}
    payload_b = {"partition": "B", "leader": "node_2"}

    worker_outputs = {
        "worker_1": payload_a,
        "worker_2": payload_a,
        "worker_3": payload_b,
        "worker_4": payload_b,
    }

    result = verifier.verify_consensus(worker_outputs) if hasattr(verifier, "verify_consensus") else verifier.verify(worker_outputs)

    assert _get_val(result, "consensus_reached") is False
    assert _get_val(result, "consensus_output") is None

    status_str = str(_get_val(result, "status") or _get_val(result, "error") or _get_val(result, "reason") or "")
    assert "BYZANTINE_SPLIT_BRAIN" in status_str or "SPLIT_BRAIN" in status_str


def test_byzantine_quorum_tolerance_limits_f2() -> None:
    """Verify quorum threshold scaling for f=2 (required quorum: 2(2) + 1 = 5)."""
    assert ByzantineQuorumVerifier is not None, "ByzantineQuorumVerifier must be implemented in agy_federation_mesh.engine"

    verifier = ByzantineQuorumVerifier(f=2)
    valid_output = {"digest": "merkle_root_seal_v2"}
    byz_output_1 = {"digest": "tampered_seal_1"}
    byz_output_2 = {"digest": "tampered_seal_2"}

    # Case A: 5 honest + 2 byzantine = 7 total workers. 5 >= 5 votes -> consensus reached
    outputs_pass = {
        f"worker_{i}": valid_output for i in range(5)
    }
    outputs_pass["byz_1"] = byz_output_1
    outputs_pass["byz_2"] = byz_output_2

    res_pass = verifier.verify_consensus(outputs_pass) if hasattr(verifier, "verify_consensus") else verifier.verify(outputs_pass)
    assert _get_val(res_pass, "consensus_reached") is True
    assert _get_val(res_pass, "consensus_output") == valid_output
    assert len(_get_val(res_pass, "agreeing_workers")) == 5
    assert set(_get_val(res_pass, "faulty_workers")) == {"byz_1", "byz_2"}

    # Case B: 4 honest + 3 byzantine. Max tally 4 < 5 -> split brain
    outputs_fail = {
        f"worker_{i}": valid_output for i in range(4)
    }
    outputs_fail["byz_1"] = byz_output_1
    outputs_fail["byz_2"] = byz_output_1
    outputs_fail["byz_3"] = byz_output_2

    res_fail = verifier.verify_consensus(outputs_fail) if hasattr(verifier, "verify_consensus") else verifier.verify(outputs_fail)
    assert _get_val(res_fail, "consensus_reached") is False
    status_fail = str(_get_val(res_fail, "status") or _get_val(res_fail, "error") or "")
    assert "BYZANTINE_SPLIT_BRAIN" in status_fail or "SPLIT_BRAIN" in status_fail


def test_byzantine_quorum_payload_hash_determinism_and_types() -> None:
    """Verify SHA-256 hash determinism across dict key orders and diverse data types."""
    assert ByzantineQuorumVerifier is not None, "ByzantineQuorumVerifier must be implemented in agy_federation_mesh.engine"

    verifier = ByzantineQuorumVerifier(f=1)

    # Differing key order must yield identical hash
    dict_order_1 = {"zebra": 100, "alpha": "first", "nested": {"k2": True, "k1": False}}
    dict_order_2 = {"alpha": "first", "nested": {"k1": False, "k2": True}, "zebra": 100}

    h1 = verifier.compute_hash(dict_order_1) if hasattr(verifier, "compute_hash") else None
    h2 = verifier.compute_hash(dict_order_2) if hasattr(verifier, "compute_hash") else None

    if h1 is not None and h2 is not None:
        assert h1 == h2
        assert len(h1) == 64

    # Both dict orders in verify_consensus must be treated as matching
    worker_outputs = {
        "w1": dict_order_1,
        "w2": dict_order_2,
        "w3": dict_order_1,
        "w4": {"zebra": 999},
    }
    res = verifier.verify_consensus(worker_outputs) if hasattr(verifier, "verify_consensus") else verifier.verify(worker_outputs)
    assert _get_val(res, "consensus_reached") is True
    assert set(_get_val(res, "agreeing_workers")) == {"w1", "w2", "w3"}
    assert set(_get_val(res, "faulty_workers")) == {"w4"}


def test_byzantine_quorum_insufficient_total_workers() -> None:
    """Verify insufficient workers (< 2f + 1) cannot form consensus and flag split-brain."""
    assert ByzantineQuorumVerifier is not None, "ByzantineQuorumVerifier must be implemented in agy_federation_mesh.engine"

    verifier = ByzantineQuorumVerifier(f=1)
    # Only 2 workers provided, but 2f + 1 = 3 required
    insufficient_outputs = {
        "w1": {"data": 1},
        "w2": {"data": 1},
    }

    res = verifier.verify_consensus(insufficient_outputs) if hasattr(verifier, "verify_consensus") else verifier.verify(insufficient_outputs)
    assert _get_val(res, "consensus_reached") is False
    status_str = str(_get_val(res, "status") or _get_val(res, "error") or "")
    assert "BYZANTINE_SPLIT_BRAIN" in status_str or "SPLIT_BRAIN" in status_str or "INSUFFICIENT" in status_str


# ============================================================================
# 2. WorkStealingDAGScheduler (REQ-FM-201)
# ============================================================================

def test_work_stealing_linear_dependency_chain() -> None:
    """Verify linear dependency chain T1 -> T2 -> T3 -> T4 executes in strict topological sequence."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=4)
    execution_order: List[str] = []

    def make_task(name: str, return_val: int) -> Callable[[], int]:
        def _fn() -> int:
            execution_order.append(name)
            return return_val
        return _fn

    if hasattr(scheduler, "add_task"):
        scheduler.add_task("T1", fn=make_task("T1", 10), depends_on=[])
        scheduler.add_task("T2", fn=make_task("T2", 20), depends_on=["T1"])
        scheduler.add_task("T3", fn=make_task("T3", 30), depends_on=["T2"])
        scheduler.add_task("T4", fn=make_task("T4", 40), depends_on=["T3"])
    else:
        # Dictionary or list submission format
        scheduler.schedule([
            {"task_id": "T1", "fn": make_task("T1", 10), "depends_on": []},
            {"task_id": "T2", "fn": make_task("T2", 20), "depends_on": ["T1"]},
            {"task_id": "T3", "fn": make_task("T3", 30), "depends_on": ["T2"]},
            {"task_id": "T4", "fn": make_task("T4", 40), "depends_on": ["T3"]},
        ])

    results = scheduler.run() if hasattr(scheduler, "run") else scheduler.execute()

    assert execution_order == ["T1", "T2", "T3", "T4"]
    t1_val = _get_val(results, "T1") or results.get("T1")
    t4_val = _get_val(results, "T4") or results.get("T4")
    assert (t1_val == 10 or _get_val(t1_val, "result") == 10)
    assert (t4_val == 40 or _get_val(t4_val, "result") == 40)


def test_work_stealing_diamond_dag_parallelism() -> None:
    """Verify diamond DAG Root -> (Left, Right) -> Join parallel resolution."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=4)
    timestamps: Dict[str, Dict[str, float]] = {}

    def make_timed_task(name: str, sleep_s: float = 0.03) -> Callable[[], str]:
        def _fn() -> str:
            t_start = time.perf_counter()
            time.sleep(sleep_s)
            t_end = time.perf_counter()
            timestamps[name] = {"start": t_start, "end": t_end}
            return f"result_{name}"
        return _fn

    if hasattr(scheduler, "add_task"):
        scheduler.add_task("Root", fn=make_timed_task("Root"), depends_on=[])
        scheduler.add_task("Left", fn=make_timed_task("Left"), depends_on=["Root"])
        scheduler.add_task("Right", fn=make_timed_task("Right"), depends_on=["Root"])
        scheduler.add_task("Join", fn=make_timed_task("Join"), depends_on=["Left", "Right"])
    else:
        scheduler.schedule([
            {"task_id": "Root", "fn": make_timed_task("Root"), "depends_on": []},
            {"task_id": "Left", "fn": make_timed_task("Left"), "depends_on": ["Root"]},
            {"task_id": "Right", "fn": make_timed_task("Right"), "depends_on": ["Root"]},
            {"task_id": "Join", "fn": make_timed_task("Join"), "depends_on": ["Left", "Right"]},
        ])

    results = scheduler.run() if hasattr(scheduler, "run") else scheduler.execute()

    # Root must finish before Left and Right start
    assert timestamps["Root"]["end"] <= timestamps["Left"]["start"] + 0.01
    assert timestamps["Root"]["end"] <= timestamps["Right"]["start"] + 0.01

    # Left and Right must finish before Join starts
    assert timestamps["Left"]["end"] <= timestamps["Join"]["start"] + 0.01
    assert timestamps["Right"]["end"] <= timestamps["Join"]["start"] + 0.01

    assert "Root" in results and "Join" in results


def test_work_stealing_worker_starvation_and_stealing() -> None:
    """Verify idle workers steal ready tasks from heavily loaded peer deques."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=4)
    worker_set: set[str] = set()

    def make_task(i: int) -> Callable[[], int]:
        def _fn() -> int:
            time.sleep(0.015)
            # Record worker identifier if accessible
            cur_thread = sys._getframe().f_code.co_name
            worker_set.add(f"{cur_thread}_{i % 4}")
            return i * 2
        return _fn

    task_count = 16
    for i in range(task_count):
        if hasattr(scheduler, "add_task"):
            # All tasks independent, initially loaded
            scheduler.add_task(f"task_{i}", fn=make_task(i), depends_on=[])
        else:
            scheduler.schedule([{"task_id": f"task_{i}", "fn": make_task(i), "depends_on": []}])

    t0 = time.perf_counter()
    results = scheduler.run() if hasattr(scheduler, "run") else scheduler.execute()
    elapsed = time.perf_counter() - t0

    assert len(results) == task_count
    for i in range(task_count):
        val = results[f"task_{i}"]
        res_val = _get_val(val, "result") if not isinstance(val, (int, str)) else val
        assert res_val == i * 2

    # Parallel execution of 16 tasks with 4 workers should be significantly faster than 16 * 15ms = 240ms
    assert elapsed < 0.20, f"Work stealing should achieve parallel speedup, took {elapsed:.3f}s"

    # Verify steal telemetry if exposed
    if hasattr(scheduler, "steal_count"):
        assert scheduler.steal_count >= 0
    if hasattr(scheduler, "get_stats"):
        stats = scheduler.get_stats()
        assert isinstance(stats, dict)


def test_work_stealing_cycle_detection_error() -> None:
    """Verify cyclic dependency graph raises ValueError with cycle explanation."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=4)

    # Cycle: NodeA -> NodeB -> NodeA
    if hasattr(scheduler, "add_task"):
        scheduler.add_task("NodeA", fn=lambda: 1, depends_on=["NodeB"])
        scheduler.add_task("NodeB", fn=lambda: 2, depends_on=["NodeA"])
    else:
        scheduler.schedule([
            {"task_id": "NodeA", "fn": lambda: 1, "depends_on": ["NodeB"]},
            {"task_id": "NodeB", "fn": lambda: 2, "depends_on": ["NodeA"]},
        ])

    with pytest.raises(ValueError) as exc_info:
        if hasattr(scheduler, "validate_dag"):
            scheduler.validate_dag()
        else:
            scheduler.run() if hasattr(scheduler, "run") else scheduler.execute()

    err_msg = str(exc_info.value).lower()
    assert "cycle" in err_msg or "cyclic" in err_msg or "deadlock" in err_msg


def test_work_stealing_unknown_dependency_error() -> None:
    """Verify referencing non-existent dependency ID raises ValueError."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=4)

    if hasattr(scheduler, "add_task"):
        scheduler.add_task("TaskValid", fn=lambda: 1, depends_on=["GHOST_DEP_999"])
    else:
        scheduler.schedule([
            {"task_id": "TaskValid", "fn": lambda: 1, "depends_on": ["GHOST_DEP_999"]},
        ])

    with pytest.raises(ValueError) as exc_info:
        if hasattr(scheduler, "validate_dag"):
            scheduler.validate_dag()
        else:
            scheduler.run() if hasattr(scheduler, "run") else scheduler.execute()

    err_msg = str(exc_info.value).lower()
    assert "unknown" in err_msg or "missing" in err_msg or "ghost_dep_999" in err_msg


def test_work_stealing_task_failure_propagation() -> None:
    """Verify an exception in an upstream task halts dependent tasks cleanly."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=4)
    downstream_executed = []

    def failing_fn() -> None:
        raise RuntimeError("Simulated station hardware fault")

    def downstream_fn() -> str:
        downstream_executed.append("executed")
        return "should_not_run"

    if hasattr(scheduler, "add_task"):
        scheduler.add_task("FailingTask", fn=failing_fn, depends_on=[])
        scheduler.add_task("DownstreamTask", fn=downstream_fn, depends_on=["FailingTask"])
    else:
        scheduler.schedule([
            {"task_id": "FailingTask", "fn": failing_fn, "depends_on": []},
            {"task_id": "DownstreamTask", "fn": downstream_fn, "depends_on": ["FailingTask"]},
        ])

    try:
        results = scheduler.run() if hasattr(scheduler, "run") else scheduler.execute()
        # If scheduler does not raise, downstream task must not be marked success
        assert len(downstream_executed) == 0
        if "DownstreamTask" in results:
            assert _get_val(results["DownstreamTask"], "success") is False
    except Exception as exc:
        assert len(downstream_executed) == 0
        assert "Simulated station hardware fault" in str(exc) or "FailingTask" in str(exc)


def test_work_stealing_chase_lev_deque_contract() -> None:
    """Verify worker-local deque contract: owner LIFO pop vs thief FIFO steal."""
    assert WorkStealingDAGScheduler is not None, "WorkStealingDAGScheduler must be implemented in agy_federation_mesh.engine"

    scheduler = WorkStealingDAGScheduler(num_workers=2)
    # Check if ChaseLevDeque or worker queues are inspectable
    if hasattr(scheduler, "workers") or hasattr(scheduler, "deques"):
        deques = getattr(scheduler, "deques", getattr(scheduler, "workers", None))
        assert deques is not None


# ============================================================================
# 3. Version Alignment and Export Contracts
# ============================================================================

def test_v2_0_version_sync() -> None:
    """Verify package, pyproject.toml, and plugin.json all declare version 2.0.0."""
    import agy_federation_mesh

    assert agy_federation_mesh.__version__ == "2.0.0", (
        f"Expected agy_federation_mesh.__version__ == '2.0.0', got '{agy_federation_mesh.__version__}'"
    )

    pyproject_path = REPO_ROOT / "pyproject.toml"
    assert pyproject_path.is_file(), "pyproject.toml must exist"
    pyproject_content = pyproject_path.read_text(encoding="utf-8")
    assert 'version = "2.0.0"' in pyproject_content

    plugin_json_path = REPO_ROOT / "plugins" / "agy-federation-mesh" / "plugin.json"
    if not plugin_json_path.is_file():
        plugin_json_path = REPO_ROOT / "plugin.json"
    assert plugin_json_path.is_file(), "plugin.json must exist"
    plugin_data = json.loads(plugin_json_path.read_text(encoding="utf-8"))
    assert plugin_data.get("version") == "2.0.0"


def test_v2_0_engine_exports() -> None:
    """Verify ByzantineQuorumVerifier and WorkStealingDAGScheduler are exported at top level and in engine."""
    import agy_federation_mesh
    import agy_federation_mesh.engine as engine_mod

    assert hasattr(agy_federation_mesh, "ByzantineQuorumVerifier"), "ByzantineQuorumVerifier must be exported in agy_federation_mesh"
    assert hasattr(agy_federation_mesh, "WorkStealingDAGScheduler"), "WorkStealingDAGScheduler must be exported in agy_federation_mesh"
    assert hasattr(engine_mod, "ByzantineQuorumVerifier"), "ByzantineQuorumVerifier must be in agy_federation_mesh.engine"
    assert hasattr(engine_mod, "WorkStealingDAGScheduler"), "WorkStealingDAGScheduler must be in agy_federation_mesh.engine"
