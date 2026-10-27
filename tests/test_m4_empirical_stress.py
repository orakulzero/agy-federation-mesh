"""
tests/test_m4_empirical_stress.py
Milestone M4: Empirical Adversarial Stress Suite for AGY Federation Mesh v2.0.0.

Authored by m4_challenger_1 (EMPIRICAL CHALLENGER).
Verifies:
- ByzantineQuorumVerifier:
  - High fault counts (f=5, N=16) with sharp phase transition (V_max = 2f = 10 vs 2f + 1 = 11).
  - Multi-variant corrupted payloads and bit-flip tampering.
  - Unicode payloads (multibyte UTF-8, Cyrillic, Chinese, Emoji, RTL, control chars).
  - Deeply nested arrays, array ordering sensitivity, and dict key order determinism.
  - Boundary conditions (f=0, N < 2f + 1, empty outputs).
  - Non-JSON serializable payload error handling.
- WorkStealingDAGScheduler:
  - 100+ tasks with high concurrency across 8 workers.
  - Deep linear chain (depth 100) topological sequencing.
  - Wide fan-out / fan-in tree (Root -> 100 Branches -> Sink).
  - Upstream exception halting and cascading prevention.
  - Dynamic uneven workload work stealing.
  - Direct concurrent ChaseLevDeque fuzzing (1 owner LIFO + 4 thieves FIFO).
"""

from __future__ import annotations

import collections
import concurrent.futures
import hashlib
import json
from pathlib import Path
import random
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from agy_federation_mesh.engine import (
    ByzantineConsensusResult,
    ByzantineQuorumVerifier,
    ChaseLevDeque,
    WorkStealingDAGScheduler,
)


def _get_val(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


# ============================================================================
# Section 1: ByzantineQuorumVerifier Empirical Stress Tests
# ============================================================================

def test_byzantine_high_fault_count_f5_n16_sharp_boundary() -> None:
    """
    Stress-test high fault tolerance f=5 in an N=16 network.
    Quorum threshold is 2f + 1 = 11.
    Empirically verify the sharp boundary:
    - V_max = 2f = 10 votes -> MUST REJECT consensus (BYZANTINE_SPLIT_BRAIN).
    - V_max = 2f + 1 = 11 votes -> MUST REACH consensus.
    """
    verifier = ByzantineQuorumVerifier(f=5)
    assert verifier.quorum_threshold == 11

    valid_payload = {
        "epoch": 105,
        "action": "COMMIT_STATE_DIFF",
        "merkle_root": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "station_signatures": ["sig_01", "sig_02", "sig_03"],
    }

    # Case A: Boundary condition V_max = 2f = 10 (10 honest, 6 Byzantine)
    outputs_at_2f: Dict[str, Any] = {}
    for i in range(10):
        outputs_at_2f[f"worker_honest_{i:02d}"] = valid_payload
    for i in range(6):
        outputs_at_2f[f"worker_byzantine_{i:02d}"] = {
            "epoch": 105,
            "action": "TAMPERED_ACTION",
            "merkle_root": f"bad_root_{i}",
        }

    res_2f = verifier.verify(outputs_at_2f)
    assert _get_val(res_2f, "consensus_reached") is False, (
        f"Consensus must NOT be reached when V_max=10 (2f) < quorum=11 (2f+1)"
    )
    assert _get_val(res_2f, "consensus_output") is None
    status_str = str(_get_val(res_2f, "status"))
    assert "BYZANTINE_SPLIT_BRAIN" in status_str or "SPLIT_BRAIN" in status_str

    # Case B: Boundary condition V_max = 2f + 1 = 11 (11 honest, 5 Byzantine)
    outputs_at_2f_plus_1: Dict[str, Any] = {}
    for i in range(11):
        outputs_at_2f_plus_1[f"worker_honest_{i:02d}"] = valid_payload
    for i in range(5):
        outputs_at_2f_plus_1[f"worker_byzantine_{i:02d}"] = {
            "epoch": 105,
            "action": "CORRUPTED_ACTION",
            "salt": i * 999,
        }

    res_2f1 = verifier.verify(outputs_at_2f_plus_1)
    assert _get_val(res_2f1, "consensus_reached") is True, (
        f"Consensus MUST be reached when V_max=11 (2f+1) >= quorum=11"
    )
    assert _get_val(res_2f1, "consensus_output") == valid_payload
    assert len(_get_val(res_2f1, "agreeing_workers")) == 11
    assert len(_get_val(res_2f1, "faulty_workers")) == 5
    assert set(_get_val(res_2f1, "faulty_workers")) == {
        f"worker_byzantine_{i:02d}" for i in range(5)
    }


def test_byzantine_random_corrupted_payloads_f3_n10() -> None:
    """
    Stress-test f=3 (quorum = 7, N = 10) against randomized payload mutations.
    7 honest nodes submit the genuine payload; 3 Byzantine nodes submit randomized corruptions.
    """
    verifier = ByzantineQuorumVerifier(f=3)
    assert verifier.quorum_threshold == 7

    genuine_payload = {
        "telemetry": {"p50": 12.4, "p95": 45.2, "errors": 0},
        "flags": [True, False, True],
        "meta": {"label": "L5_FEDERATION_PULSE", "nested_id": 99999},
    }

    # Run 25 randomized trials
    rng = random.Random(42)
    for trial in range(25):
        worker_outputs: Dict[str, Any] = {}
        for h in range(7):
            worker_outputs[f"honest_{h}"] = genuine_payload

        # 3 Byzantine corruptions
        worker_outputs["byz_0"] = {
            "telemetry": {"p50": 12.4 + rng.random(), "p95": 45.2, "errors": 0},
            "flags": [True, False, True],
            "meta": {"label": "L5_FEDERATION_PULSE", "nested_id": 99999},
        }
        worker_outputs["byz_1"] = {
            "telemetry": {"p50": 12.4, "p95": 45.2, "errors": rng.randint(1, 100)},
            "flags": [True, True, True],
            "meta": {"label": "CORRUPTED", "nested_id": 99999},
        }
        worker_outputs["byz_2"] = None  # None payload

        res = verifier.verify(worker_outputs)
        assert _get_val(res, "consensus_reached") is True
        assert _get_val(res, "consensus_output") == genuine_payload
        assert len(_get_val(res, "agreeing_workers")) == 7
        assert set(_get_val(res, "faulty_workers")) == {"byz_0", "byz_1", "byz_2"}


def test_byzantine_unicode_and_multibyte_payloads() -> None:
    """
    Stress-test Byzantine consensus with complex Unicode strings:
    - Cyrillic: Russian engineering text
    - East Asian: Chinese / Japanese ideographs
    - Emoji: high code point SMP characters (\U0001F680)
    - Bidirectional / RTL: Arabic / Hebrew
    - Control & Zero-Width characters: \u200b \u200c \u200d \ufeff
    """
    verifier = ByzantineQuorumVerifier(f=1)
    unicode_payload = {
        "cyrillic": "\u0410\u043d\u0442\u0438\u0433\u0440\u0430\u0432\u0438\u0442\u0430\u0446\u0438\u044f \u0422\u0435\u0441\u0442",
        "cjk": "\u5fae\u670d\u52a1\u67b6\u6784 \u91cf\u5b50\u8ba1\u7b97",
        "emoji": "\U0001F680 \U0001F525 \U0001F6E1 \u2705",
        "arabic": "\u0627\u0644\u0645\u0635\u0627\u062f\u0642\u0629 \u0627\u0644\u0628\u064a\u0632\u0646\u0637\u064a\u0629",
        "invisible": "prefix\u200b\u200c\u200d\ufeffsuffix",
        "escapes": "line1\nline2\ttabbed\\\"quoted\\\"",
    }

    worker_outputs = {
        "w1": unicode_payload,
        "w2": unicode_payload,
        "w3": unicode_payload,
        "w4": {"cyrillic": "divergent text"},
    }

    res = verifier.verify(worker_outputs)
    assert _get_val(res, "consensus_reached") is True
    assert _get_val(res, "consensus_output") == unicode_payload

    # Verify deterministic SHA-256 hash formatting
    c_hash = _get_val(res, "consensus_hash")
    assert isinstance(c_hash, str) and len(c_hash) == 64
    manual_json = json.dumps(unicode_payload, sort_keys=True, separators=(",", ":"))
    expected_hash = hashlib.sha256(manual_json.encode("utf-8")).hexdigest()
    assert c_hash == expected_hash


def test_byzantine_deeply_nested_arrays_and_ordering() -> None:
    """
    Stress-test nested arrays and ordering sensitivity:
    - Array order matters: [1, 2, 3] != [3, 2, 1] -> must not match.
    - Dict key order does NOT matter -> must match.
    - Large 500-element nested list.
    """
    verifier = ByzantineQuorumVerifier(f=1)

    deep_list = [i for i in range(500)]
    deep_payload = {"level_1": {"level_2": {"data": deep_list, "flag": True}}}

    # Array with reverse order (different data!)
    divergent_array_payload = {"level_1": {"level_2": {"data": list(reversed(deep_list)), "flag": True}}}

    # Dict with permuted keys (identical data!)
    permuted_dict_payload = {"level_1": {"level_2": {"flag": True, "data": deep_list}}}

    worker_outputs = {
        "w0": deep_payload,
        "w1": permuted_dict_payload,  # Should match w0
        "w2": deep_payload,           # Should match w0
        "w3": divergent_array_payload,# Divergent array ordering
    }

    res = verifier.verify(worker_outputs)
    assert _get_val(res, "consensus_reached") is True
    assert set(_get_val(res, "agreeing_workers")) == {"w0", "w1", "w2"}
    assert set(_get_val(res, "faulty_workers")) == {"w3"}


def test_byzantine_boundary_edge_conditions() -> None:
    """Test edge cases: f=0, empty worker dict, insufficient workers."""
    # 1. f=0: quorum threshold = 1
    v0 = ByzantineQuorumVerifier(f=0)
    assert v0.quorum_threshold == 1
    res_single = v0.verify({"node_alone": {"status": "SOLITARY"}})
    assert _get_val(res_single, "consensus_reached") is True

    # 2. Empty worker outputs
    v1 = ByzantineQuorumVerifier(f=1)
    res_empty = v1.verify({})
    assert _get_val(res_empty, "consensus_reached") is False
    assert "empty" in str(_get_val(res_empty, "status")).lower()

    # 3. Insufficient workers (f=3, quorum=7, but only 4 workers reporting)
    v3 = ByzantineQuorumVerifier(f=3)
    outputs_few = {f"w_{i}": {"x": 1} for i in range(4)}
    res_few = v3.verify(outputs_few)
    assert _get_val(res_few, "consensus_reached") is False
    assert "insufficient" in str(_get_val(res_few, "status")).lower()

    # 4. Negative f validation
    with pytest.raises(ValueError):
        ByzantineQuorumVerifier(f=-1)


def test_byzantine_non_serializable_payload_observation() -> None:
    """
    Empirical probe: what happens when a Byzantine worker injects a non-JSON serializable object (e.g. bytes)?
    Under standard json.dumps, bytes raises TypeError.
    This test verifies whether the verifier handles or raises TypeError.
    """
    verifier = ByzantineQuorumVerifier(f=1)
    worker_outputs = {
        "w1": {"data": "valid_json"},
        "w2": {"data": "valid_json"},
        "w3": {"data": "valid_json"},
        "w_byz": b"raw_unserializable_bytes",
    }

    try:
        res = verifier.verify(worker_outputs)
        # If the verifier catches TypeError and marks w_byz as faulty:
        assert _get_val(res, "consensus_reached") is True
        assert "w_byz" in _get_val(res, "faulty_workers")
    except TypeError as te:
        # If verifier raises TypeError, document this behavior
        assert "is not JSON serializable" in str(te) or "TypeError" in str(te)


# ============================================================================
# Section 2: WorkStealingDAGScheduler Empirical Stress Tests
# ============================================================================

def test_work_stealing_128_tasks_high_concurrency_8_workers() -> None:
    """
    Stress-test WorkStealingDAGScheduler with 128 tasks across 8 workers.
    Ensures all 128 tasks execute, return correct results, and achieve parallel execution.
    """
    scheduler = WorkStealingDAGScheduler(num_workers=8)
    task_count = 128
    results_collector: Dict[str, int] = {}
    lock = threading.Lock()

    def make_task_fn(idx: int) -> Callable[[], int]:
        def _fn() -> int:
            # Micro sleep to allow thread contention and stealing
            time.sleep(0.001)
            val = idx * 10
            with lock:
                results_collector[f"task_{idx}"] = val
            return val
        return _fn

    for i in range(task_count):
        scheduler.add_task(f"task_{i}", fn=make_task_fn(i), depends_on=[])

    t0 = time.perf_counter()
    out = scheduler.run()
    elapsed = time.perf_counter() - t0

    assert len(out) == task_count
    assert len(results_collector) == task_count

    for i in range(task_count):
        assert out[f"task_{i}"] == i * 10

    # 128 * 1ms sequential = 128ms. With 8 workers, should finish well under 1.5s
    assert elapsed < 1.5, f"Execution too slow: {elapsed:.3f}s for 128 tasks"

    stats = scheduler.get_stats()
    assert stats["total_tasks"] == task_count
    assert stats["num_workers"] == 8


def test_work_stealing_deep_linear_chain_depth_100() -> None:
    """
    Stress-test a deep linear chain: T_0 -> T_1 -> T_2 -> ... -> T_99 (depth 100).
    Verify strict topological execution sequence, no deadlocks, and correct accumulator.
    """
    scheduler = WorkStealingDAGScheduler(num_workers=4)
    chain_length = 100
    trace: List[int] = []
    lock = threading.Lock()

    def make_step_fn(step: int) -> Callable[[], int]:
        def _fn() -> int:
            with lock:
                trace.append(step)
            return step * 2
        return _fn

    scheduler.add_task("T_0", fn=make_step_fn(0), depends_on=[])
    for step in range(1, chain_length):
        scheduler.add_task(f"T_{step}", fn=make_step_fn(step), depends_on=[f"T_{step-1}"])

    out = scheduler.run()

    assert len(out) == chain_length
    assert trace == list(range(chain_length)), "Deep chain violated sequential execution order"
    assert out["T_99"] == 99 * 2


def test_work_stealing_wide_fan_out_fan_in_tree() -> None:
    """
    Stress-test wide fan-out fan-in topology:
    Root -> [Branch_0 .. Branch_99] (100 parallel branches) -> JoinSink.
    Ensures that JoinSink never runs before all 100 branches finish.
    """
    scheduler = WorkStealingDAGScheduler(num_workers=8)
    branch_count = 100
    branch_finished: List[int] = []
    lock = threading.Lock()

    def root_fn() -> str:
        return "ROOT_SEED"

    def branch_fn(idx: int) -> Callable[[], int]:
        def _fn() -> int:
            time.sleep(0.002)
            with lock:
                branch_finished.append(idx)
            return idx * 3
        return _fn

    join_executed_at_branch_count = -1

    def join_fn() -> int:
        nonlocal join_executed_at_branch_count
        with lock:
            join_executed_at_branch_count = len(branch_finished)
        return sum(branch_finished) * 3

    scheduler.add_task("Root", fn=root_fn, depends_on=[])
    branch_ids = [f"Branch_{i}" for i in range(branch_count)]
    for i in range(branch_count):
        scheduler.add_task(branch_ids[i], fn=branch_fn(i), depends_on=["Root"])

    scheduler.add_task("JoinSink", fn=join_fn, depends_on=branch_ids)

    out = scheduler.run()

    assert len(out) == branch_count + 2
    assert join_executed_at_branch_count == branch_count, (
        f"JoinSink executed prematurely! Only {join_executed_at_branch_count} branches had finished"
    )
    assert out["JoinSink"] == sum(range(branch_count)) * 3


def test_work_stealing_task_exception_halting_and_isolation() -> None:
    """
    Verify upstream task exception halts downstream execution immediately
    and propagates the original exception out of scheduler.run().
    """
    scheduler = WorkStealingDAGScheduler(num_workers=4)
    downstream_executed = False

    def healthy_root() -> str:
        return "OK"

    def failing_task() -> None:
        raise ValueError("FATAL_STATION_PAYLOAD_ERROR")

    def downstream_of_failing() -> str:
        nonlocal downstream_executed
        downstream_executed = True
        return "UNREACHABLE"

    scheduler.add_task("RootA", fn=healthy_root, depends_on=[])
    scheduler.add_task("RootB_Failing", fn=failing_task, depends_on=[])
    scheduler.add_task("Downstream", fn=downstream_of_failing, depends_on=["RootB_Failing"])

    with pytest.raises(ValueError) as exc_info:
        scheduler.run()

    assert "FATAL_STATION_PAYLOAD_ERROR" in str(exc_info.value)
    assert downstream_executed is False, "Downstream task must NEVER execute when upstream task failed"


def test_work_stealing_dynamic_uneven_load_balancing() -> None:
    """
    Stress-test dynamic load balancing:
    Worker 0 initially receives 24 tasks:
    - First 4 tasks are 'heavy' (sleep 25ms each).
    - Remaining 20 tasks are 'light' (sleep 1ms each).
    With 4 workers, idle workers MUST steal light tasks and achieve steal_count > 0.
    """
    scheduler = WorkStealingDAGScheduler(num_workers=4)

    def heavy_task(i: int) -> Callable[[], int]:
        def _fn() -> int:
            time.sleep(0.025)
            return i
        return _fn

    def light_task(i: int) -> Callable[[], int]:
        def _fn() -> int:
            time.sleep(0.001)
            return i
        return _fn

    for i in range(4):
        scheduler.add_task(f"heavy_{i}", fn=heavy_task(i), depends_on=[])
    for i in range(4, 24):
        scheduler.add_task(f"light_{i}", fn=light_task(i), depends_on=[])

    t0 = time.perf_counter()
    res = scheduler.run()
    elapsed = time.perf_counter() - t0

    assert len(res) == 24
    # Serial execution: 4 * 25ms + 20 * 1ms = 120ms.
    # Parallel execution with work stealing should be well under 250ms
    assert elapsed < 0.25, f"Execution took too long: {elapsed:.3f}s"

    stats = scheduler.get_stats()
    assert stats["steal_count"] > 0, f"Expected work-stealing occurrences, got {stats['steal_count']}"


def test_chase_lev_deque_concurrent_fuzz_1_owner_4_thieves() -> None:
    """
    Direct concurrent fuzzing of ChaseLevDeque:
    - 1 Owner thread pushing 500 items and popping from tail (LIFO).
    - 4 Thief threads concurrently stealing from head (FIFO).
    - Total items popped + stolen MUST equal 500 with zero duplicates and zero drops.
    """
    deque = ChaseLevDeque(worker_id=0)
    total_items = 500

    popped_by_owner: List[int] = []
    stolen_by_thieves: List[int] = []
    stolen_lock = threading.Lock()
    owner_done = threading.Event()

    def owner_routine() -> None:
        for i in range(total_items):
            deque.push(i)
            # Intermittently pop owner item
            if i % 3 == 0:
                item = deque.pop()
                if item is not None:
                    popped_by_owner.append(item)
            time.sleep(0.0001)
        owner_done.set()
        # Drain remaining items
        while True:
            item = deque.pop()
            if item is None:
                break
            popped_by_owner.append(item)

    def thief_routine(thief_id: int) -> None:
        while not owner_done.is_set() or len(deque) > 0:
            item = deque.steal()
            if item is not None:
                with stolen_lock:
                    stolen_by_thieves.append(item)
            else:
                time.sleep(0.0002)

    threads: List[threading.Thread] = []
    t_owner = threading.Thread(target=owner_routine, name="owner_thread")
    threads.append(t_owner)

    for tid in range(4):
        t_thief = threading.Thread(target=thief_routine, args=(tid,), name=f"thief_{tid}")
        threads.append(t_thief)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    all_retrieved = popped_by_owner + stolen_by_thieves
    assert len(all_retrieved) == total_items, (
        f"Item mismatch: expected {total_items}, got {len(all_retrieved)} (popped: {len(popped_by_owner)}, stolen: {len(stolen_by_thieves)})"
    )
    # Check uniqueness
    assert len(set(all_retrieved)) == total_items, "Detected duplicate task popped/stolen from ChaseLevDeque!"
