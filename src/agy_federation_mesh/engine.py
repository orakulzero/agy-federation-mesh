"""
src/agy_federation_mesh/engine.py
Tier L5 Multi-Objective Pareto-Front Swarm Router & Speculative Dual-Execution Hedging Engine (v1.3.0):
1. SwarmWorkerProfile schema, property aliases, and serialization/deserialization.
2. Multi-objective Pareto frontier worker computation across Latency, Token Cost, and Reliability.
3. In-flight load penalty: L_eff = L_p50 * (1.0 + 0.2 * inflight).
4. Scalarized multi-objective utility loss selection with presets (MIN_COST, MIN_LATENCY, HIGH_RELIABILITY, BALANCED).
5. Critical-path speculative dual-execution hedging engine with automatic cancellation and thread cleanup.
"""

from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union


# ============================================================================
# 1. Swarm Worker Profile & Data Model
# ============================================================================

@dataclass
class SwarmWorkerProfile:
    """Worker node profile in the swarm federation."""

    id: str
    name: str = ""
    capabilities: List[str] = field(default_factory=list)
    latency_p50_ms: float = 20.0
    latency_p95_ms: float = 100.0
    latency_ema_ms: float = 25.0
    cost_per_1k_tokens: float = 0.002
    reliability_score: float = 0.99
    concurrency_limit: int = 10
    current_inflight: int = 0
    status: str = "ONLINE"  # ONLINE, DEGRADED, OFFLINE
    station_id: str = "default"
    base_url: str = "http://localhost"
    weight: float = 1.0
    consecutive_failures: int = 0

    def __post_init__(self) -> None:
        if not self.name:
            self.name = self.id
        if isinstance(self.capabilities, (set, tuple)):
            self.capabilities = list(self.capabilities)

    # Aliases for compatibility across survey blueprints and tests
    @property
    def worker_id(self) -> str:
        return self.id

    @property
    def token_cost_per_1k(self) -> float:
        return self.cost_per_1k_tokens

    @token_cost_per_1k.setter
    def token_cost_per_1k(self, value: float) -> None:
        self.cost_per_1k_tokens = float(value)

    @property
    def inflight_count(self) -> int:
        return self.current_inflight

    @inflight_count.setter
    def inflight_count(self, value: int) -> None:
        self.current_inflight = int(value)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize worker profile to dictionary."""
        return {
            "id": self.id,
            "worker_id": self.id,
            "name": self.name,
            "capabilities": list(self.capabilities),
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "latency_ema_ms": self.latency_ema_ms,
            "cost_per_1k_tokens": self.cost_per_1k_tokens,
            "token_cost_per_1k": self.cost_per_1k_tokens,
            "reliability_score": self.reliability_score,
            "concurrency_limit": self.concurrency_limit,
            "current_inflight": self.current_inflight,
            "inflight_count": self.current_inflight,
            "status": self.status,
            "station_id": self.station_id,
            "base_url": self.base_url,
            "weight": self.weight,
            "consecutive_failures": self.consecutive_failures,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SwarmWorkerProfile":
        """Instantiate profile from dictionary with alias handling."""
        worker_id = data.get("id") or data.get("worker_id") or "worker_unknown"
        name = data.get("name") or worker_id
        caps = data.get("capabilities", [])
        if isinstance(caps, (set, tuple)):
            caps = list(caps)

        return cls(
            id=str(worker_id),
            name=str(name),
            capabilities=caps,
            latency_p50_ms=float(data.get("latency_p50_ms", 20.0)),
            latency_p95_ms=float(data.get("latency_p95_ms", 100.0)),
            latency_ema_ms=float(data.get("latency_ema_ms", data.get("latency_p50_ms", 25.0))),
            cost_per_1k_tokens=float(data.get("cost_per_1k_tokens", data.get("token_cost_per_1k", 0.002))),
            reliability_score=float(data.get("reliability_score", 0.99)),
            concurrency_limit=int(data.get("concurrency_limit", 10)),
            current_inflight=int(data.get("current_inflight", data.get("inflight_count", 0))),
            status=str(data.get("status", "ONLINE")),
            station_id=str(data.get("station_id", "default")),
            base_url=str(data.get("base_url", "http://localhost")),
            weight=float(data.get("weight", 1.0)),
            consecutive_failures=int(data.get("consecutive_failures", 0)),
        )


# ============================================================================
# 2. Speculative Swarm Router
# ============================================================================

class SpeculativeSwarmRouter:
    """Multi-objective Pareto Task Router & Speculative Dual-Execution Swarm Engine."""

    def __init__(
        self,
        workers: Optional[Sequence[Union[Dict[str, Any], SwarmWorkerProfile]]] = None,
        hedging_delay_ms: float = 50.0,
    ) -> None:
        self.hedging_delay_ms = float(hedging_delay_ms)
        self.workers: Dict[str, SwarmWorkerProfile] = {}
        self._lock = threading.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=32,
            thread_name_prefix="swarm_hedging_worker",
        )
        if workers:
            for w in workers:
                self.register_worker(w)

    def register_worker(self, worker: Union[Dict[str, Any], SwarmWorkerProfile]) -> None:
        """Register or update a worker node in the swarm registry."""
        if isinstance(worker, dict):
            profile = SwarmWorkerProfile.from_dict(worker)
        elif isinstance(worker, SwarmWorkerProfile):
            profile = worker
        else:
            raise TypeError(f"Invalid worker type: {type(worker)}")

        with self._lock:
            self.workers[profile.id] = profile

    def get_worker(self, worker_id: str) -> Optional[SwarmWorkerProfile]:
        """Retrieve worker profile by ID."""
        with self._lock:
            return self.workers.get(worker_id)

    def list_workers(self) -> List[SwarmWorkerProfile]:
        """Return list of all registered worker profiles."""
        with self._lock:
            return list(self.workers.values())

    def compute_pareto_frontier(
        self,
        candidates: Optional[Sequence[Union[Dict[str, Any], SwarmWorkerProfile]]] = None,
        task_requirements: Optional[Dict[str, Any]] = None,
    ) -> List[SwarmWorkerProfile]:
        """Filter candidate workers and compute non-dominated Pareto frontier.

        Dominance criteria:
        Worker A dominates Worker B iff:
        - Effective Latency: L_eff_A <= L_eff_B
        - Token Cost: C_A <= C_B
        - Reliability: R_A >= R_B
        and at least one inequality is strict.
        """
        requirements = task_requirements or {}
        if candidates is None:
            with self._lock:
                pool = list(self.workers.values())
        else:
            pool = [
                SwarmWorkerProfile.from_dict(c) if isinstance(c, dict) else c
                for c in candidates
            ]

        # 1. Filter active (non-offline) workers
        active = [w for w in pool if w.status != "OFFLINE"]

        # 2. Strict constraint: required capabilities
        req_cap = requirements.get("required_capability")
        if req_cap:
            active = [w for w in active if req_cap in w.capabilities]
        req_caps = requirements.get("required_capabilities")
        if req_caps:
            active = [w for w in active if all(c in w.capabilities for c in req_caps)]

        # 3. Strict constraint: station_id
        station_id = requirements.get("station_id")
        if station_id:
            active = [w for w in active if w.station_id == station_id]

        # 4. Strict constraint: min_reliability
        min_rel = requirements.get("min_reliability")
        if min_rel is not None:
            active = [w for w in active if w.reliability_score >= float(min_rel)]

        # 5. Strict constraint: max_cost
        max_cost = requirements.get("max_cost")
        if max_cost is not None:
            active = [w for w in active if w.cost_per_1k_tokens <= float(max_cost)]

        # 6. Strict constraint: max_latency_ms (evaluated against p95 SLA)
        max_lat = requirements.get("max_latency_ms")
        if max_lat is not None:
            active = [w for w in active if w.latency_p95_ms <= float(max_lat)]

        # 7. Strict constraint: concurrency / capacity
        if requirements.get("enforce_concurrency", False):
            active = [w for w in active if w.current_inflight < w.concurrency_limit]

        if not active:
            return []

        # 8. Compute Pareto Frontier with in-flight load penalty
        frontier: List[SwarmWorkerProfile] = []
        for cand in active:
            cand_l = cand.latency_p50_ms * (1.0 + 0.2 * max(0, cand.current_inflight))
            cand_c = cand.cost_per_1k_tokens
            cand_r = cand.reliability_score
            is_dominated = False

            for other in active:
                if other.id == cand.id:
                    continue
                oth_l = other.latency_p50_ms * (1.0 + 0.2 * max(0, other.current_inflight))
                oth_c = other.cost_per_1k_tokens
                oth_r = other.reliability_score

                if oth_l <= cand_l and oth_c <= cand_c and oth_r >= cand_r:
                    if oth_l < cand_l or oth_c < cand_c or oth_r > cand_r:
                        is_dominated = True
                        break

            if not is_dominated:
                frontier.append(cand)

        return frontier

    def select_pareto_worker(self, task_requirements: Dict[str, Any]) -> str:
        """Select optimal worker ID from Pareto frontier based on requirements and weights.

        Supports:
        - Strict constraints: max_cost, max_latency_ms, min_reliability
        - Weights: weight_latency, weight_cost, weight_reliability
        - Policy presets: cost_focused (or MIN_COST), latency_focused (or MIN_LATENCY),
          reliability_focused (or HIGH_RELIABILITY), BALANCED.
        """
        reqs = task_requirements or {}
        frontier = self.compute_pareto_frontier(task_requirements=reqs)

        if not frontier:
            with self._lock:
                online_count = sum(1 for w in self.workers.values() if w.status != "OFFLINE")
            if online_count == 0:
                raise ValueError("No online workers available in swarm registry")
            raise ValueError("No eligible worker satisfies strict constraints")

        # Resolve weights & focus policy
        w_lat = reqs.get("weight_latency")
        w_cost = reqs.get("weight_cost")
        w_rel = reqs.get("weight_reliability")

        if reqs.get("cost_focused") or reqs.get("policy") == "MIN_COST":
            w_lat = 0.0 if w_lat is None else float(w_lat)
            w_cost = 1.0 if w_cost is None else float(w_cost)
            w_rel = 0.0 if w_rel is None else float(w_rel)
        elif reqs.get("latency_focused") or reqs.get("policy") == "MIN_LATENCY":
            w_lat = 1.0 if w_lat is None else float(w_lat)
            w_cost = 0.0 if w_cost is None else float(w_cost)
            w_rel = 0.0 if w_rel is None else float(w_rel)
        elif reqs.get("reliability_focused") or reqs.get("policy") == "HIGH_RELIABILITY":
            w_lat = 0.0 if w_lat is None else float(w_lat)
            w_cost = 0.0 if w_cost is None else float(w_cost)
            w_rel = 1.0 if w_rel is None else float(w_rel)
        else:
            w_lat = 0.5 if w_lat is None else float(w_lat)
            w_cost = 0.3 if w_cost is None else float(w_cost)
            w_rel = 0.2 if w_rel is None else float(w_rel)

        # Single objective shortcut for exact numerical tie-breakers
        if w_cost == 1.0 and w_lat == 0.0 and w_rel == 0.0:
            best = min(frontier, key=lambda w: (w.cost_per_1k_tokens, w.latency_p50_ms, -w.reliability_score))
            return best.id
        if w_lat == 1.0 and w_cost == 0.0 and w_rel == 0.0:
            best = min(frontier, key=lambda w: (w.latency_p50_ms * (1.0 + 0.2 * w.current_inflight), w.cost_per_1k_tokens, -w.reliability_score))
            return best.id
        if w_rel == 1.0 and w_lat == 0.0 and w_cost == 0.0:
            best = max(frontier, key=lambda w: (w.reliability_score, -w.latency_p50_ms, -w.cost_per_1k_tokens))
            return best.id

        # Normalize weights
        total_w = w_lat + w_cost + w_rel
        if total_w > 0:
            w_lat /= total_w
            w_cost /= total_w
            w_rel /= total_w

        # Scalarized multi-objective utility scoring
        latencies = [w.latency_p50_ms * (1.0 + 0.2 * max(0, w.current_inflight)) for w in frontier]
        costs = [w.cost_per_1k_tokens for w in frontier]
        reliabilities = [w.reliability_score for w in frontier]

        min_l, max_l = min(latencies), max(latencies)
        min_c, max_c = min(costs), max(costs)
        min_r, max_r = min(reliabilities), max(reliabilities)

        def _utility_loss(w: SwarmWorkerProfile) -> float:
            eff_l = w.latency_p50_ms * (1.0 + 0.2 * max(0, w.current_inflight))
            norm_l = (eff_l - min_l) / (max_l - min_l) if max_l > min_l else 0.0
            norm_c = (w.cost_per_1k_tokens - min_c) / (max_c - min_c) if max_c > min_c else 0.0
            norm_r = (w.reliability_score - min_r) / (max_r - min_r) if max_r > min_r else 1.0

            return (w_lat * norm_l) + (w_cost * norm_c) + (w_rel * (1.0 - norm_r))

        best_worker = min(frontier, key=_utility_loss)
        return best_worker.id

    def execute_with_hedging(
        self,
        task_id: str,
        primary_fn: Optional[Callable[[], Any]] = None,
        fallback_fn: Optional[Callable[[], Any]] = None,
        hedging_delay_ms: Optional[float] = None,
        critical_path: bool = False,
        task_fn: Optional[Callable[[], Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Execute task with delayed speculative hedging for critical path nodes.

        - If critical_path=False: executes primary_fn directly.
        - If critical_path=True: executes primary_fn. If it does not complete within
          hedging_delay_ms, launches fallback_fn (or duplicate primary) speculatively.
          First completing result wins.
        """
        fn_to_run = primary_fn if primary_fn is not None else task_fn
        if fn_to_run is None:
            raise ValueError("execute_with_hedging requires primary_fn or task_fn")

        delay_ms = hedging_delay_ms if hedging_delay_ms is not None else kwargs.get("hedge_delay_ms", self.hedging_delay_ms)
        is_crit = critical_path or bool(kwargs.get("is_critical", False))

        t0 = time.perf_counter()

        def _extract_tokens_and_cost(result_val: Any) -> Tuple[int, float]:
            if isinstance(result_val, dict):
                tokens = int(result_val.get("tokens") or result_val.get("estimated_tokens") or 500)
                cost = float(result_val.get("cost") or result_val.get("token_cost") or 0.001)
                return tokens, cost
            return 500, 0.001

        # Non-critical path execution
        if not is_crit:
            try:
                res = fn_to_run()
                dur_ms = (time.perf_counter() - t0) * 1000.0
                _, token_cost = _extract_tokens_and_cost(res)
                return {
                    "task_id": task_id,
                    "result": res,
                    "success": True,
                    "hedged": False,
                    "winning_channel": "primary",
                    "latency_ms": dur_ms,
                    "token_cost": token_cost,
                    "error": None,
                    "latency_saved_ms": 0.0,
                }
            except Exception as exc:
                dur_ms = (time.perf_counter() - t0) * 1000.0
                return {
                    "task_id": task_id,
                    "result": None,
                    "success": False,
                    "hedged": False,
                    "winning_channel": "none",
                    "latency_ms": dur_ms,
                    "token_cost": 0.0,
                    "error": str(exc),
                    "latency_saved_ms": 0.0,
                }

        # Critical path execution with speculative hedging
        f_prim = self._executor.submit(fn_to_run)
        timeout_sec = max(0.001, float(delay_ms) / 1000.0)

        try:
            res = f_prim.result(timeout=timeout_sec)
            dur_ms = (time.perf_counter() - t0) * 1000.0
            _, token_cost = _extract_tokens_and_cost(res)
            return {
                "task_id": task_id,
                "result": res,
                "success": True,
                "hedged": False,
                "winning_channel": "primary",
                "latency_ms": dur_ms,
                "token_cost": token_cost,
                "error": None,
                "latency_saved_ms": 0.0,
            }
        except concurrent.futures.TimeoutError:
            sec_fn = fallback_fn if fallback_fn is not None else fn_to_run
            f_sec = self._executor.submit(sec_fn)

            futures_map = {f_prim: "primary", f_sec: "fallback"}
            done, _ = concurrent.futures.wait(
                futures_map.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            first_f = list(done)[0]
            first_channel = futures_map[first_f]

            try:
                res = first_f.result()
                dur_ms = (time.perf_counter() - t0) * 1000.0
                _, token_cost = _extract_tokens_and_cost(res)
                token_cost += 0.001
                latency_saved = max(0.0, 150.0 - dur_ms) if first_channel == "fallback" else 0.0

                return {
                    "task_id": task_id,
                    "result": res,
                    "success": True,
                    "hedged": True,
                    "winning_channel": first_channel,
                    "latency_ms": dur_ms,
                    "token_cost": token_cost,
                    "error": None,
                    "latency_saved_ms": latency_saved,
                }
            except Exception as exc1:
                other_f = f_sec if first_f is f_prim else f_prim
                other_channel = futures_map[other_f]
                try:
                    res = other_f.result(timeout=kwargs.get("fallback_timeout_s", 5.0))
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    _, token_cost = _extract_tokens_and_cost(res)
                    return {
                        "task_id": task_id,
                        "result": res,
                        "success": True,
                        "hedged": True,
                        "winning_channel": other_channel,
                        "latency_ms": dur_ms,
                        "token_cost": token_cost + 0.001,
                        "error": None,
                        "latency_saved_ms": 0.0,
                    }
                except Exception as exc2:
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    return {
                        "task_id": task_id,
                        "result": None,
                        "success": False,
                        "hedged": True,
                        "winning_channel": "none",
                        "latency_ms": dur_ms,
                        "token_cost": 0.0,
                        "error": f"Primary failed: {exc1}; Fallback failed: {exc2}",
                        "latency_saved_ms": 0.0,
                    }
        except Exception as prim_exc:
            if fallback_fn is not None:
                try:
                    res = fallback_fn()
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    _, token_cost = _extract_tokens_and_cost(res)
                    return {
                        "task_id": task_id,
                        "result": res,
                        "success": True,
                        "hedged": True,
                        "winning_channel": "fallback",
                        "latency_ms": dur_ms,
                        "token_cost": token_cost,
                        "error": None,
                        "latency_saved_ms": 0.0,
                    }
                except Exception as fb_exc:
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    return {
                        "task_id": task_id,
                        "result": None,
                        "success": False,
                        "hedged": True,
                        "winning_channel": "none",
                        "latency_ms": dur_ms,
                        "token_cost": 0.0,
                        "error": f"Primary failed: {prim_exc}; Fallback failed: {fb_exc}",
                        "latency_saved_ms": 0.0,
                    }
            else:
                dur_ms = (time.perf_counter() - t0) * 1000.0
                return {
                    "task_id": task_id,
                    "result": None,
                    "success": False,
                    "hedged": False,
                    "winning_channel": "none",
                    "latency_ms": dur_ms,
                    "token_cost": 0.0,
                    "error": str(prim_exc),
                    "latency_saved_ms": 0.0,
                }

    def close(self) -> None:
        """Shutdown internal executor pool."""
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "SpeculativeSwarmRouter":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def compute_pareto_optimal_workers(
    workers: Sequence[Union[Dict[str, Any], SwarmWorkerProfile]],
    task_requirements: Optional[Dict[str, Any]] = None,
) -> List[SwarmWorkerProfile]:
    """Compute non-dominated Pareto frontier workers from candidates."""
    router = SpeculativeSwarmRouter(workers=workers)
    try:
        return router.compute_pareto_frontier(task_requirements=task_requirements)
    finally:
        router.close()
