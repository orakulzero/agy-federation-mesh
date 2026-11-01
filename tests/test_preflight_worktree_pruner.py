"""Agile V Verification Tests for Pre-Flight Git Worktree Pruner & Lock Cleaner.

Traceability:
- REQ-FM-WORKTREE-01: Pre-flight orphaned git worktree pruner and stale lock cleaner before DAG node dispatch.

Quality Framework: ISO 9001:2015, ISO/IEC 42001:2023, Agile V Role Separation.
Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Any, Dict, List
import pytest

from agy_federation_mesh.l1_primitives import StationNode, WorkflowTask
from agy_federation_mesh.l3_pipeline import FederationMeshOrchestrator

try:
    from agy_federation_mesh.worktree_pruner import prune_orphaned_worktrees_and_locks
except ImportError:
    prune_orphaned_worktrees_and_locks = None


def _init_test_git_repo(repo_dir: Path) -> Path:
    repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=str(repo_dir), check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "mesh@antigravity.org"], cwd=str(repo_dir), check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Mesh Tester"], cwd=str(repo_dir), check=True, capture_output=True)
    readme = repo_dir / "README.md"
    readme.write_text("# Federation Node Repo\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(repo_dir), check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "Initial commit", "-q"], cwd=str(repo_dir), check=True, capture_output=True)
    return repo_dir


# ============================================================================
# REQ-FM-WORKTREE-01: Pre-Flight Worktree Pruner & Stale Lock Cleaner
# ============================================================================


def test_req_fm_worktree_01_module_contract_and_presence():
    """Verify agy_federation_mesh.worktree_pruner module contract is implemented (REQ-FM-WORKTREE-01)."""
    assert prune_orphaned_worktrees_and_locks is not None, (
        "Contract violation: agy_federation_mesh.worktree_pruner.prune_orphaned_worktrees_and_locks must be implemented"
    )


def test_req_fm_worktree_01_clean_repository_reports_no_deadlock(tmp_path: Path):
    """Verify healthy git workspace returns zero deadlock risk and empty issues (REQ-FM-WORKTREE-01)."""
    assert prune_orphaned_worktrees_and_locks is not None
    repo = _init_test_git_repo(tmp_path / "clean_mesh_repo")
    report = prune_orphaned_worktrees_and_locks(repo, dry_run=False)
    assert report["has_deadlock_risk"] is False
    assert report["issues_count"] == 0
    assert len(report["issues"]) == 0
    assert report["healed"] is False


def test_req_fm_worktree_01_removes_stale_index_lock(tmp_path: Path):
    """Verify stale .git/index.lock is detected in dry-run and deleted in active run (REQ-FM-WORKTREE-01)."""
    assert prune_orphaned_worktrees_and_locks is not None
    repo = _init_test_git_repo(tmp_path / "stale_index_repo")
    index_lock = repo / ".git" / "index.lock"
    index_lock.write_text("lock pid 99999\n", encoding="utf-8")

    # Dry-run
    dry_rep = prune_orphaned_worktrees_and_locks(repo, dry_run=True)
    assert dry_rep["has_deadlock_risk"] is True
    assert "stale_index_lock" in dry_rep["issues"]
    assert index_lock.exists()

    # Active run
    heal_rep = prune_orphaned_worktrees_and_locks(repo, dry_run=False)
    assert heal_rep["healed"] is True
    assert "removed stale .git/index.lock" in heal_rep["actions_taken"]
    assert not index_lock.exists()


def test_req_fm_worktree_01_prunes_dangling_worktree_and_stale_locks(tmp_path: Path):
    """Verify orphaned worktree directories and *.lock files are safely cleaned (REQ-FM-WORKTREE-01)."""
    assert prune_orphaned_worktrees_and_locks is not None
    repo = _init_test_git_repo(tmp_path / "deadlock_mesh_repo")
    wt_root = repo / ".git" / "worktrees"
    wt_root.mkdir(parents=True, exist_ok=True)

    # 1. Stale worktree with lock file
    wt_active = wt_root / "node_worker_1"
    wt_active.mkdir(parents=True, exist_ok=True)
    target_dir = tmp_path / "real_worker_dir"
    target_dir.mkdir(parents=True, exist_ok=True)
    (wt_active / "gitdir").write_text(str(target_dir), encoding="utf-8")
    (wt_active / "locked").write_text("reason: stale dispatch\n", encoding="utf-8")

    # 2. Dangling worktree whose target path is missing
    wt_dangling = wt_root / "node_worker_ghost"
    wt_dangling.mkdir(parents=True, exist_ok=True)
    (wt_dangling / "gitdir").write_text(str(tmp_path / "nonexistent_station_path"), encoding="utf-8")

    report = prune_orphaned_worktrees_and_locks(repo, dry_run=False)
    assert report["has_deadlock_risk"] is True
    assert any("dangling_worktree:node_worker_ghost" in iss for iss in report["issues"])
    assert any("stale_worktree_locked:node_worker_1" in iss for iss in report["issues"])
    assert not wt_dangling.exists()
    assert not (wt_active / "locked").exists()


def test_req_fm_worktree_01_unsets_extensions_worktree_config(tmp_path: Path):
    """Verify extensions.worktreeConfig is unset to prevent repository format incompatibility (REQ-FM-WORKTREE-01)."""
    assert prune_orphaned_worktrees_and_locks is not None
    repo = _init_test_git_repo(tmp_path / "cfg_mesh_repo")
    subprocess.run(["git", "config", "extensions.worktreeConfig", "true"], cwd=str(repo), check=True, capture_output=True)

    report = prune_orphaned_worktrees_and_locks(repo, dry_run=False)
    assert "extensions_worktree_config_enabled" in report["issues"]
    assert "unset extensions.worktreeConfig" in report["actions_taken"]

    check_cfg = subprocess.run(["git", "config", "--get", "extensions.worktreeConfig"], cwd=str(repo), capture_output=True, text=True)
    assert check_cfg.stdout.strip() == ""


def test_req_fm_worktree_01_preflight_hook_before_dag_dispatch(tmp_path: Path):
    """Verify pre-flight cleanup runs prior to DAG workflow execution (REQ-FM-WORKTREE-01)."""
    assert prune_orphaned_worktrees_and_locks is not None
    repo = _init_test_git_repo(tmp_path / "dag_dispatch_repo")
    stale_lock = repo / ".git" / "index.lock"
    stale_lock.write_text("lock before dag\n", encoding="utf-8")

    # Pre-flight check
    preflight_rep = prune_orphaned_worktrees_and_locks(repo, dry_run=False)
    assert preflight_rep["healed"] is True
    assert not stale_lock.exists()

    # Now DAG dispatch proceeds cleanly without deadlock risk
    nodes = [
        StationNode(station_id="node_a", name="Node A", base_url="http://127.0.0.1:8001", capabilities=["task_a"]),
        StationNode(station_id="node_b", name="Node B", base_url="http://127.0.0.1:8002", capabilities=["task_b"]),
    ]
    orchestrator = FederationMeshOrchestrator(nodes=nodes, transport=lambda st, act, pay: {"result": "ok"})
    tasks = [
        WorkflowTask(task_id="t1", station_id="node_a", action="task_a"),
        WorkflowTask(task_id="t2", station_id="node_b", action="task_b", depends_on=["t1"]),
    ]
    wf_res = orchestrator.execute_cross_station_workflow("clean_dispatch_flow", tasks)
    assert wf_res["ok"] is True
    assert wf_res["status"] == "COMPLETED"
