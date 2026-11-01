"""Pre-Flight Git Worktree Pruner & Stale Lock Cleaner for Federation Mesh.

Traceability:
- REQ-FM-WORKTREE-01: Pre-flight orphaned git worktree pruner and stale lock cleaner before DAG node dispatch.

Quality Framework: ISO 9001:2015, ISO/IEC 42001:2023, Agile V.
Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
from typing import Any, Dict, List, Optional


def _run_git_cmd(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def prune_orphaned_worktrees_and_locks(
    repo_path: Path | str,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Diagnose and clean dangling worktrees, stale lockfiles, and invalid git configs (REQ-FM-WORKTREE-01)."""
    repo = Path(repo_path).resolve()
    git_dir = repo / ".git"
    issues: List[str] = []
    actions_taken: List[str] = []

    if not git_dir.is_dir():
        return {
            "repo_path": str(repo),
            "dry_run": dry_run,
            "has_deadlock_risk": False,
            "issues_count": 0,
            "issues": [],
            "healed": False,
            "actions_taken": [],
        }

    # 1. Stale .git/index.lock
    index_lock = git_dir / "index.lock"
    if index_lock.is_file():
        issues.append("stale_index_lock")
        if not dry_run:
            try:
                index_lock.unlink()
                actions_taken.append("removed stale .git/index.lock")
            except OSError:
                pass

    # 2. Inspect .git/worktrees
    wt_root = git_dir / "worktrees"
    if wt_root.is_dir():
        for wt_entry in list(wt_root.iterdir()):
            if not wt_entry.is_dir():
                continue

            # Check dangling gitdir
            gitdir_file = wt_entry / "gitdir"
            if gitdir_file.is_file():
                try:
                    target_str = gitdir_file.read_text(encoding="utf-8").strip()
                    if not Path(target_str).exists():
                        issues.append(f"dangling_worktree:{wt_entry.name}")
                        if not dry_run:
                            shutil.rmtree(wt_entry, ignore_errors=True)
                            actions_taken.append(f"pruned dangling worktree {wt_entry.name}")
                            continue
                except OSError:
                    pass

            # Check locked marker
            locked_marker = wt_entry / "locked"
            if locked_marker.is_file():
                issues.append(f"stale_worktree_locked:{wt_entry.name}")
                if not dry_run:
                    try:
                        locked_marker.unlink()
                        actions_taken.append(f"unlocked stale worktree {wt_entry.name}")
                    except OSError:
                        pass

            # Check individual lock files
            for lk in wt_entry.glob("*.lock"):
                issues.append(f"stale_worktree_lock_file:{wt_entry.name}:{lk.name}")
                if not dry_run:
                    try:
                        lk.unlink()
                        actions_taken.append(f"removed worktree lock file {lk.name}")
                    except OSError:
                        pass

    # 3. extensions.worktreeConfig
    wt_cfg = _run_git_cmd(repo, "config", "--get", "extensions.worktreeConfig").stdout.strip().lower()
    if wt_cfg == "true":
        issues.append("extensions_worktree_config_enabled")
        if not dry_run:
            _run_git_cmd(repo, "config", "--unset", "extensions.worktreeConfig")
            actions_taken.append("unset extensions.worktreeConfig")

    has_deadlock = len(issues) > 0
    healed = len(actions_taken) > 0

    return {
        "repo_path": str(repo),
        "dry_run": dry_run,
        "has_deadlock_risk": has_deadlock,
        "issues_count": len(issues),
        "issues": issues,
        "healed": healed,
        "actions_taken": actions_taken,
    }


def preflight_dag_dispatch_check(repo_path: Path | str | None = None) -> Dict[str, Any]:
    """Pre-flight hook running automated worktree and lock cleanup before DAG node dispatch (REQ-FM-WORKTREE-01)."""
    target = Path(repo_path).resolve() if repo_path else Path.cwd().resolve()
    return prune_orphaned_worktrees_and_locks(target, dry_run=False)


class WorktreePruner:
    """Automated worktree and git lock pruner for multi-station federation workflows (REQ-FM-WORKTREE-01)."""

    def __init__(self, repo_path: Path | str = ".") -> None:
        self.repo_path = Path(repo_path).resolve()

    def prune(self, dry_run: bool = False) -> Dict[str, Any]:
        return prune_orphaned_worktrees_and_locks(self.repo_path, dry_run=dry_run)

    def preflight_check(self) -> Dict[str, Any]:
        return preflight_dag_dispatch_check(self.repo_path)
