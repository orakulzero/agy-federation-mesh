"""Locked Agile V (P30) Verification Suite for Line-Span Lock Oracle & Concurrent RangeEdit Sequencer.

Requirement Traceability:
- REQ-FM-LINELOCK-01: AST/Line-Span Lock Oracle & Bottom-Up Concurrent RangeEdit Sequencer (`line_span_lock_oracle.py`)

Quality Framework: ISO 9001:2015, ISO/IEC 42001:2023, Agile V Role Separation.
Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agy_federation_mesh.line_span_lock_oracle import (  # noqa: E402
    LineSpanLockOracle,
)


def _field(obj: Any, name: str) -> Any:
    """Extract field from dict or dataclass/object transparently."""
    return obj[name] if isinstance(obj, dict) else getattr(obj, name)


def test_acquire_and_release_non_overlapping_span_locks() -> None:
    """REQ-FM-LINELOCK-01: Verify non-overlapping span locks on the same file succeed with unique lck_ IDs and release cleanly."""
    oracle = LineSpanLockOracle()
    res1 = oracle.acquire_span_lock("src/app.py", 10, 20, "agent_1", margin_lines=2)
    res2 = oracle.acquire_span_lock("src/app.py", 30, 40, "agent_2", margin_lines=2)

    assert _field(res1, "acquired") is True
    assert _field(res2, "acquired") is True
    lock_id_1 = _field(res1, "lock_id")
    lock_id_2 = _field(res2, "lock_id")
    assert isinstance(lock_id_1, str) and lock_id_1.startswith("lck_")
    assert isinstance(lock_id_2, str) and lock_id_2.startswith("lck_")
    assert lock_id_1 != lock_id_2

    assert oracle.release_span_lock(lock_id_1) is True
    assert oracle.release_span_lock(lock_id_2) is True


def test_acquire_span_lock_detects_overlap_and_margin_collision() -> None:
    """REQ-FM-LINELOCK-01: Verify margin collision (effective zone 8..22 vs 21..30) blocks agent_2 with queue_rebase."""
    oracle = LineSpanLockOracle()
    res1 = oracle.acquire_span_lock("src/app.py", 10, 20, "agent_1", margin_lines=2)
    assert _field(res1, "acquired") is True

    res2 = oracle.acquire_span_lock("src/app.py", 21, 30, "agent_2", margin_lines=2)
    assert _field(res2, "acquired") is False
    assert _field(res2, "conflict_agent_id") == "agent_1"
    assert _field(res2, "resolution") == "queue_rebase"


def test_sequence_concurrent_range_edits_applies_bottom_up_without_drift() -> None:
    """REQ-FM-LINELOCK-01: Verify sequence_concurrent_range_edits applies non-overlapping edits bottom-up without line drift."""
    oracle = LineSpanLockOracle()
    original_content = "".join(f"line{i}\n" for i in range(1, 11))
    edit_a = {
        "agent_id": "agent_1",
        "start_line": 2,
        "end_line": 3,
        "replacement": "new2a\nnew2b\nnew2c\nnew3",
    }
    edit_b = {
        "agent_id": "agent_2",
        "start_line": 8,
        "end_line": 9,
        "replacement": "new8\nnew9",
    }

    res = oracle.sequence_concurrent_range_edits("src/app.py", original_content, [edit_a, edit_b])
    assert len(res["applied_edits"]) == 2
    assert res["conflicted_edits"] == []
    assert res["merged_content"].splitlines() == [
        "line1",
        "new2a",
        "new2b",
        "new2c",
        "new3",
        "line4",
        "line5",
        "line6",
        "line7",
        "new8",
        "new9",
        "line10",
    ]


def test_sequence_concurrent_range_edits_isolates_overlapping_conflicts() -> None:
    """REQ-FM-LINELOCK-01: Verify overlapping edits are isolated into conflicted_edits while non-overlapping edits apply."""
    oracle = LineSpanLockOracle()
    original_content = "".join(f"line{i}\n" for i in range(1, 45))
    edit_a = {"agent_id": "agent_1", "start_line": 10, "end_line": 20, "replacement": "replaced_10_20"}
    edit_b = {"agent_id": "agent_2", "start_line": 15, "end_line": 25, "replacement": "conflicting_15_25"}
    edit_c = {"agent_id": "agent_3", "start_line": 35, "end_line": 40, "replacement": "replaced_35_40"}

    res = oracle.sequence_concurrent_range_edits(
        "src/app.py",
        original_content,
        [edit_a, edit_b, edit_c],
    )
    assert len(res["applied_edits"]) == 2
    assert len(res["conflicted_edits"]) == 1
    assert "replaced_10_20" in res["merged_content"]
    assert "replaced_35_40" in res["merged_content"]
    assert "conflicting_15_25" not in res["merged_content"]


def test_same_agent_reentrant_or_different_file_locks_succeed() -> None:
    """REQ-FM-LINELOCK-01: Verify locks on different files do not collide and same-agent reentrant locks succeed."""
    oracle = LineSpanLockOracle()
    res_a = oracle.acquire_span_lock("src/file_a.py", 10, 20, "agent_1", margin_lines=2)
    res_b = oracle.acquire_span_lock("src/file_b.py", 10, 20, "agent_2", margin_lines=2)
    res_reentrant = oracle.acquire_span_lock("src/file_a.py", 12, 18, "agent_1", margin_lines=2)

    assert _field(res_a, "acquired") is True
    assert _field(res_b, "acquired") is True
    assert _field(res_reentrant, "acquired") is True
