"""Transactional Line-Span Lock Oracle & Bottom-Up RangeEdit Sequencer (REQ-FM-LINELOCK-01).

Prevents concurrent multi-agent edit collisions on shared files by enforcing
margin-aware line-span locks and sequencing non-overlapping RangeEdits in
bottom-up line order so earlier line expansions never shift downstream spans.

Requirement Traceability:
- REQ-FM-LINELOCK-01: AST/Line-Span Lock Oracle & Bottom-Up Concurrent RangeEdit Sequencer
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from typing import Any


class LineSpanLockOracle:
    """Manages per-file line-span locks with safety margins and sequences bottom-up RangeEdits (REQ-FM-LINELOCK-01)."""

    def __init__(self) -> None:
        """Initialize empty lock table for concurrent swarm agents (REQ-FM-LINELOCK-01)."""
        self._locks: dict[str, dict[str, Any]] = {}

    def acquire_span_lock(
        self,
        file_path: str,
        start_line: int,
        end_line: int,
        agent_id: str,
        margin_lines: int = 2,
    ) -> dict[str, Any]:
        """Attempt to acquire a margin-guarded line-span lock on file_path for agent_id (REQ-FM-LINELOCK-01)."""
        s_line = int(start_line)
        e_line = int(end_line)
        margin = max(0, int(margin_lines))
        eff_start = max(1, s_line - margin)
        eff_end = e_line + margin

        for existing in self._locks.values():
            if existing["file_path"] != file_path:
                continue
            if existing["agent_id"] == agent_id:
                continue
            # Check if requested span collides with existing lock's guarded zone
            if s_line <= existing["effective_end"] and e_line >= existing["effective_start"]:
                return {
                    "acquired": False,
                    "lock_id": None,
                    "file_path": file_path,
                    "start_line": s_line,
                    "end_line": e_line,
                    "agent_id": agent_id,
                    "conflict_agent_id": existing["agent_id"],
                    "resolution": "queue_rebase",
                }

        lock_id = f"lck_{uuid.uuid4().hex[:12]}"
        record = {
            "lock_id": lock_id,
            "file_path": file_path,
            "start_line": s_line,
            "end_line": e_line,
            "effective_start": eff_start,
            "effective_end": eff_end,
            "agent_id": agent_id,
            "margin_lines": margin,
        }
        self._locks[lock_id] = record
        return {
            "acquired": True,
            "lock_id": lock_id,
            "file_path": file_path,
            "start_line": s_line,
            "end_line": e_line,
            "effective_start": eff_start,
            "effective_end": eff_end,
            "agent_id": agent_id,
            "conflict_agent_id": None,
            "resolution": "acquired",
        }

    def release_span_lock(self, lock_id: str) -> bool:
        """Release an active line-span lock by its lock_id (REQ-FM-LINELOCK-01)."""
        return self._locks.pop(lock_id, None) is not None

    def sequence_concurrent_range_edits(
        self,
        file_path: str,
        original_content: str,
        edits: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Isolate overlapping edits and apply non-overlapping edits bottom-up without line drift (REQ-FM-LINELOCK-01)."""
        applied_edits: list[dict[str, Any]] = []
        conflicted_edits: list[dict[str, Any]] = []

        for raw_edit in edits:
            edit = dict(raw_edit)
            s_line = int(edit["start_line"])
            e_line = int(edit["end_line"])
            has_conflict = False
            for accepted in applied_edits:
                acc_start = int(accepted["start_line"])
                acc_end = int(accepted["end_line"])
                if max(s_line, acc_start) <= min(e_line, acc_end):
                    has_conflict = True
                    break
            if has_conflict:
                conflicted_edits.append(edit)
            else:
                applied_edits.append(edit)

        lines = original_content.splitlines()
        trailing_newline = original_content.endswith("\n")

        bottom_up_edits = sorted(
            applied_edits,
            key=lambda item: (int(item["start_line"]), int(item["end_line"])),
            reverse=True,
        )
        for edit in bottom_up_edits:
            start_idx = max(0, int(edit["start_line"]) - 1)
            end_idx = int(edit["end_line"])
            replacement_lines = str(edit["replacement"]).splitlines()
            lines[start_idx:end_idx] = replacement_lines

        merged_content = "\n".join(lines)
        if trailing_newline and merged_content:
            merged_content += "\n"

        return {
            "file_path": file_path,
            "applied_edits": applied_edits,
            "conflicted_edits": conflicted_edits,
            "merged_content": merged_content,
        }
