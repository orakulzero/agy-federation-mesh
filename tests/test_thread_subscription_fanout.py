"""Locked Agile V (P30) Verification Suite for Thread Subscription Fanout & PR Review Loop.

Requirement Traceability:
- REQ-FM-SUB-01: Thread Subscription Hub, Reactive Idle-to-Working Wakeup & GraphQL Review Resolution

Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agy_federation_mesh.thread_subscription_fanout import (  # noqa: E402
    ReviewThreadEvent,
    ThreadSubscriptionHub,
    TicketRecord,
)


def test_create_ticket_isolates_worktree_and_subscribes_initial_thread() -> None:
    """REQ-FM-SUB-01: Verify create_ticket isolates worktree_dir and subscribes PR/issue streams."""
    hub = ThreadSubscriptionHub()
    tkt = hub.create_ticket(
        ticket_id="tkt_101",
        short_name="fix-auth",
        base_work_dir="/state",
        initial_thread="issue:101",
        pr_number=55,
    )
    assert isinstance(tkt, TicketRecord)
    assert tkt.worktree_dir == "/state/work/fix-auth"
    for stream in ("issue:101", "pr:55", "pr:55:reviews", "pr:55:ci"):
        assert stream in tkt.subscriptions


def test_fanout_wakes_idle_ticket_on_pr_review_comment_or_ci_failure() -> None:
    """REQ-FM-SUB-01: Verify fanout routes review comment to idle ticket and wakes it without eval."""
    hub = ThreadSubscriptionHub()
    tkt = hub.create_ticket(
        ticket_id="tkt_101",
        short_name="fix-auth",
        base_work_dir="/state",
        initial_thread="issue:101",
        pr_number=55,
    )
    tkt.stage = "idle"
    res = hub.fanout_thread_event(
        "pr:55:reviews",
        {
            "comment_id": 9001,
            "author": "reviewer",
            "is_bot": False,
            "body": "Fix line 42",
        },
    )
    assert res["delivered_to"] == ["tkt_101"]
    assert tkt.stage == "working"
    assert res["bypassed_eval"] is True


def test_stopped_by_human_ticket_not_woken_by_fanout() -> None:
    """REQ-FM-SUB-01: Verify stopped_by_human ticket stays ended and is not woken by fanout."""
    hub = ThreadSubscriptionHub()
    tkt = hub.create_ticket(
        ticket_id="tkt_102",
        short_name="aborted-feat",
        base_work_dir="/state",
        initial_thread="issue:102",
        pr_number=56,
    )
    tkt.stopped_by_human = True
    tkt.stage = "ended"

    res = hub.fanout_thread_event(
        "pr:56:reviews",
        {
            "comment_id": 9002,
            "author": "reviewer",
            "is_bot": False,
            "body": "Another comment",
        },
    )
    assert "tkt_102" not in res["delivered_to"]
    assert tkt.stage == "ended"


def test_bot_events_ignored_during_fanout_unless_ci_check() -> None:
    """REQ-FM-SUB-01: Verify bot conversation comments are ignored while CI failure wakes idle ticket."""
    hub = ThreadSubscriptionHub()
    tkt = hub.create_ticket(
        ticket_id="tkt_103",
        short_name="ci-fix",
        base_work_dir="/state",
        initial_thread="issue:103",
        pr_number=55,
    )
    tkt.stage = "idle"

    res_bot = hub.fanout_thread_event(
        "pr:55",
        {
            "comment_id": 9003,
            "author": "codecov[bot]",
            "is_bot": True,
            "body": "Coverage report",
        },
    )
    assert res_bot["delivered_to"] == []
    assert tkt.stage == "idle"

    res_ci = hub.fanout_thread_event(
        "pr:55:ci",
        {
            "status": "failure",
            "check_name": "pytest",
        },
    )
    assert res_ci["delivered_to"] == ["tkt_103"]
    assert tkt.stage == "working"


def test_build_resolve_review_thread_graphql_mutation() -> None:
    """REQ-FM-SUB-01: Verify GraphQL resolveReviewThread mutation payload structure."""
    hub = ThreadSubscriptionHub()
    evt = ReviewThreadEvent(
        thread_node_id="PRRT_kwDO12345",
        pr_number=55,
        author="reviewer",
        body="Nitpick resolved",
    )
    assert evt.thread_node_id == "PRRT_kwDO12345"
    q = hub.build_resolve_review_thread_graphql("PRRT_kwDO12345")
    assert isinstance(q, dict)
    assert "resolveReviewThread" in q["query"]
    assert q["variables"] == {"threadId": "PRRT_kwDO12345"}


def test_req_fm_sub_01_traceability_and_english_hygiene() -> None:
    """REQ-FM-SUB-01: Verify thread_subscription_fanout.py contains REQ-FM-SUB-01 and 0 Cyrillic chars."""
    target_file = SRC_ROOT / "agy_federation_mesh" / "thread_subscription_fanout.py"
    assert target_file.is_file(), f"Missing target module: {target_file}"
    content = target_file.read_text(encoding="utf-8")
    assert "REQ-FM-SUB-01" in content
    for ch in content:
        code = ord(ch)
        assert not (0x0400 <= code <= 0x04FF), f"Cyrillic character found: {ch!r}"
