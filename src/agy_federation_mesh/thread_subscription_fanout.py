"""Thread Subscription Fanout Hub, Reactive Idle-to-Working Wakeup & GraphQL Review Resolution.

Requirement Traceability:
- REQ-FM-SUB-01: Thread Subscription Hub, Reactive Idle-to-Working Wakeup & GraphQL Review Resolution
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ReviewThreadEvent:
    """Represents a GitHub pull request review thread event (REQ-FM-SUB-01)."""

    thread_node_id: str
    pr_number: int
    author: str
    body: str
    is_bot: bool = False


@dataclass
class TicketRecord:
    """State and worktree isolation record for a subscribed superagent ticket (REQ-FM-SUB-01)."""

    ticket_id: str
    short_name: str
    worktree_dir: str
    subscriptions: set[str] = field(default_factory=set)
    stage: str = "working"
    stopped_by_human: bool = False
    pr_number: int | None = None
    events: list[dict[str, Any]] = field(default_factory=list)


class ThreadSubscriptionHub:
    """Manages ticket worktree isolation, thread stream fanout, and review thread GraphQL mutations (REQ-FM-SUB-01)."""

    def __init__(self) -> None:
        """Initialize an empty ticket registry for subscription fanout (REQ-FM-SUB-01)."""
        self._tickets: dict[str, TicketRecord] = {}

    def create_ticket(
        self,
        ticket_id: str,
        short_name: str,
        base_work_dir: str = "/state",
        initial_thread: str | None = None,
        pr_number: int | None = None,
    ) -> TicketRecord:
        """Create a ticket with an isolated git worktree path and stream subscriptions (REQ-FM-SUB-01)."""
        normalized_base = base_work_dir.rstrip("/")
        worktree_dir = f"{normalized_base}/work/{short_name}"
        subs: set[str] = set()
        if initial_thread:
            subs.add(initial_thread)
        if pr_number is not None:
            subs.add(f"pr:{pr_number}")
            subs.add(f"pr:{pr_number}:reviews")
            subs.add(f"pr:{pr_number}:ci")

        record = TicketRecord(
            ticket_id=ticket_id,
            short_name=short_name,
            worktree_dir=worktree_dir,
            subscriptions=subs,
            stage="working",
            stopped_by_human=False,
            pr_number=pr_number,
        )
        self._tickets[ticket_id] = record
        return record

    def fanout_thread_event(
        self,
        stream: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Fan out a stream event to subscribed tickets and wake idle tickets reactively (REQ-FM-SUB-01)."""
        is_ci_stream = stream.endswith(":ci")
        if not is_ci_stream and bool(payload.get("is_bot", False)):
            return {
                "stream": stream,
                "delivered_to": [],
                "bypassed_eval": True,
                "ignored_bot": True,
            }

        if is_ci_stream:
            ci_status = str(payload.get("status", "")).lower()
            if ci_status and ci_status not in {"failure", "error", "timed_out", "cancelled"}:
                return {
                    "stream": stream,
                    "delivered_to": [],
                    "bypassed_eval": True,
                }

        delivered_to: list[str] = []
        for ticket in self._tickets.values():
            if stream not in ticket.subscriptions:
                continue
            if ticket.stopped_by_human or ticket.stage == "ended":
                continue
            ticket.events.append(dict(payload))
            if ticket.stage == "idle":
                ticket.stage = "working"
            delivered_to.append(ticket.ticket_id)

        return {
            "stream": stream,
            "delivered_to": delivered_to,
            "bypassed_eval": True,
        }

    def build_resolve_review_thread_graphql(
        self,
        thread_node_id: str,
    ) -> dict[str, Any]:
        """Construct the GitHub GraphQL resolveReviewThread mutation payload (REQ-FM-SUB-01)."""
        query = (
            "mutation ResolveReviewThread($threadId: ID!) {\n"
            "  resolveReviewThread(input: {threadId: $threadId}) {\n"
            "    thread {\n"
            "      id\n"
            "      isResolved\n"
            "    }\n"
            "  }\n"
            "}"
        )
        return {
            "query": query,
            "variables": {"threadId": thread_node_id},
        }
