"""Locked Agile V verification suite for WebRTC Swarm Federation.

Requirement Traceability:
- REQ-FM-WEBRTC-01: Decentralized P2P Agent Swarm Federation over WebRTC DataChannels
  with capability discovery, HMAC-SHA256 ICE candidate signing, CRDT/versioned LSP
  symbol index synchronization, and 2/3 majority patch quorum consensus.

Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

import copy
from pathlib import Path
import re
import sys
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from agy_federation_mesh.webrtc_swarm_federation import (  # noqa: E402
    FederatedPeerNode,
    SubSwarmSession,
    WebRTCSwarmFederation,
)


def _extract_symbol_entry(merged: Any, file_path: str, sym_name: str) -> dict[str, Any]:
    """Resolve symbol record from dict-keyed or list-based merged index."""
    if isinstance(merged, dict):
        if (file_path, sym_name) in merged:
            return merged[(file_path, sym_name)]
        if f"{file_path}:{sym_name}" in merged:
            return merged[f"{file_path}:{sym_name}"]
        if "symbols" in merged:
            return _extract_symbol_entry(merged["symbols"], file_path, sym_name)
        for val in merged.values():
            if isinstance(val, dict) and val.get("file") == file_path and val.get("name", "").strip() == sym_name.strip():
                return val
    if isinstance(merged, list):
        for item in merged:
            if isinstance(item, dict) and item.get("file") == file_path and item.get("name", "").strip() == sym_name.strip():
                return item
    raise KeyError(f"Symbol {sym_name} in {file_path} not found in {merged!r}")


def test_register_peers_and_assemble_capability_sub_swarm() -> None:
    """Verify peer registration, capability filtering, and lowest-latency leader election (REQ-FM-WEBRTC-01)."""
    fed = WebRTCSwarmFederation(cluster_secret="test-secret")
    fed.register_peer(FederatedPeerNode("node-a", "v=0\r\no=node-a 1 1 IN IP4 10.0.0.1", ["ast", "lsp", "git"], 28.5))
    fed.register_peer(FederatedPeerNode("node-b", "v=0\r\no=node-b 1 1 IN IP4 10.0.0.2", ["ast", "lsp"], 9.4))
    fed.register_peer(FederatedPeerNode("node-c", "v=0\r\no=node-c 1 1 IN IP4 10.0.0.3", ["vision"], 3.1))

    session = fed.assemble_sub_swarm(task_id="task-1", required_capabilities=["ast", "lsp"], min_peers=2)
    assert isinstance(session, SubSwarmSession)
    assert len(session.member_ids) >= 2
    assert "node-a" in session.member_ids
    assert "node-b" in session.member_ids
    assert "node-c" not in session.member_ids
    assert session.leader_id == "node-b"


def test_sign_and_verify_ice_candidate_envelope() -> None:
    """Verify HMAC-SHA256 signing and tamper rejection for ICE candidate envelopes (REQ-FM-WEBRTC-01)."""
    fed = WebRTCSwarmFederation(cluster_secret="test-secret-ice")
    env = fed.sign_ice_candidate_envelope(
        peer_id="node-a",
        sdp_mid="data0",
        candidate="candidate:1 1 UDP 2122260223 192.168.1.10 54321 typ host",
    )
    assert isinstance(env, dict)
    sig = env.get("signature") or env.get("hmac_signature")
    assert isinstance(sig, str) and len(sig) == 64
    assert fed.verify_ice_candidate_envelope(env) is True

    tampered = copy.deepcopy(env)
    tampered["candidate"] = "candidate:1 1 UDP 2122260223 10.99.99.99 54321 typ host"
    assert fed.verify_ice_candidate_envelope(tampered) is False


def test_exchange_and_merge_lsp_symbol_index() -> None:
    """Verify versioned LSP symbol index synchronization resolves highest version per (file, name) (REQ-FM-WEBRTC-01)."""
    fed = WebRTCSwarmFederation(cluster_secret="test-secret-lsp")
    fed.register_peer(FederatedPeerNode("node-a", "sdp-a", ["ast", "lsp"], 12.0))
    fed.register_peer(FederatedPeerNode("node-b", "sdp-b", ["ast", "lsp"], 15.0))
    session = fed.assemble_sub_swarm(task_id="task-lsp", required_capabilities=["lsp"], min_peers=2)

    fed.exchange_lsp_symbol_index(
        session.swarm_id,
        "node-a",
        symbols=[
            {"name": " foo", "file": "a.py", "line": 10, "version": 2},
            {"name": "bar", "file": "b.py", "line": 40, "version": 5},
        ],
    )
    merged = fed.exchange_lsp_symbol_index(
        session.swarm_id,
        "node-b",
        symbols=[
            {"name": " foo", "file": "a.py", "line": 22, "version": 4},
            {"name": "bar", "file": "b.py", "line": 12, "version": 1},
        ],
    )

    foo_entry = _extract_symbol_entry(merged, "a.py", "foo")
    bar_entry = _extract_symbol_entry(merged, "b.py", "bar")
    assert foo_entry["version"] == 4 and foo_entry["line"] == 22
    assert bar_entry["version"] == 5 and bar_entry["line"] == 40


def test_propose_and_reach_quorum_consensus_on_patch() -> None:
    """Verify 2/3 majority quorum consensus calculation and state recording on proposed patches (REQ-FM-WEBRTC-01)."""
    fed = WebRTCSwarmFederation(cluster_secret="test-secret-quorum")
    for idx, pid in enumerate(("node-a", "node-b", "node-c")):
        fed.register_peer(FederatedPeerNode(pid, f"sdp-{pid}", ["ast"], 10.0 + idx))
    session = fed.assemble_sub_swarm(task_id="task-q", required_capabilities=["ast"], min_peers=3)

    res_pass = fed.propose_consensus_patch(
        session.swarm_id, patch_id="p1", diff_digest="abc123", votes={"node-a": True, "node-b": True, "node-c": False}
    )
    quorum_ok = res_pass["quorum_reached"] if isinstance(res_pass, dict) else getattr(res_pass, "quorum_reached")
    assert quorum_ok is True

    res_fail = fed.propose_consensus_patch(
        session.swarm_id, patch_id="p2", diff_digest="def456", votes={"node-a": True, "node-b": False, "node-c": False}
    )
    quorum_fail = res_fail["quorum_reached"] if isinstance(res_fail, dict) else getattr(res_fail, "quorum_reached")
    assert quorum_fail is False


def test_sub_swarm_rejects_insufficient_capable_peers() -> None:
    """Verify assemble_sub_swarm raises ValueError when capable peers < min_peers (REQ-FM-WEBRTC-01)."""
    fed = WebRTCSwarmFederation(cluster_secret="test-secret-min")
    fed.register_peer(FederatedPeerNode("node-only-one", "sdp-1", ["ast", "lsp"], 10.0))
    with pytest.raises(ValueError):
        fed.assemble_sub_swarm(task_id="task-insufficient", required_capabilities=["ast", "lsp"], min_peers=2)


def test_req_fm_webrtc_01_traceability_and_english_hygiene() -> None:
    """Verify REQ-FM-WEBRTC-01 tag and 0 Cyrillic characters in implementation and tests (REQ-FM-WEBRTC-01)."""
    impl_path = SRC_DIR / "agy_federation_mesh" / "webrtc_swarm_federation.py"
    assert impl_path.is_file(), f"Implementation file missing: {impl_path}"
    impl_text = impl_path.read_text(encoding="utf-8")
    test_text = Path(__file__).read_text(encoding="utf-8")

    assert "REQ-FM-WEBRTC-01" in impl_text
    assert re.search(r"[\u0400-\u04FF]", impl_text) is None
    assert re.search(r"[\u0400-\u04FF]", test_text) is None
