"""Decentralized P2P Agent Swarm Federation over WebRTC DataChannels (REQ-FM-WEBRTC-01).

Provides peer capability discovery, lowest-latency sub-swarm assembly, HMAC-SHA256
ICE candidate envelope authentication, CRDT/versioned LSP symbol index merging,
and 2/3 majority Byzantine-resilient patch quorum consensus.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
from typing import Any


@dataclass(frozen=True)
class FederatedPeerNode:
    """Descriptor for a federated P2P swarm worker over WebRTC (REQ-FM-WEBRTC-01)."""

    peer_id: str
    sdp_offer: str
    capabilities: list[str] = field(default_factory=list)
    latency_ms: float = 0.0


@dataclass
class SubSwarmSession:
    """Active capability-matched sub-swarm session state (REQ-FM-WEBRTC-01)."""

    swarm_id: str
    task_id: str
    member_ids: list[str]
    leader_id: str
    required_capabilities: list[str]


class WebRTCSwarmFederation:
    """Coordinator for P2P WebRTC swarm formation, LSP sync, and patch consensus (REQ-FM-WEBRTC-01)."""

    def __init__(self, cluster_secret: str) -> None:
        """Initialize federation coordinator with shared HMAC secret (REQ-FM-WEBRTC-01)."""
        if not cluster_secret:
            raise ValueError("cluster_secret must be non-empty (REQ-FM-WEBRTC-01)")
        self._cluster_secret = cluster_secret.encode("utf-8")
        self._peers: dict[str, FederatedPeerNode] = {}
        self._sessions: dict[str, SubSwarmSession] = {}
        self._symbol_indices: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
        self._patch_votes: dict[str, dict[str, dict[str, Any]]] = {}

    def register_peer(self, peer: FederatedPeerNode) -> None:
        """Register or update a federated WebRTC peer node (REQ-FM-WEBRTC-01)."""
        self._peers[peer.peer_id] = peer

    def assemble_sub_swarm(
        self,
        task_id: str,
        required_capabilities: list[str],
        min_peers: int = 1,
    ) -> SubSwarmSession:
        """Assemble a capability-filtered sub-swarm and elect lowest-latency leader (REQ-FM-WEBRTC-01)."""
        req_set = set(required_capabilities)
        capable = [
            peer
            for peer in self._peers.values()
            if req_set.issubset(set(peer.capabilities))
        ]
        if len(capable) < min_peers:
            raise ValueError(
                f"Insufficient capable peers for task {task_id!r}: "
                f"required {min_peers}, found {len(capable)} (REQ-FM-WEBRTC-01)"
            )
        capable.sort(key=lambda p: (float(p.latency_ms), p.peer_id))
        swarm_id = f"swarm-{task_id}"
        session = SubSwarmSession(
            swarm_id=swarm_id,
            task_id=task_id,
            member_ids=[p.peer_id for p in capable],
            leader_id=capable[0].peer_id,
            required_capabilities=list(required_capabilities),
        )
        self._sessions[swarm_id] = session
        self._symbol_indices.setdefault(swarm_id, {})
        self._patch_votes.setdefault(swarm_id, {})
        return session

    def _compute_ice_hmac(self, peer_id: str, sdp_mid: str, candidate: str) -> str:
        """Compute canonical HMAC-SHA256 signature for an ICE candidate tuple (REQ-FM-WEBRTC-01)."""
        message = f"{peer_id}|{sdp_mid}|{candidate}".encode("utf-8")
        return hmac.new(self._cluster_secret, message, hashlib.sha256).hexdigest()

    def sign_ice_candidate_envelope(
        self,
        peer_id: str,
        sdp_mid: str,
        candidate: str,
    ) -> dict[str, Any]:
        """Sign an ICE candidate signaling envelope with HMAC-SHA256 (REQ-FM-WEBRTC-01)."""
        sig = self._compute_ice_hmac(peer_id, sdp_mid, candidate)
        return {
            "peer_id": peer_id,
            "sdp_mid": sdp_mid,
            "candidate": candidate,
            "signature": sig,
            "hmac_signature": sig,
        }

    def verify_ice_candidate_envelope(self, envelope: dict[str, Any]) -> bool:
        """Verify HMAC-SHA256 authenticity of an ICE candidate envelope (REQ-FM-WEBRTC-01)."""
        peer_id = str(envelope.get("peer_id", ""))
        sdp_mid = str(envelope.get("sdp_mid", ""))
        candidate = str(envelope.get("candidate", ""))
        sig = envelope.get("signature") or envelope.get("hmac_signature")
        if not isinstance(sig, str) or len(sig) != 64:
            return False
        expected = self._compute_ice_hmac(peer_id, sdp_mid, candidate)
        return hmac.compare_digest(sig, expected)

    def exchange_lsp_symbol_index(
        self,
        swarm_id: str,
        peer_id: str,
        symbols: list[dict[str, Any]],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Synchronize versioned LSP symbols, keeping highest version per (file, name) (REQ-FM-WEBRTC-01)."""
        index = self._symbol_indices.setdefault(swarm_id, {})
        for sym in symbols:
            file_path = str(sym.get("file", "")).strip()
            sym_name = str(sym.get("name", "")).strip()
            version = int(sym.get("version", 0))
            key = (file_path, sym_name)
            existing = index.get(key)
            if existing is None or version > int(existing.get("version", -1)):
                index[key] = {
                    **sym,
                    "file": file_path,
                    "name": sym_name,
                    "version": version,
                    "origin_peer_id": peer_id,
                }
        return dict(index)

    def propose_consensus_patch(
        self,
        swarm_id: str,
        patch_id: str,
        diff_digest: str,
        votes: dict[str, bool],
    ) -> dict[str, Any]:
        """Evaluate 2/3 majority quorum consensus on a proposed code patch (REQ-FM-WEBRTC-01)."""
        total_votes = len(votes)
        approvals = sum(1 for approved in votes.values() if approved)
        quorum_reached = total_votes > 0 and (approvals * 3 >= total_votes * 2)
        record = {
            "swarm_id": swarm_id,
            "patch_id": patch_id,
            "diff_digest": diff_digest,
            "approvals": approvals,
            "total_votes": total_votes,
            "quorum_reached": quorum_reached,
        }
        self._patch_votes.setdefault(swarm_id, {})[patch_id] = record
        return record
