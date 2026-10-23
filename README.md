# AGY Federation Mesh

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10+-emerald.svg)](pyproject.toml)
[![Architecture: Tier L5](https://img.shields.io/badge/Architecture-Tier%20L5%20Federation-purple.svg)](#architecture)

Tier L5 Federated Cross-Station Swarm Orchestrator and Distributed Directed Acyclic Graph (DAG) Execution Mesh.

---

## Architecture Overview

```mermaid
graph TD
    Mesh["Tier L5: AGY Federation Mesh Controller"]

    S1["Station L4: Mission Control<br/>Port 8000"]
    S2["Station L4: Flow Video Studio<br/>Port 8765"]
    S3["Station L4: PE Reversing Lab<br/>Port 8768"]
    S4["Station L4: Secret Shield<br/>Port 8770"]

    Mesh -->|Topology Discovery &amp; Health| S1
    Mesh -->|Veo Video Render Dispatch| S2
    Mesh -->|Static Binary Triage| S3
    Mesh -->|DPAPI Secret Quarantine| S4
```

1. **Station Catalog & Mesh Topology**: Discovers and tracks active Tier L4 stations, validating endpoints, capabilities, and computing a deterministic 64-character SHA-256 mesh topology digest.
2. **DAG Execution Engine**: Validates task dependencies, resolves topological execution sequences, and rejects cyclic dependencies.
3. **Federated Orchestrator**: Executes multi-station workflows with circuit breaker protection, halting execution on station failures and recording W3C PROV-DM provenance logs.
4. **Command Matrix Cockpit**: Dark-mode web interface and REST API for real-time monitoring and pipeline triggering.

---

## Quickstart

```bash
# Query active mesh topology
python -m agy_federation_mesh.cli topology --json

# Run composite cross-station pipeline
python -m agy_federation_mesh.cli run --workflow security_reversing_audit --json

# Launch Command Matrix Web Cockpit
uvicorn agy_federation_mesh.l4_app:app --port 8780 --reload
```

---

## Verification

```bash
pytest tests/test_federation_mesh.py -v
```

---

## License

MIT (c) 2026 Obscured-grey.
