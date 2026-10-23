---
name: federation-mesh
description: Autonomous cross-station DAG workflow orchestrator, node discovery mesh, and distributed task routing pipeline.
---

# AGY Federation Mesh Skill

Coordinates distributed execution across Tier L4 workstations:
- **AGY Mission Control**: Central release oversight, bus telemetry, and brain memory search.
- **Flow Video Studio**: Veo multi-scene educational lesson production.
- **PE Reversing Lab**: Static PE32+ binary triage, entropy scanning, and YARA rule generation.
- **AGY Secret Shield**: Windows DPAPI secret quarantine and cryptographic cleanliness audit.

## Architecture

1. **Station Discovery**: Dynamically queries station ports and builds a 64-character SHA-256 topology digest.
2. **DAG Validation**: Validates cross-station dependencies and eliminates circular execution paths.
3. **Execution**: Dispatches tasks sequentially or in parallel with automatic failure-halting.
4. **Provenance**: Records every step into a W3C PROV-DM compatible execution ledger.

## CLI Usage

```bash
# Query active mesh topology
python -m agy_federation_mesh.cli topology --json

# Run composite cross-station pipeline
python -m agy_federation_mesh.cli run --workflow security_reversing_audit --json
```
