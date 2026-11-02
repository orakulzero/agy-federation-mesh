"""Locked Agile V (P30) Verification Suite for Multi-Repository Dependency Graph & Cross-Repo API Repair.

Requirement Traceability:
- REQ-FM-DEPGRAPH-01: Multi-Repo Dependency Graph & Cross-Repo Breaking Change Repair (`multi_repo_dependency_repair.py`)

Quality Framework: ISO 9001:2015, ISO/IEC 42001:2023, Agile V Role Separation.
Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from agy_federation_mesh.multi_repo_dependency_repair import (  # noqa: E402
    BreakingSignatureChange,
    MultiRepoDependencyRepair,
)


def test_detect_breaking_signature_changes_between_versions() -> None:
    """REQ-FM-DEPGRAPH-01: Verify AST signature comparison detects added required parameters."""
    rep = MultiRepoDependencyRepair()
    before_code = "def connect(host: str, port: int = 8000) -> bool:\n    return True\n"
    after_code = "def connect(host: str, port: int, tls: bool) -> bool:\n    return True\n"

    changes = rep.detect_breaking_signature_changes(
        "agy_golden_stack",
        before_code,
        after_code,
    )
    assert len(changes) == 1
    expected = BreakingSignatureChange(
        module_name="agy_golden_stack",
        symbol_name="connect",
        old_params=["host", "port"],
        new_params=["host", "port", "tls"],
        required_added=["tls"],
        removed_params=[],
    )
    assert changes[0] == expected


def test_non_breaking_optional_parameter_addition_ignored() -> None:
    """REQ-FM-DEPGRAPH-01: Verify adding an optional parameter with a default value does not trigger a breaking change."""
    rep = MultiRepoDependencyRepair()
    before_code = "def connect(host: str, port: int = 8000) -> bool:\n    return True\n"
    after_code = "def connect(host: str, port: int = 8000, timeout: float = 5.0) -> bool:\n    return True\n"

    changes = rep.detect_breaking_signature_changes(
        "agy_golden_stack",
        before_code,
        after_code,
    )
    assert changes == []


def test_scan_dependent_repos_finds_impacted_call_sites() -> None:
    """REQ-FM-DEPGRAPH-01: Verify build_cross_repo_repair_plan locates downstream call sites needing repair."""
    rep = MultiRepoDependencyRepair()
    before_code = "def connect(host: str, port: int = 8000) -> bool:\n    return True\n"
    after_code = "def connect(host: str, port: int, tls: bool) -> bool:\n    return True\n"
    changes = rep.detect_breaking_signature_changes("agy_golden_stack", before_code, after_code)

    dependent_files = {
        "agy-doctor-mux": {
            "mux.py": "from agy_golden_stack import connect\nres = connect('localhost', 8080)\n",
        }
    }
    plan = rep.build_cross_repo_repair_plan(changes, dependent_files)
    assert "agy-doctor-mux" in plan["impacted_repos"]
    sites = plan["impacted_repos"]["agy-doctor-mux"]
    assert len(sites) == 1
    assert sites[0]["file"] == "mux.py"
    assert sites[0]["line"] == 2
    assert sites[0]["symbol"] == "connect"
    assert "tls" in sites[0]["missing_required_args"]


def test_unimpacted_repos_excluded_from_repair_plan() -> None:
    """REQ-FM-DEPGRAPH-01: Verify repositories that do not call the changed symbol are omitted from impacted_repos."""
    rep = MultiRepoDependencyRepair()
    changes = [
        BreakingSignatureChange(
            module_name="agy_golden_stack",
            symbol_name="connect",
            old_params=["host", "port"],
            new_params=["host", "port", "tls"],
            required_added=["tls"],
            removed_params=[],
        )
    ]
    dependent_files = {
        "clean-repo": {
            "app.py": "import math\nx = math.sqrt(16)\n",
        }
    }
    plan = rep.build_cross_repo_repair_plan(changes, dependent_files)
    assert "clean-repo" not in plan["impacted_repos"]


def test_generate_paired_pr_manifest() -> None:
    """REQ-FM-DEPGRAPH-01: Verify generate_paired_pr_manifest orders upstream PR first followed by linked downstream PRs."""
    rep = MultiRepoDependencyRepair()
    changes = [
        BreakingSignatureChange(
            module_name="agy_golden_stack",
            symbol_name="connect",
            old_params=["host", "port"],
            new_params=["host", "port", "tls"],
            required_added=["tls"],
            removed_params=[],
        )
    ]
    dependent_files = {
        "agy-doctor-mux": {
            "mux.py": "from agy_golden_stack import connect\nres = connect('localhost', 8080)\n",
        }
    }
    plan = rep.build_cross_repo_repair_plan(changes, dependent_files)
    manifest = rep.generate_paired_pr_manifest(plan)
    assert len(manifest) == 2
    assert manifest[0]["role"] == "upstream"
    assert manifest[0]["repo"] == "agy_golden_stack"
    assert manifest[1]["role"] == "downstream"
    assert manifest[1]["repo"] == "agy-doctor-mux"
    assert manifest[1]["depends_on_repo"] == "agy_golden_stack"


def test_req_fm_depgraph_01_traceability_and_english_hygiene() -> None:
    """REQ-FM-DEPGRAPH-01: Verify multi_repo_dependency_repair.py contains REQ-FM-DEPGRAPH-01 and 0 Cyrillic chars."""
    target_file = SRC_ROOT / "agy_federation_mesh" / "multi_repo_dependency_repair.py"
    assert target_file.is_file(), f"Missing target module: {target_file}"
    content = target_file.read_text(encoding="utf-8")
    assert "REQ-FM-DEPGRAPH-01" in content
    for ch in content:
        code = ord(ch)
        assert not (0x0400 <= code <= 0x04FF), f"Cyrillic character found: {ch!r}"
