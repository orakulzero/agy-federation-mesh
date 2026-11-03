"""
ISO 9001:2015 / Agile V Verification Suite — REQ-FM-ASTMERGE-01
Component: AST-Aware Three-Way Merge Driver (Python / JSON / Markdown)
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

import agy_federation_mesh
from agy_federation_mesh.ast_merge_driver import AstMergeDriver, merge_three_way


def test_req_fm_astmerge_01_python_non_overlapping_functions_and_imports_merge() -> None:
    """Verify REQ-FM-ASTMERGE-01 non-overlapping Python functions, imports, and comments merge cleanly."""
    assert getattr(agy_federation_mesh, "__v3_generation__", None) == "3.0.0"
    assert getattr(agy_federation_mesh, "AstMergeDriver", None) is AstMergeDriver
    assert getattr(agy_federation_mesh, "merge_three_way", None) is merge_three_way

    base_py = "# REQ-BASE-01\ndef alpha():\n    return 1\n"
    ours_py = (
        "import os\n"
        "# REQ-BASE-01\n"
        "def alpha():\n"
        "    return 1\n\n"
        "def beta():\n"
        "    # REQ-OURS-01\n"
        "    return 2\n"
    )
    theirs_py = (
        "import sys\n"
        "# REQ-BASE-01\n"
        "def alpha():\n"
        "    return 1\n\n"
        "def gamma():\n"
        "    # REQ-THEIRS-01\n"
        "    return 3\n"
    )

    res = merge_three_way(base_py, ours_py, theirs_py, file_format="py")
    assert res["requirement_id"] == "REQ-FM-ASTMERGE-01"
    assert res["status"] == "MERGED"
    assert res["conflicts"] == []
    assert "beta" in res["merged_symbols"]
    assert "gamma" in res["merged_symbols"]

    merged_code = res["merged_content"]
    tree = ast.parse(merged_code)
    fn_names = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    assert {"alpha", "beta", "gamma"}.issubset(fn_names)
    assert "# REQ-OURS-01" in merged_code
    assert "# REQ-THEIRS-01" in merged_code


def test_req_fm_astmerge_01_python_conflicting_function_edit_flagged() -> None:
    """Verify REQ-FM-ASTMERGE-01 flags conflicting edits to the same Python symbol."""
    base_py = "def compute(x):\n    return x + 1\n"
    ours_py = "def compute(x):\n    return x + 10\n"
    theirs_py = "def compute(x):\n    return x * 99\n"

    res = merge_three_way(base_py, ours_py, theirs_py, file_format="py")
    assert res["requirement_id"] == "REQ-FM-ASTMERGE-01"
    assert res["status"] == "CONFLICT"
    assert len(res["conflicts"]) == 1
    assert res["conflicts"][0]["symbol"] == "compute"


def test_req_fm_astmerge_01_json_and_markdown_three_way_merge() -> None:
    """Verify REQ-FM-ASTMERGE-01 merges JSON keys and Markdown sections cleanly."""
    driver = AstMergeDriver()

    base_json = json.dumps({"a": 1})
    ours_json = json.dumps({"a": 1, "b": 2})
    theirs_json = json.dumps({"a": 1, "c": 3})

    res_json_ok = driver.merge_three_way(base_json, ours_json, theirs_json, file_format="json")
    assert res_json_ok["requirement_id"] == "REQ-FM-ASTMERGE-01"
    assert res_json_ok["status"] == "MERGED"
    assert res_json_ok["conflicts"] == []
    assert json.loads(res_json_ok["merged_content"]) == {"a": 1, "b": 2, "c": 3}

    ours_json_conflict = json.dumps({"a": 10})
    theirs_json_conflict = json.dumps({"a": 20})
    res_json_conflict = driver.merge_three_way(
        base_json, ours_json_conflict, theirs_json_conflict, file_format="json"
    )
    assert res_json_conflict["status"] == "CONFLICT"
    assert len(res_json_conflict["conflicts"]) >= 1

    base_md = "# Title\n\n## Intro\nBase intro.\n"
    ours_md = "# Title\n\n## Intro\nBase intro.\n\n## Architecture\nOurs arch.\n"
    theirs_md = "# Title\n\n## Intro\nBase intro.\n\n## Verification\nTheirs verif.\n"

    res_md = driver.merge_three_way(base_md, ours_md, theirs_md, file_format="md")
    assert res_md["requirement_id"] == "REQ-FM-ASTMERGE-01"
    assert res_md["status"] == "MERGED"
    assert res_md["conflicts"] == []
    assert "## Architecture" in res_md["merged_content"]
    assert "## Verification" in res_md["merged_content"]
