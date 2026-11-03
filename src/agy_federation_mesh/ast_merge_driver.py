"""AST-Aware Three-Way Merge Driver for Python, JSON, and Markdown (REQ-FM-ASTMERGE-01).

Provides structural three-way merging across Python AST symbols, JSON keys, and
Markdown heading sections while preserving inline comments and flagging true
symbol-level conflicts.

Requirement Traceability:
- REQ-FM-ASTMERGE-01: AST-Aware Three-Way Merge Driver
"""

from __future__ import annotations

import ast
import json
from typing import Any

_MISSING = object()


def _extract_python_units(source: str) -> tuple[list[str], dict[str, str], list[str]]:
    """Extract ordered imports and top-level symbol source slices from Python code (REQ-FM-ASTMERGE-01)."""
    lines = source.splitlines()
    tree = ast.parse(source)
    imports: list[str] = []
    symbols: dict[str, str] = {}
    order: list[str] = []

    for node in tree.body:
        start_line = node.lineno
        if getattr(node, "decorator_list", None):
            start_line = min(start_line, *(d.lineno for d in node.decorator_list))
        end_line = getattr(node, "end_lineno", start_line) or start_line

        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imp_slice = "\n".join(lines[start_line - 1 : end_line]).strip()
            if imp_slice and imp_slice not in imports:
                imports.append(imp_slice)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            comment_start = start_line
            while comment_start > 1 and lines[comment_start - 2].strip().startswith("#"):
                comment_start -= 1
            sym_slice = "\n".join(lines[comment_start - 1 : end_line]).rstrip()
            symbols[node.name] = sym_slice
            if node.name not in order:
                order.append(node.name)
    return imports, symbols, order


def _merge_ordered_map(
    base_map: dict[str, Any],
    ours_map: dict[str, Any],
    theirs_map: dict[str, Any],
    key_order: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Perform three-way structural merge over an ordered mapping (REQ-FM-ASTMERGE-01)."""
    merged: dict[str, Any] = {}
    conflicts: list[dict[str, Any]] = []

    for key in key_order:
        b_val = base_map.get(key, _MISSING)
        o_val = ours_map.get(key, _MISSING)
        t_val = theirs_map.get(key, _MISSING)

        if o_val == t_val:
            if o_val is not _MISSING:
                merged[key] = o_val
        elif o_val == b_val:
            if t_val is not _MISSING:
                merged[key] = t_val
        elif t_val == b_val:
            if o_val is not _MISSING:
                merged[key] = o_val
        else:
            conflicts.append(
                {
                    "symbol": key,
                    "base": None if b_val is _MISSING else b_val,
                    "ours": None if o_val is _MISSING else o_val,
                    "theirs": None if t_val is _MISSING else t_val,
                }
            )
    return merged, conflicts


def _parse_markdown_sections(text: str) -> tuple[dict[str, str], list[str]]:
    """Split Markdown document into heading-keyed sections (REQ-FM-ASTMERGE-01)."""
    sections: dict[str, str] = {}
    order: list[str] = []
    current_key = "__preamble__"
    buf: list[str] = []

    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            if buf:
                content = "\n".join(buf).strip()
                if content:
                    sections[current_key] = content
                    if current_key not in order:
                        order.append(current_key)
            current_key = line.strip()
            buf = [line]
        else:
            buf.append(line)

    if buf:
        content = "\n".join(buf).strip()
        if content:
            sections[current_key] = content
            if current_key not in order:
                order.append(current_key)
    return sections, order


class AstMergeDriver:
    """Structural three-way merge driver for Python, JSON, and Markdown files (REQ-FM-ASTMERGE-01)."""

    def merge_three_way(
        self,
        base_text: str,
        ours_text: str,
        theirs_text: str,
        file_format: str = "py",
    ) -> dict[str, Any]:
        """Merge base, ours, and theirs texts according to file_format (REQ-FM-ASTMERGE-01)."""
        fmt = file_format.lower().lstrip(".")
        if fmt in ("py", "python"):
            b_imp, b_sym, b_ord = _extract_python_units(base_text)
            o_imp, o_sym, o_ord = _extract_python_units(ours_text)
            t_imp, t_sym, t_ord = _extract_python_units(theirs_text)

            merged_imports: list[str] = []
            for imp in b_imp + o_imp + t_imp:
                if imp in b_imp and (imp not in o_imp or imp not in t_imp):
                    continue
                if imp not in merged_imports:
                    merged_imports.append(imp)

            all_keys = list(dict.fromkeys(b_ord + o_ord + t_ord))
            merged_map, conflicts = _merge_ordered_map(b_sym, o_sym, t_sym, all_keys)
            blocks: list[str] = []
            if merged_imports:
                blocks.append("\n".join(merged_imports))
            blocks.extend(merged_map.values())
            merged_content = ("\n\n".join(blocks) + "\n") if blocks else ""
            return {
                "requirement_id": "REQ-FM-ASTMERGE-01",
                "status": "CONFLICT" if conflicts else "MERGED",
                "conflicts": conflicts,
                "merged_symbols": list(merged_map.keys()),
                "merged_content": merged_content,
            }

        if fmt == "json":
            b_obj = json.loads(base_text) if base_text.strip() else {}
            o_obj = json.loads(ours_text) if ours_text.strip() else {}
            t_obj = json.loads(theirs_text) if theirs_text.strip() else {}
            all_keys = list(dict.fromkeys(list(b_obj.keys()) + list(o_obj.keys()) + list(t_obj.keys())))
            merged_map, conflicts = _merge_ordered_map(b_obj, o_obj, t_obj, all_keys)
            return {
                "requirement_id": "REQ-FM-ASTMERGE-01",
                "status": "CONFLICT" if conflicts else "MERGED",
                "conflicts": conflicts,
                "merged_symbols": list(merged_map.keys()),
                "merged_content": json.dumps(merged_map, indent=2) if not conflicts else "",
            }

        if fmt in ("md", "markdown"):
            b_sec, b_ord = _parse_markdown_sections(base_text)
            o_sec, o_ord = _parse_markdown_sections(ours_text)
            t_sec, t_ord = _parse_markdown_sections(theirs_text)
            all_keys = list(dict.fromkeys(b_ord + o_ord + t_ord))
            merged_map, conflicts = _merge_ordered_map(b_sec, o_sec, t_sec, all_keys)
            merged_content = ("\n\n".join(merged_map.values()) + "\n") if merged_map else ""
            return {
                "requirement_id": "REQ-FM-ASTMERGE-01",
                "status": "CONFLICT" if conflicts else "MERGED",
                "conflicts": conflicts,
                "merged_symbols": list(merged_map.keys()),
                "merged_content": merged_content,
            }

        raise ValueError(f"Unsupported file_format for AST merge: {file_format}")


def merge_three_way(
    base_text: str,
    ours_text: str,
    theirs_text: str,
    file_format: str = "py",
) -> dict[str, Any]:
    """Convenience wrapper executing AstMergeDriver.merge_three_way (REQ-FM-ASTMERGE-01)."""
    return AstMergeDriver().merge_three_way(
        base_text=base_text,
        ours_text=ours_text,
        theirs_text=theirs_text,
        file_format=file_format,
    )
