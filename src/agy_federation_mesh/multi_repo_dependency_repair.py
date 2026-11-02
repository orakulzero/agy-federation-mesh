"""Multi-Repository Dependency Graph & Cross-Repo Breaking Change Repair (REQ-FM-DEPGRAPH-01).

Detects breaking Python function/method signature mutations across upstream
modules, scans downstream repositories for impacted call sites via AST analysis,
and synthesizes ordered upstream/downstream paired pull request manifests.

Requirement Traceability:
- REQ-FM-DEPGRAPH-01: Multi-Repo Dependency Graph & Cross-Repo Breaking Change Repair
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class BreakingSignatureChange:
    """Represents a breaking signature change in an upstream callable symbol (REQ-FM-DEPGRAPH-01)."""

    module_name: str
    symbol_name: str
    old_params: List[str]
    new_params: List[str]
    required_added: List[str]
    removed_params: List[str]


class MultiRepoDependencyRepair:
    """Analyzes cross-repository API signature changes and plans downstream call-site repairs (REQ-FM-DEPGRAPH-01)."""

    @staticmethod
    def _extract_functions(source_code: str) -> Dict[str, Tuple[List[str], List[str]]]:
        """Return mapping of function_name -> (all_params, required_params)."""
        tree = ast.parse(source_code)
        funcs: Dict[str, Tuple[List[str], List[str]]] = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                raw_args = [
                    a.arg for a in node.args.args if a.arg not in ("self", "cls")
                ]
                num_defaults = len(node.args.defaults)
                req_cutoff = len(raw_args) - num_defaults
                required_args = raw_args[: max(0, req_cutoff)]
                funcs[node.name] = (raw_args, required_args)
        return funcs

    def detect_breaking_signature_changes(
        self,
        module_name: str,
        before_code: str,
        after_code: str,
    ) -> List[BreakingSignatureChange]:
        """Compare AST function signatures between versions and return breaking changes (REQ-FM-DEPGRAPH-01)."""
        before_funcs = self._extract_functions(before_code)
        after_funcs = self._extract_functions(after_code)
        changes: List[BreakingSignatureChange] = []

        for sym_name, (old_params, _old_req) in before_funcs.items():
            if sym_name not in after_funcs:
                continue
            new_params, new_req = after_funcs[sym_name]
            required_added = [p for p in new_req if p not in old_params]
            removed_params = [p for p in old_params if p not in new_params]
            if required_added or removed_params:
                changes.append(
                    BreakingSignatureChange(
                        module_name=module_name,
                        symbol_name=sym_name,
                        old_params=old_params,
                        new_params=new_params,
                        required_added=required_added,
                        removed_params=removed_params,
                    )
                )
        return changes

    def build_cross_repo_repair_plan(
        self,
        changes: List[BreakingSignatureChange],
        dependent_files: Dict[str, Dict[str, str]],
    ) -> Dict[str, Any]:
        """Scan dependent repositories and locate call sites impacted by breaking changes (REQ-FM-DEPGRAPH-01)."""
        changes_by_symbol = {c.symbol_name: c for c in changes}
        impacted_repos: Dict[str, List[Dict[str, Any]]] = {}

        for repo_name, files_map in dependent_files.items():
            repo_sites: List[Dict[str, Any]] = []
            for file_path, code in files_map.items():
                tree = ast.parse(code)
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    called_name = None
                    if isinstance(node.func, ast.Name):
                        called_name = node.func.id
                    elif isinstance(node.func, ast.Attribute):
                        called_name = node.func.attr

                    if called_name not in changes_by_symbol:
                        continue
                    change = changes_by_symbol[called_name]
                    pos_covered = set(change.new_params[: len(node.args)])
                    kw_covered = {kw.arg for kw in node.keywords if kw.arg}
                    covered = pos_covered | kw_covered
                    missing_req = [
                        p for p in change.required_added if p not in covered
                    ]
                    if missing_req or change.removed_params:
                        repo_sites.append(
                            {
                                "file": file_path,
                                "line": node.lineno,
                                "symbol": called_name,
                                "upstream_module": change.module_name,
                                "missing_required_args": missing_req,
                            }
                        )
            if repo_sites:
                impacted_repos[repo_name] = repo_sites

        return {
            "changes": changes,
            "impacted_repos": impacted_repos,
        }

    def generate_paired_pr_manifest(
        self,
        plan: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Generate ordered PR manifest with upstream PRs followed by linked downstream PRs (REQ-FM-DEPGRAPH-01)."""
        manifest: List[Dict[str, Any]] = []
        upstream_repos: List[str] = []
        for change in plan.get("changes", []):
            if change.module_name not in upstream_repos:
                upstream_repos.append(change.module_name)
                manifest.append(
                    {
                        "role": "upstream",
                        "repo": change.module_name,
                    }
                )

        primary_upstream = upstream_repos[0] if upstream_repos else ""
        for repo_name, sites in plan.get("impacted_repos", {}).items():
            manifest.append(
                {
                    "role": "downstream",
                    "repo": repo_name,
                    "depends_on_repo": primary_upstream,
                    "repair_sites": sites,
                }
            )
        return manifest
