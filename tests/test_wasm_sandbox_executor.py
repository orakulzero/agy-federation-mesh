"""Locked Agile V (P30) Verification Suite for WebAssembly (WASM) Sandboxed Task Executor.

Requirement Traceability:
- REQ-FM-WASM-01: Sub-Millisecond Sandboxed WebAssembly (WASM) Task Executor with
  WASI Capability Gates, Fuel Instruction Metering, and Memory Page Caps (`wasm_sandbox_executor.py`)

Hygiene: 100% English-only (0 Cyrillic characters).
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from agy_federation_mesh.wasm_sandbox_executor import (  # noqa: E402
    WasmSandboxConfig,
    WasmSandboxExecutor,
)


def _field(obj: Any, name: str) -> Any:
    """Extract field from dict or dataclass/object transparently."""
    if isinstance(obj, dict):
        return obj[name]
    return getattr(obj, name)


def test_validate_module_envelope_valid_wasm_and_wat() -> None:
    """REQ-FM-WASM-01: Verify validate_module_envelope accepts valid WASM binary header and WAT s-expression module."""
    executor = WasmSandboxExecutor()

    wasm_bytes = b"\x00asm\x01\x00\x00\x00\x01\x07\x01\x60\x02\x7f\x7f\x01\x7f"
    res_wasm = executor.validate_module_envelope(wasm_bytes)
    assert _field(res_wasm, "valid") is True
    assert _field(res_wasm, "format") == "wasm"
    assert _field(res_wasm, "violations") == []

    wat_source = '(module (func (export "compute") (param i32) (result i32) local.get 0))'
    res_wat = executor.validate_module_envelope(wat_source)
    assert _field(res_wat, "valid") is True
    assert _field(res_wat, "format") == "wat"
    assert _field(res_wat, "violations") == []


def test_validate_module_envelope_blocks_forbidden_host_imports() -> None:
    """REQ-FM-WASM-01: Verify validate_module_envelope rejects forbidden host imports when network/fs disabled."""
    cfg = WasmSandboxConfig(allow_network=False, allow_filesystem=False)
    executor = WasmSandboxExecutor(config=cfg)

    for forbidden in ("sock_open", "path_open", "proc_exit", "os.system"):
        malicious_wat = f'(module (import "wasi_snapshot_preview1" "{forbidden}" (func $f)))'
        res = executor.validate_module_envelope(malicious_wat)
        assert _field(res, "valid") is False
        violations = _field(res, "violations")
        assert isinstance(violations, list) and len(violations) >= 1


def test_execute_sandboxed_task_deterministic_computation() -> None:
    """REQ-FM-WASM-01: Verify deterministic sandboxed computation returns COMPLETED, output=50, fuel, and SHA-256 receipt."""
    executor = WasmSandboxExecutor()
    res = executor.execute_sandboxed_task(
        task_id="t_1",
        code_payload="x * 2 + y",
        inputs={"x": 21, "y": 8},
    )
    assert _field(res, "status") == "COMPLETED"
    assert _field(res, "output") == 50
    assert _field(res, "fuel_consumed") > 0
    assert float(_field(res, "startup_time_us")) >= 0.0
    receipt = _field(res, "execution_receipt_sha256")
    assert isinstance(receipt, str) and len(receipt) == 64
    assert all(c in "0123456789abcdefABCDEF" for c in receipt)


def test_execute_sandboxed_task_enforces_fuel_instruction_limit() -> None:
    """REQ-FM-WASM-01: Verify payload exceeding max_fuel_instructions terminates with FUEL_EXHAUSTED."""
    cfg = WasmSandboxConfig(max_fuel_instructions=10)
    executor = WasmSandboxExecutor(config=cfg)
    heavy_payload = " + ".join(str(i) for i in range(1, 30))
    res = executor.execute_sandboxed_task(
        task_id="t_fuel",
        code_payload=heavy_payload,
        inputs={"iterations": 50},
    )
    assert _field(res, "status") == "FUEL_EXHAUSTED"


def test_execute_sandboxed_task_enforces_memory_page_limit() -> None:
    """REQ-FM-WASM-01: Verify payload exceeding max_memory_pages (64 KB per page) returns MEMORY_LIMIT_EXCEEDED."""
    cfg = WasmSandboxConfig(max_memory_pages=1)
    executor = WasmSandboxExecutor(config=cfg)
    res = executor.execute_sandboxed_task(
        task_id="t_mem",
        code_payload='(module (memory 4) (func (export "alloc") (result i32) i32.const 131072))',
        inputs={"allocate_bytes": 131072},
    )
    assert _field(res, "status") == "MEMORY_LIMIT_EXCEEDED"


def test_execute_sandboxed_task_blocks_policy_violation() -> None:
    """REQ-FM-WASM-01: Verify payload attempting forbidden network or filesystem access returns POLICY_VIOLATION."""
    executor = WasmSandboxExecutor()
    res = executor.execute_sandboxed_task(
        task_id="t_policy",
        code_payload='(module (import "wasi_snapshot_preview1" "sock_open" (func $sock)))',
        inputs={},
    )
    assert _field(res, "status") == "POLICY_VIOLATION"
