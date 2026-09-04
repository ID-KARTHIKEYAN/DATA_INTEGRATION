"""GENERIC_SCRIPTS holds script names (comma-separated), not inline code.

Scripts live in <repo>/scripts/<name>.py and receive CUSTOM_SCRIPT_PARAMS as a dict.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from etl_framework.exceptions import LoadExecutionError
from etl_framework.logging_utils import log_event


def run_generic_scripts(
    *,
    scripts_field: object | None,
    params: object | None,
    spark,
    context: dict[str, Any],
    scripts_root: Path,
) -> None:
    names = [s.strip() for s in str(scripts_field or "").split(",") if s.strip()]
    if not names:
        return
    param_map = params if isinstance(params, dict) else {}
    if params is not None and not isinstance(params, dict):
        raise LoadExecutionError("CUSTOM_SCRIPT_PARAMS must be map<string,string>")
    for name in names:
        if "/" in name or "\\" in name or ".." in name:
            raise LoadExecutionError(f"GENERIC_SCRIPTS entry must be a file stem, got {name!r}")
        path = scripts_root / f"{name}.py"
        if not path.is_file():
            raise LoadExecutionError(f"GENERIC_SCRIPTS file not found: {path}")
        log_event("generic_script", script=str(path), params=param_map)
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise LoadExecutionError(f"Cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not hasattr(module, "run"):
            raise LoadExecutionError(f"{path} must define run(spark, params, context)")
        module.run(spark, param_map, context)
