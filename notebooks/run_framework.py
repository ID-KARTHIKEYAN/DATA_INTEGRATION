"""Databricks Python-task entry point for one metadata-selected ETL layer/task."""

import sys
from pathlib import Path


def _widget(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name).strip()  # noqa: F821
    except Exception:
        return default


for name, default in (("GROUP_ID", ""), ("RUN_LAYER", ""), ("TASK_ID", ""), ("CATALOG", "demo_catalog")):
    try:
        dbutils.widgets.text(name, default)  # noqa: F821
    except Exception:
        pass

repo_candidates = list(Path("/Workspace/Repos").glob("*/DATA_INTEGRATION"))
if not repo_candidates:
    raise RuntimeError("DATA_INTEGRATION repository was not found under /Workspace/Repos")
sys.path.insert(0, str(repo_candidates[0]))

from framework.orchestrator import Orchestrator

arguments = sys.argv[1:]
group_id = (arguments[0] if len(arguments) > 0 else _widget("GROUP_ID")).strip().upper()
layer = (arguments[1] if len(arguments) > 1 else _widget("RUN_LAYER")).strip().upper()
task_id = (arguments[2] if len(arguments) > 2 else _widget("TASK_ID")).strip() or None
if not group_id or layer not in {"L0", "L1", "L2"}:
    raise ValueError("GROUP_ID and RUN_LAYER (L0, L1, or L2) are required")

catalog = (arguments[3] if len(arguments) > 3 else _widget("CATALOG", "demo_catalog")).strip()
results = Orchestrator(spark, catalog).run(group_id, layer, task_id)  # noqa: F821
print(f"Completed {len(results)} task(s): {results}")