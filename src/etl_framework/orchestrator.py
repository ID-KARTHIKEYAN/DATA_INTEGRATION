from __future__ import annotations

from pathlib import Path

from etl_framework.config import DEFAULT_CATALOG, VALID_LAYERS
from etl_framework.exceptions import MetadataValidationError
from etl_framework.logging_utils import log_event
from etl_framework.runner import LayerRunner


def detect_layer(group_id: str, run_layer: str) -> str:
    layer = (run_layer or "").strip().upper()
    if layer:
        if layer not in VALID_LAYERS:
            raise MetadataValidationError(f"RUN_LAYER must be L0, L1, or L2, got {run_layer!r}")
        return layer
    gid = group_id.strip().upper()
    for suffix in VALID_LAYERS:
        if gid.endswith(f"_{suffix}"):
            return suffix
    raise MetadataValidationError(
        "Cannot infer ETL_LAYER. Pass RUN_LAYER=L0|L1|L2 or use DATA_FLOW_GROUP_ID suffix _L0/_L1/_L2."
    )


def run(
    *,
    group_id: str,
    target_load_table: str = "",
    run_layer: str = "",
    environment: str = "dev",
    control_catalog: str = DEFAULT_CATALOG,
    scripts_root: str | Path | None = None,
    free_edition: bool = True,
) -> None:
    gid = (group_id or "").strip()
    if not gid:
        raise MetadataValidationError("GROUP_ID is required")
    layer = detect_layer(gid, run_layer)
    root = Path(scripts_root) if scripts_root else Path(__file__).resolve().parents[2] / "scripts"
    log_event(
        "orchestrator_start",
        group_id=gid,
        layer=layer,
        target_load_table=target_load_table or "ALL",
        environment=environment,
        control_catalog=control_catalog,
    )
    runner = LayerRunner(
        control_catalog=control_catalog,
        scripts_root=root,
        free_edition=free_edition,
    )
    executed = runner.run_group(
        group_id=gid,
        layer=layer,
        target_object=(target_load_table or "").strip(),
        environment=environment,
    )
    log_event("orchestrator_complete", group_id=gid, layer=layer, tasks=executed)
