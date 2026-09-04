"""Structured logging. Failures are logged then re-raised by callers."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

_LOGGER = logging.getLogger("etl_framework")
if not _LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    _LOGGER.addHandler(handler)
    _LOGGER.setLevel(logging.INFO)


def log_event(event: str, **fields: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **{k: v for k, v in fields.items() if v is not None},
    }
    _LOGGER.info(json.dumps(payload, default=str))
