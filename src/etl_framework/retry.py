from __future__ import annotations

import time
from typing import Callable, TypeVar

from etl_framework.exceptions import FrameworkError, LoadExecutionError
from etl_framework.logging_utils import log_event

T = TypeVar("T")

_TRANSIENT_MARKERS = (
    "temporarily_unavailable",
    "deadline exceeded",
    "connection reset",
    "sparkconnect",
    "rate limit",
    "429",
    "503",
)


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, FrameworkError) and not isinstance(exc, LoadExecutionError):
        return False
    text = str(exc).lower()
    return any(m in text for m in _TRANSIENT_MARKERS)


def retry_call(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_sleep_sec: float = 2.0,
    context: str = "",
) -> T:
    last: BaseException | None = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — classified then re-raised
            last = exc
            if i >= attempts or not is_transient(exc):
                raise
            log_event("retry", attempt=i, context=context, error=str(exc))
            time.sleep(base_sleep_sec * (2 ** (i - 1)))
    raise last  # pragma: no cover
