# =============================================================================
# framework/exception_handler.py
# Retry logic and error classification for the ETL framework.
# Distinguishes between retryable (transient) and non-retryable errors.
# =============================================================================

from __future__ import annotations
import time
import traceback as tb
from typing import Callable, Tuple, Type


# ── Non-retryable errors (fail immediately, never retry) ─────────────────

NON_RETRYABLE = (
    ValueError,      # ValidationError, ConfigurationError, TransformError
    TypeError,
)


# ── Retryable error patterns (transient, worth retrying) ─────────────────

RETRYABLE_MESSAGES = (
    "concurrent",           # ConcurrentAppendException, ConcurrentDeleteReadException
    "transient",
    "connection",
    "timeout",
    "503",
    "rate limit",
    "throttl",
    "py4j",                 # Py4JError (driver/executor disconnect)
    "java.net",
    "socket",
)


def is_retryable(exc: Exception) -> bool:
    """
    Returns True if the exception is likely transient and worth retrying.
    Returns False for logic/config errors that retrying cannot fix.
    """
    if isinstance(exc, NON_RETRYABLE):
        return False
    msg = str(exc).lower()
    return any(pattern in msg for pattern in RETRYABLE_MESSAGES)


def with_retry(
    fn: Callable,
    max_retries: int   = 3,
    backoff_secs: int  = 10,
    label: str         = "",
) -> object:
    """
    Executes *fn()* with exponential back-off retry.

    Args:
        fn:           Zero-argument callable to execute.
        max_retries:  Maximum number of attempts (default 3).
        backoff_secs: Base wait time in seconds. Doubles each attempt.
        label:        Human-readable label for log messages.

    Returns:
        Result of fn() on success.

    Raises:
        The last exception if all retries are exhausted.
        Non-retryable exceptions are re-raised immediately.
    """
    last_exc: Exception = None  # type: ignore

    for attempt in range(1, max_retries + 1):
        try:
            return fn()

        except Exception as e:
            if not is_retryable(e):
                # Non-transient — re-raise immediately, no point retrying
                raise

            last_exc = e
            if attempt == max_retries:
                print(
                    f"  ❌ [{label}] All {max_retries} attempt(s) failed. "
                    f"Last error: {type(e).__name__}: {str(e)[:200]}"
                )
                raise

            wait = backoff_secs * (2 ** (attempt - 1))
            print(
                f"  ⚠ [{label}] Attempt {attempt}/{max_retries} failed "
                f"({type(e).__name__}). Retrying in {wait}s..."
            )
            time.sleep(wait)

    raise last_exc  # type: ignore (unreachable but satisfies type checker)


def format_error(exc: Exception, max_len: int = 400) -> str:
    """
    Returns a clean, truncated error message suitable for audit_log.MESSAGE.
    Includes exception type and the first max_len characters of the message.
    """
    return f"{type(exc).__name__}: {str(exc)[:max_len]}"


def print_traceback(exc: Exception, label: str = "") -> None:
    """
    Prints the full traceback to stdout (visible in Databricks job logs).
    """
    prefix = f"[{label}] " if label else ""
    print(f"\n{'='*70}")
    print(f"{prefix}FULL TRACEBACK:")
    print(f"{'='*70}")
    tb.print_exc()
    print(f"{'='*70}\n")
