import time
import random
import traceback
from datetime import datetime
from typing import Any, Callable, List, Optional, Tuple


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


def _default_is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, NonRetryableError):
        return False
    if isinstance(exc, RetryableError):
        return True
    retryable_messages = [
        "timeout",
        "timed out",
        "connection reset",
        "broken pipe",
        "temporary failure",
        "network error",
        "throttl",
        "rate limit",
        "deadlock",
        "could not serialize",
        "concurrent",
        "merge",
        "try again",
        "retry"
    ]
    msg = str(exc).lower()
    type_name = type(exc).__name__.lower()
    combined = msg + " " + type_name
    return any(r in combined for r in retryable_messages)


class RetryHandler:
    def __init__(self, max_attempts: int = 3, base_delay_sec: float = 1.0,
                 max_delay_sec: float = 60.0, jitter: bool = True,
                 is_retryable: Optional[Callable[[BaseException], bool]] = None,
                 on_retry=None, on_failure=None):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.max_attempts = max_attempts
        self.base_delay_sec = base_delay_sec
        self.max_delay_sec = max_delay_sec
        self.jitter = jitter
        self.is_retryable = is_retryable or _default_is_retryable
        self.on_retry = on_retry
        self.on_failure = on_failure
        self.attempts = 0
        self.last_exception: Optional[BaseException] = None
        self.timings: List[Tuple[int, float, datetime, Optional[str]]] = []

    def _compute_delay(self, attempt: int) -> float:
        exp_delay = self.base_delay_sec * (2 ** (attempt - 1))
        delay = min(exp_delay, self.max_delay_sec)
        if self.jitter and delay > 0:
            delay = delay * (0.5 + random.random() * 0.5)
        return delay

    def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.max_attempts + 1):
            start = datetime.now()
            self.attempts = attempt
            try:
                result = func(*args, **kwargs)
                self.timings.append((attempt, (datetime.now() - start).total_seconds(), start, None))
                return result
            except Exception as exc:
                last_exc = exc
                self.last_exception = exc
                duration = (datetime.now() - start).total_seconds()
                self.timings.append((attempt, duration, start, f"{type(exc).__name__}: {str(exc)[:200]}"))
                can_retry = attempt < self.max_attempts and self.is_retryable(exc)
                if self.on_retry and can_retry:
                    try:
                        self.on_retry(attempt, exc, args, kwargs)
                    except Exception:
                        pass
                if not can_retry:
                    if self.on_failure:
                        try:
                            self.on_failure(attempt, exc, args, kwargs)
                        except Exception:
                            pass
                    raise
                delay = self._compute_delay(attempt)
                print(
                    f"[RetryHandler] Attempt {attempt}/{self.max_attempts} FAILED "
                    f"({type(exc).__name__}: {str(exc)[:100]}). "
                    f"Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
        if last_exc:
            raise last_exc
        raise RuntimeError("RetryHandler.execute terminated without result")

    def summary(self) -> str:
        lines = [f"RetryHandler summary: {self.attempts} attempt(s)"]
        for (att, dur, ts, err) in self.timings:
            status = "OK" if err is None else f"FAIL: {err[:80]}"
            lines.append(f"  Attempt {att}: {dur:.2f}s at {ts.isoformat()} -> {status}")
        return "\n".join(lines)


def retry_with_backoff(max_attempts: int = 3, base_delay_sec: float = 1.0,
                       max_delay_sec: float = 60.0, jitter: bool = True,
                       is_retryable: Optional[Callable[[BaseException], bool]] = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            handler = RetryHandler(
                max_attempts=max_attempts,
                base_delay_sec=base_delay_sec,
                max_delay_sec=max_delay_sec,
                jitter=jitter,
                is_retryable=is_retryable
            )
            return handler.execute(func, *args, **kwargs)
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator
