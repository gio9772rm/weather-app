"""Bound a cron's whole lifetime, including HTTP retry sleeps and native I/O."""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from threading import Timer

DEFAULT_MAX_RUNTIME_SECONDS = 15 * 60


def run_with_watchdog(
    entrypoint: Callable[[], int], *, timeout_seconds: float | None = None
) -> int:
    """Terminate a stuck worker so the next scheduled run can acquire its lock.

    Use a daemon watchdog rather than a request timeout or a main-thread signal:
    it also works during DNS resolution, Retry-After sleeps and native blocking
    calls. PostgreSQL rolls back unfinished transactions and releases advisory
    locks when this process exits; already committed observations stay saved.
    """
    seconds = (
        float(os.getenv("INGEST_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_SECONDS))
        if timeout_seconds is None
        else timeout_seconds
    )
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("INGEST_MAX_RUNTIME_SECONDS must be finite and positive")

    def stop_stalled_worker() -> None:
        # Avoid logging locks: the main thread might be stuck while holding one.
        try:
            os.write(
                2,
                (
                    f"Acquisizione interrotta: durata massima di {seconds:g} "
                    "secondi superata; ripresa al prossimo ciclo programmato.\n"
                ).encode(),
            )
        finally:
            os._exit(124)

    watchdog = Timer(seconds, stop_stalled_worker)
    watchdog.daemon = True
    watchdog.start()
    try:
        return entrypoint()
    finally:
        watchdog.cancel()
