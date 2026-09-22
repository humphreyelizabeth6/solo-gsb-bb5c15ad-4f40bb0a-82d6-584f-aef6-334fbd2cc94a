"""Runtime configuration via environment variables."""
from __future__ import annotations

import os

#: Path to the SQLite database file.
DATABASE_PATH: str = os.environ.get("DATABASE_PATH", "events.db")

#: How long (seconds) a stream waits for a missing seq before the gap is skipped.
GAP_TIMEOUT_SECONDS: float = float(os.environ.get("GAP_TIMEOUT_SECONDS", "30"))

#: How often (seconds) the background sweeper checks for expired gaps.
SWEEP_INTERVAL_SECONDS: float = float(os.environ.get("SWEEP_INTERVAL_SECONDS", "1"))
