import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """运行参数，全部可通过环境变量覆盖。"""

    db_path: str
    gap_timeout_seconds: float
    sweep_interval_seconds: float


def get_settings() -> Settings:
    return Settings(
        db_path=os.getenv("GATE_DB_PATH", "/data/gate.db"),
        gap_timeout_seconds=float(os.getenv("GAP_TIMEOUT_SECONDS", "5")),
        sweep_interval_seconds=float(os.getenv("SWEEP_INTERVAL_SECONDS", "0.5")),
    )
