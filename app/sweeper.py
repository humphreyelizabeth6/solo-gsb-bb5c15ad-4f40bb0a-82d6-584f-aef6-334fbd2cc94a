"""后台超时扫描任务。"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("event_gate.sweeper")


async def sweeper_loop(service, interval: float) -> None:
    while True:
        try:
            advanced = await service.sweep_once()
            if advanced:
                logger.info("sweeper advanced %d stream(s)", advanced)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 扫描失败不应杀死进程
            logger.exception("sweeper iteration failed")
        await asyncio.sleep(interval)
