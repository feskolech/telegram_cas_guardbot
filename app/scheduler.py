import asyncio
import time
from typing import Awaitable, Callable

async def run_periodic(name: str, interval_sec: int, coro: Callable[[], Awaitable[None]]):
    while True:
        start = time.time()
        try:
            await coro()
        except Exception:
            # can add logging here if needed
            pass
        elapsed = time.time() - start
        sleep_for = max(1.0, interval_sec - elapsed)
        await asyncio.sleep(sleep_for)
