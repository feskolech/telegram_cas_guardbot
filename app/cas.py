import aiohttp
import asyncio
import json
import time


class CASUnavailable(Exception):
    pass


class CASCircuitOpen(Exception):
    pass

class CASClient:
    def __init__(self, session: aiohttp.ClientSession, timeout_seconds: int = 7, cooldown_seconds: int = 60):
        self.session = session
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.cooldown_seconds = max(1, int(cooldown_seconds))
        self._down_until_ts = 0
        self._last_failure_log_ts = 0
        self._last_failure_sig = ""

    def should_log_failure(self, message: str, interval_sec: int = 60) -> bool:
        """
        Best-effort in-process deduplication of CAS failure logs.
        Logs at most once per interval, or immediately if failure signature changes.
        """
        now = int(time.time())
        sig = (message or "")[:200]
        if sig != self._last_failure_sig:
            self._last_failure_sig = sig
            self._last_failure_log_ts = now
            return True
        if now - self._last_failure_log_ts >= max(1, int(interval_sec)):
            self._last_failure_log_ts = now
            return True
        return False

    async def is_banned(self, user_id: int) -> bool:
        now = int(time.time())
        if now < self._down_until_ts:
            raise CASCircuitOpen(f"CAS cooldown until {self._down_until_ts}")

        url = f"https://api.cas.chat/check?user_id={user_id}"
        try:
            async with self.session.get(url, timeout=self.timeout) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise CASUnavailable(f"HTTP {resp.status}: {body[:200]}")
                data = await resp.json(content_type=None)
        except CASUnavailable:
            self._down_until_ts = now + self.cooldown_seconds
            raise
        except (asyncio.TimeoutError, aiohttp.ClientError, json.JSONDecodeError) as e:
            self._down_until_ts = now + self.cooldown_seconds
            raise CASUnavailable(f"{type(e).__name__}: {e}") from e
        except Exception as e:
            self._down_until_ts = now + self.cooldown_seconds
            raise CASUnavailable(f"{type(e).__name__}: {e}") from e

        if not isinstance(data, dict):
            self._down_until_ts = now + self.cooldown_seconds
            raise CASUnavailable(f"Invalid CAS payload type: {type(data).__name__}")

        # CAS: ok==true + result => record found (CAS banned)
        return bool(data.get("ok") is True and data.get("result"))
