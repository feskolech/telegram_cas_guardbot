import aiohttp

class CASClient:
    def __init__(self, session: aiohttp.ClientSession, timeout_seconds: int = 7):
        self.session = session
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def is_banned(self, user_id: int) -> bool:
        url = f"https://api.cas.chat/check?user_id={user_id}"
        async with self.session.get(url, timeout=self.timeout) as resp:
            data = await resp.json(content_type=None)

        # CAS: ok==true + result => record found (CAS banned)
        return bool(data.get("ok") is True and data.get("result"))
