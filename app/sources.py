import aiohttp

EXPORT_URL = "https://api.cas.chat/export.csv"
LOLS_URL = "https://lols.bot/scammers.txt"

class LocalScamDB:
    def __init__(self):
        self._set: set[int] = set()

    def contains(self, user_id: int) -> bool:
        return user_id in self._set

    def replace_all(self, new_ids: set[int]):
        self._set = new_ids

    def size(self) -> int:
        return len(self._set)

async def _download_text(session: aiohttp.ClientSession, url: str, timeout_seconds: int) -> str:
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with session.get(url, timeout=timeout) as resp:
        resp.raise_for_status()
        return await resp.text()

def _parse_ids_from_lols(text: str) -> set[int]:
    out: set[int] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.add(int(line))
        except ValueError:
            continue
    return out

def _parse_ids_from_export_csv(text: str) -> set[int]:
    """
    export.csv can have headers/multiple columns.
    Use int from the first column.
    """
    out: set[int] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("user") and "id" in line.lower():
            continue
        first = line.split(",", 1)[0].strip()
        try:
            out.add(int(first))
        except ValueError:
            continue
    return out

async def refresh_sources(
    session: aiohttp.ClientSession,
    scamdb: LocalScamDB,
    timeout_seconds: int,
) -> tuple[int, int, int]:
    """
    Returns: (total_ids, export_ids, lols_ids)
    """
    export_text = await _download_text(session, EXPORT_URL, timeout_seconds)
    lols_text = await _download_text(session, LOLS_URL, timeout_seconds)

    export_ids = _parse_ids_from_export_csv(export_text)
    lols_ids = _parse_ids_from_lols(lols_text)

    ids = set()
    ids |= export_ids
    ids |= lols_ids

    scamdb.replace_all(ids)
    return (len(ids), len(export_ids), len(lols_ids))
