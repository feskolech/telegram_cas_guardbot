from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

def parse_duration(s: str) -> int:
    """
    '30m' -> 1800, '1h' -> 3600, '1d' -> 86400, '45s' -> 45
    """
    s = (s or "").strip().lower()
    if not s:
        raise ValueError("Empty duration")

    num = ""
    unit = ""
    for ch in s:
        if ch.isdigit():
            num += ch
        else:
            unit += ch

    if not num or unit not in {"s", "m", "h", "d"}:
        raise ValueError(f"Invalid duration format: {s}")

    n = int(num)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return n * mult

@dataclass(frozen=True)
class Config:
    bot_token: str
    db_path: str
    banned_log_path: str

    recheck_interval_sec: int
    update_export_interval_sec: int
    update_lols_interval_sec: int

    message_cache_limit: int
    seen_ttl_days: int

    http_timeout_seconds: int

def load_config() -> Config:
    return Config(
        bot_token=os.environ["BOT_TOKEN"],
        db_path=os.getenv("DB_PATH", "/data/bot.sqlite3"),
        banned_log_path=os.getenv("BANNED_LOG_PATH", "/data/banned.txt"),

        recheck_interval_sec=parse_duration(os.getenv("RECHECK_INTERVAL", "15m")),
        update_export_interval_sec=parse_duration(os.getenv("UPDATE_EXPORT_INTERVAL", "30m")),
        update_lols_interval_sec=parse_duration(os.getenv("UPDATE_LOLS_INTERVAL", "30m")),

        message_cache_limit=int(os.getenv("MESSAGE_CACHE_LIMIT", "50")),
        seen_ttl_days=int(os.getenv("SEEN_TTL_DAYS", "7")),

        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "7")),
    )
