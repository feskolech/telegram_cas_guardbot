import aiosqlite
import time
from typing import Optional

MODE_NOTIFY = "notify"
MODE_QUICKBAN = "quickban"

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS chat_settings (
  chat_id INTEGER PRIMARY KEY,
  mode TEXT NOT NULL,
  silent INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS whitelist (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS seen_users (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  last_seen_ts INTEGER NOT NULL,
  first_seen_ts INTEGER NOT NULL,
  PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS msg_cache (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS acted_users (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  action_ts INTEGER NOT NULL,
  PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS action_log (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  mode TEXT NOT NULL,
  reason TEXT NOT NULL,
  source TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cas_cache (
  chat_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  last_check_ts INTEGER NOT NULL,
  is_banned INTEGER NOT NULL,
  PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS source_updates (
  name TEXT PRIMARY KEY,
  last_ts INTEGER NOT NULL,
  count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_info (
  chat_id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS error_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  chat_id INTEGER,
  user_id INTEGER,
  message TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_msg_cache_chat_user_ts
ON msg_cache(chat_id, user_id, ts);

CREATE INDEX IF NOT EXISTS idx_action_log_chat_ts
ON action_log(chat_id, ts);

CREATE INDEX IF NOT EXISTS idx_cas_cache_ts
ON cas_cache(last_check_ts);

CREATE INDEX IF NOT EXISTS idx_error_log_ts
ON error_log(ts);
"""

class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def open(self):
        self.conn = await aiosqlite.connect(self.path)
        await self.conn.executescript(SCHEMA)
        await self._migrate()
        await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def _migrate(self):
        assert self.conn
        await self._ensure_column("seen_users", "first_seen_ts", "INTEGER")
        await self.conn.execute(
            "UPDATE seen_users SET first_seen_ts=last_seen_ts WHERE first_seen_ts IS NULL"
        )
        await self._ensure_column("action_log", "source", "TEXT NOT NULL DEFAULT 'unknown'")
        await self._ensure_column("chat_settings", "silent", "INTEGER NOT NULL DEFAULT 0")
        await self.conn.commit()

    async def _ensure_column(self, table: str, column: str, ddl: str):
        assert self.conn
        cur = await self.conn.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in await cur.fetchall()]
        if column not in cols:
            await self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    async def get_mode(self, chat_id: int) -> str:
        assert self.conn
        cur = await self.conn.execute("SELECT mode FROM chat_settings WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
        return row[0] if row else MODE_QUICKBAN

    async def set_mode(self, chat_id: int, mode: str):
        assert self.conn
        await self.conn.execute(
            "INSERT INTO chat_settings(chat_id, mode) VALUES(?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET mode=excluded.mode",
            (chat_id, mode),
        )
        await self.conn.commit()

    async def get_silent(self, chat_id: int) -> bool:
        assert self.conn
        cur = await self.conn.execute("SELECT silent FROM chat_settings WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
        return bool(row[0]) if row else False

    async def set_silent(self, chat_id: int, silent: bool):
        assert self.conn
        await self.conn.execute(
            "INSERT INTO chat_settings(chat_id, mode, silent) VALUES(?, ?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET silent=excluded.silent",
            (chat_id, MODE_QUICKBAN, int(bool(silent))),
        )
        await self.conn.commit()

    async def is_whitelisted(self, chat_id: int, user_id: int) -> bool:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT 1 FROM whitelist WHERE chat_id=? AND user_id=?",
            (chat_id, user_id),
        )
        return (await cur.fetchone()) is not None

    async def add_whitelist(self, chat_id: int, user_id: int):
        assert self.conn
        await self.conn.execute(
            "INSERT OR IGNORE INTO whitelist(chat_id, user_id) VALUES(?, ?)",
            (chat_id, user_id),
        )
        await self.conn.commit()

    async def touch_seen(self, chat_id: int, user_id: int):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO seen_users(chat_id, user_id, last_seen_ts, first_seen_ts) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(chat_id, user_id) DO UPDATE SET last_seen_ts=excluded.last_seen_ts",
            (chat_id, user_id, now, now),
        )
        await self.conn.commit()

    async def list_seen_users(self, min_ts: int) -> list[tuple[int, int, int]]:
        """
        Returns list of (chat_id, user_id, last_seen_ts)
        """
        assert self.conn
        cur = await self.conn.execute(
            "SELECT chat_id, user_id, last_seen_ts FROM seen_users WHERE last_seen_ts>=?",
            (min_ts,),
        )
        return await cur.fetchall()

    async def prune_seen_users(self, min_ts: int):
        assert self.conn
        await self.conn.execute("DELETE FROM seen_users WHERE last_seen_ts < ?", (min_ts,))
        await self.conn.execute("DELETE FROM acted_users WHERE action_ts < ?", (min_ts,))
        await self.conn.execute("DELETE FROM cas_cache WHERE last_check_ts < ?", (min_ts,))
        await self.conn.commit()

    async def add_message_id(self, chat_id: int, user_id: int, message_id: int, limit: int):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO msg_cache(chat_id, user_id, message_id, ts) VALUES(?, ?, ?, ?)",
            (chat_id, user_id, message_id, now),
        )
        # enforce limit per (chat,user): delete older extra
        await self.conn.execute(
            """
            DELETE FROM msg_cache
            WHERE rowid IN (
              SELECT rowid FROM msg_cache
              WHERE chat_id=? AND user_id=?
              ORDER BY ts DESC
              LIMIT -1 OFFSET ?
            )
            """,
            (chat_id, user_id, limit),
        )
        await self.conn.commit()

    async def get_cached_messages(self, chat_id: int, user_id: int) -> list[int]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT message_id FROM msg_cache WHERE chat_id=? AND user_id=? ORDER BY ts DESC",
            (chat_id, user_id),
        )
        rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def clear_cached_messages(self, chat_id: int, user_id: int):
        assert self.conn
        await self.conn.execute("DELETE FROM msg_cache WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        await self.conn.commit()

    async def is_actioned(self, chat_id: int, user_id: int) -> bool:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT 1 FROM acted_users WHERE chat_id=? AND user_id=?",
            (chat_id, user_id),
        )
        return (await cur.fetchone()) is not None

    async def mark_actioned(self, chat_id: int, user_id: int):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT OR IGNORE INTO acted_users(chat_id, user_id, action_ts) VALUES(?, ?, ?)",
            (chat_id, user_id, now),
        )
        await self.conn.commit()

    async def try_mark_actioned(self, chat_id: int, user_id: int) -> bool:
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT OR IGNORE INTO acted_users(chat_id, user_id, action_ts) VALUES(?, ?, ?)",
            (chat_id, user_id, now),
        )
        cur = await self.conn.execute("SELECT changes()")
        row = await cur.fetchone()
        await self.conn.commit()
        return bool(row and row[0] == 1)

    async def add_action_log(self, chat_id: int, user_id: int, action: str, mode: str, reason: str, source: str):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO action_log(chat_id, user_id, action, mode, reason, source, ts) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, action, mode, reason, source, now),
        )
        await self.conn.commit()

    async def get_action_stats(self, chat_id: int, since_ts: int) -> tuple[int, int, int, int]:
        """
        Returns: (total, notify_count, quickban_count, unique_users)
        """
        assert self.conn
        cur = await self.conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN action='notify' THEN 1 ELSE 0 END) AS notify_count,
              SUM(CASE WHEN action='quickban' THEN 1 ELSE 0 END) AS quickban_count,
              COUNT(DISTINCT user_id) AS unique_users
            FROM action_log
            WHERE chat_id=? AND ts>=?
            """,
            (chat_id, since_ts),
        )
        row = await cur.fetchone()
        if not row:
            return (0, 0, 0, 0)
        return (
            int(row[0] or 0),
            int(row[1] or 0),
            int(row[2] or 0),
            int(row[3] or 0),
        )

    async def add_error_log(self, source: str, chat_id: int | None, user_id: int | None, message: str):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO error_log(source, chat_id, user_id, message, ts) VALUES(?, ?, ?, ?, ?)",
            (source, chat_id, user_id, message, now),
        )
        await self.conn.commit()

    async def upsert_chat_info(self, chat_id: int, title: str):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO chat_info(chat_id, title, updated_ts) VALUES(?, ?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, updated_ts=excluded.updated_ts",
            (chat_id, title, now),
        )
        await self.conn.commit()

    async def upsert_source_update(self, name: str, count: int):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO source_updates(name, last_ts, count) VALUES(?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET last_ts=excluded.last_ts, count=excluded.count",
            (name, now, count),
        )
        await self.conn.commit()

    async def get_cas_cache(self, chat_id: int, user_id: int) -> Optional[tuple[int, bool]]:
        assert self.conn
        cur = await self.conn.execute(
            "SELECT last_check_ts, is_banned FROM cas_cache WHERE chat_id=? AND user_id=?",
            (chat_id, user_id),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return (int(row[0]), bool(row[1]))

    async def set_cas_cache(self, chat_id: int, user_id: int, is_banned: bool):
        assert self.conn
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO cas_cache(chat_id, user_id, last_check_ts, is_banned) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(chat_id, user_id) DO UPDATE SET last_check_ts=excluded.last_check_ts, is_banned=excluded.is_banned",
            (chat_id, user_id, now, int(is_banned)),
        )
        await self.conn.commit()
