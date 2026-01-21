import hashlib
import html
import hmac
import os
import sqlite3
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
import uvicorn

DB_PATH = os.getenv("DB_PATH", "/data/bot.sqlite3")
ADMIN_ENABLED = os.getenv("ADMIN_ENABLED", "false").strip().lower() in {"1", "true", "yes"}
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
ADMIN_PORT = int(os.getenv("ADMIN_PORT", "9005"))
ADMIN_AUTH_MODE = os.getenv("ADMIN_AUTH_MODE", "token").strip().lower()
ADMIN_TELEGRAM_IDS = os.getenv("ADMIN_TELEGRAM_IDS", "").strip()
ADMIN_TELEGRAM_BOT_USERNAME = os.getenv("ADMIN_TELEGRAM_BOT_USERNAME", "").strip()
ADMIN_TELEGRAM_BOT_TOKEN = os.getenv("ADMIN_TELEGRAM_BOT_TOKEN", "").strip() or os.getenv("BOT_TOKEN", "").strip()
ADMIN_SESSION_SECRET = os.getenv("ADMIN_SESSION_SECRET", "").strip() or ADMIN_TOKEN
ADMIN_SESSION_TTL_SEC = int(os.getenv("ADMIN_SESSION_TTL_SEC", "43200"))
ADMIN_PUBLIC_URL = os.getenv("ADMIN_PUBLIC_URL", "").strip().rstrip("/")
ADMIN_TELEGRAM_AUTH_MAX_AGE_SEC = int(os.getenv("ADMIN_TELEGRAM_AUTH_MAX_AGE_SEC", "86400"))
UPDATE_EXPORT_INTERVAL = os.getenv("UPDATE_EXPORT_INTERVAL", "30m")
UPDATE_LOLS_INTERVAL = os.getenv("UPDATE_LOLS_INTERVAL", "30m")

app = FastAPI()


def _auth_modes() -> set[str]:
    if ADMIN_AUTH_MODE == "both":
        return {"token", "telegram"}
    return {ADMIN_AUTH_MODE}


def _require_auth(request: Request):
    if not ADMIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin disabled")
    modes = _auth_modes()
    if "token" in modes and _check_token_auth(request):
        return
    if "telegram" in modes and _check_session_auth(request):
        return
    if "token" in modes:
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    raise HTTPException(status_code=401, detail="Missing or invalid session")


def _check_token_auth(request: Request) -> bool:
    if not ADMIN_TOKEN:
        return False
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    token = auth.removeprefix("Bearer ").strip()
    return token == ADMIN_TOKEN


def _check_session_auth(request: Request) -> bool:
    cookie = request.cookies.get("admin_session", "")
    if not cookie:
        return False
    return _verify_session(cookie)


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _since(seconds: int) -> int:
    return int(time.time()) - seconds

def _parse_duration(s: str) -> int:
    s = (s or "").strip().lower()
    if not s:
        return 0
    num = ""
    unit = ""
    for ch in s:
        if ch.isdigit():
            num += ch
        else:
            unit += ch
    if not num or unit not in {"s", "m", "h", "d"}:
        return 0
    n = int(num)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return n * mult

def _get_source_updates(conn: sqlite3.Connection) -> dict:
    try:
        cur = conn.execute(
            "SELECT name, last_ts, count FROM source_updates"
        )
    except sqlite3.OperationalError:
        return {}
    rows = cur.fetchall()
    return {r["name"]: {"last_ts": int(r["last_ts"]), "count": int(r["count"])} for r in rows}

def _fmt_ts(ts: int | None) -> str:
    if not ts:
        return "-"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _percentile(values: list[int], pct: float) -> int | None:
    if not values:
        return None
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round((pct / 100.0) * (len(values) - 1)))))
    return values[k]


def _stats_for_chat(conn: sqlite3.Connection, chat_id: int, since_ts: int) -> dict:
    cur = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN action='notify' THEN 1 ELSE 0 END) AS notify_count,
          SUM(CASE WHEN action='quickban' THEN 1 ELSE 0 END) AS quickban_count,
          SUM(CASE WHEN source='local' THEN 1 ELSE 0 END) AS local_count,
          SUM(CASE WHEN source='cas' THEN 1 ELSE 0 END) AS cas_count,
          COUNT(DISTINCT user_id) AS unique_users
        FROM action_log
        WHERE chat_id=? AND ts>=?
        """,
        (chat_id, since_ts),
    )
    row = cur.fetchone() or {}
    return {
        "total": int(row["total"] or 0),
        "notify": int(row["notify_count"] or 0),
        "quickban": int(row["quickban_count"] or 0),
        "local": int(row["local_count"] or 0),
        "cas": int(row["cas_count"] or 0),
        "unique": int(row["unique_users"] or 0),
    }


def _stats_all(conn: sqlite3.Connection, since_ts: int) -> dict:
    cur = conn.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN action='notify' THEN 1 ELSE 0 END) AS notify_count,
          SUM(CASE WHEN action='quickban' THEN 1 ELSE 0 END) AS quickban_count,
          SUM(CASE WHEN source='local' THEN 1 ELSE 0 END) AS local_count,
          SUM(CASE WHEN source='cas' THEN 1 ELSE 0 END) AS cas_count,
          COUNT(DISTINCT user_id) AS unique_users
        FROM action_log
        WHERE ts>=?
        """,
        (since_ts,),
    )
    row = cur.fetchone() or {}
    return {
        "total": int(row["total"] or 0),
        "notify": int(row["notify_count"] or 0),
        "quickban": int(row["quickban_count"] or 0),
        "local": int(row["local_count"] or 0),
        "cas": int(row["cas_count"] or 0),
        "unique": int(row["unique_users"] or 0),
    }


def _fetch_time_to_action(conn: sqlite3.Connection, since_ts: int) -> list[int]:
    cur = conn.execute(
        """
        SELECT (a.ts - s.first_seen_ts) AS delta
        FROM action_log a
        JOIN seen_users s ON a.chat_id=s.chat_id AND a.user_id=s.user_id
        WHERE a.ts>=? AND s.first_seen_ts IS NOT NULL AND a.ts>=s.first_seen_ts
        """,
        (since_ts,),
    )
    return [int(r["delta"]) for r in cur.fetchall() if r["delta"] is not None]


def _recent_actions(conn: sqlite3.Connection, limit: int = 25) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT chat_id, user_id, action, mode, reason, source, ts
        FROM action_log
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def _recent_errors(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT source, chat_id, user_id, message, ts
        FROM error_log
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def _list_chats(conn: sqlite3.Connection) -> list[tuple[int, str | None]]:
    cur = conn.execute(
        """
        SELECT a.chat_id AS chat_id, c.title AS title
        FROM (SELECT DISTINCT chat_id FROM action_log) a
        LEFT JOIN chat_info c ON a.chat_id=c.chat_id
        ORDER BY a.chat_id
        """
    )
    return [(int(r["chat_id"]), r["title"]) for r in cur.fetchall()]


def _fmt_stats_line(stats: dict) -> str:
    return (
        f"total {stats['total']} | notify {stats['notify']} | quickban {stats['quickban']} | "
        f"local {stats['local']} | cas {stats['cas']} | unique {stats['unique']}"
    )


def _source_class(source: str) -> str:
    if source == "local":
        return "tag-local"
    if source == "cas":
        return "tag-cas"
    return "muted"


@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.get("/api/sources")
def api_sources(request: Request):
    _require_auth(request)
    export_interval = _parse_duration(UPDATE_EXPORT_INTERVAL) or 1800
    lols_interval = _parse_duration(UPDATE_LOLS_INTERVAL) or 1800
    with _connect() as conn:
        updates = _get_source_updates(conn)
    def _pack(name: str, interval: int) -> dict:
        row = updates.get(name)
        last_ts = row["last_ts"] if row else None
        count = row["count"] if row else None
        next_ts = (last_ts + interval) if last_ts and interval else None
        return {"last_ts": last_ts, "next_ts": next_ts, "count": count}
    return {
        "export": _pack("export", export_interval),
        "lols": _pack("lols", lols_interval),
        "total": _pack("total", 0),
        "server_ts": int(time.time()),
        "export_interval_sec": export_interval,
        "lols_interval_sec": lols_interval,
    }


@app.get("/api/actions")
def api_actions(request: Request):
    _require_auth(request)
    with _connect() as conn:
        rows = _recent_actions(conn, limit=25)
    return {
        "items": [
            {
                "ts": int(r["ts"]),
                "chat_id": int(r["chat_id"]),
                "user_id": int(r["user_id"]),
                "action": r["action"],
                "source": r["source"],
                "reason": r["reason"],
            }
            for r in rows
        ]
    }


@app.get("/api/stats")
def api_stats(request: Request):
    _require_auth(request)
    day = _since(86400)
    week = _since(7 * 86400)
    month = _since(30 * 86400)
    with _connect() as conn:
        global_day = _stats_all(conn, day)
        global_week = _stats_all(conn, week)
        global_month = _stats_all(conn, month)
    return {
        "day": global_day,
        "week": global_week,
        "month": global_month,
    }


def _parse_admin_ids(value: str) -> set[int]:
    out: set[int] = set()
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            out.add(int(item))
        except ValueError:
            continue
    return out


def _sign_session(user_id: int, issued_ts: int) -> str:
    msg = f"{user_id}:{issued_ts}".encode("utf-8")
    sig = hmac.new(ADMIN_SESSION_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return f"{user_id}:{issued_ts}:{sig}"


def _verify_session(value: str) -> bool:
    if not ADMIN_SESSION_SECRET:
        return False
    parts = value.split(":")
    if len(parts) != 3:
        return False
    try:
        user_id = int(parts[0])
        issued_ts = int(parts[1])
    except ValueError:
        return False
    if int(time.time()) - issued_ts > ADMIN_SESSION_TTL_SEC:
        return False
    expected = _sign_session(user_id, issued_ts)
    return hmac.compare_digest(expected, value)


def _verify_telegram_payload(payload: dict) -> bool:
    if not ADMIN_TELEGRAM_BOT_TOKEN:
        return False
    if "hash" not in payload:
        return False
    payload = dict(payload)
    check_hash = payload.pop("hash")
    allowed = {
        "id",
        "first_name",
        "last_name",
        "username",
        "photo_url",
        "auth_date",
    }
    payload = {k: v for k, v in payload.items() if k in allowed}
    data_check_string = "\n".join(f"{k}={payload[k]}" for k in sorted(payload.keys()))
    secret_key = hashlib.sha256(ADMIN_TELEGRAM_BOT_TOKEN.encode("utf-8")).digest()
    hmac_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(hmac_hash, check_hash)

def _is_recent_auth_date(payload: dict) -> bool:
    auth_date = payload.get("auth_date")
    try:
        auth_ts = int(auth_date)
    except (TypeError, ValueError):
        return False
    now = int(time.time())
    return (now - auth_ts) <= ADMIN_TELEGRAM_AUTH_MAX_AGE_SEC

def _safe_next(value: str) -> str:
    if not value or not value.startswith("/") or value.startswith("//") or "://" in value:
        return "/"
    return value

def _esc(value: object) -> str:
    return html.escape(str(value), quote=True)

@app.get("/login", response_class=HTMLResponse)
def login(request: Request):
    if not ADMIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin disabled")
    modes = _auth_modes()
    if "telegram" not in modes and "token" not in modes:
        raise HTTPException(status_code=404, detail="Login disabled")
    if "telegram" in modes:
        if not ADMIN_TELEGRAM_BOT_USERNAME:
            return HTMLResponse("ADMIN_TELEGRAM_BOT_USERNAME is not set", status_code=503)
        if not ADMIN_PUBLIC_URL:
            return HTMLResponse("ADMIN_PUBLIC_URL is not set", status_code=503)
        if not ADMIN_SESSION_SECRET:
            return HTMLResponse("ADMIN_SESSION_SECRET is not set", status_code=503)
        if not ADMIN_TELEGRAM_IDS:
            return HTMLResponse("ADMIN_TELEGRAM_IDS is not set", status_code=503)
    if "token" in modes and not ADMIN_TOKEN:
        return HTMLResponse("ADMIN_TOKEN is not set", status_code=503)
    next_url = _safe_next(request.query_params.get("next", "/"))
    query = urlencode({"next": next_url})
    error = (request.query_params.get("error") or "").strip()
    error_msg = "Invalid token. Please try again." if error == "invalid_token" else ""
    auth_url = f"{ADMIN_PUBLIC_URL}/auth/telegram?{query}" if "telegram" in modes else ""
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Login · CAS Guard Admin</title>
  <style>
    body {{
      margin: 0;
      font-family: "Trebuchet MS", "Verdana", "Tahoma", sans-serif;
      background: #0f141b;
      color: #e9eef5;
      display: grid;
      place-items: center;
      height: 100vh;
    }}
    .card {{
      padding: 24px 28px;
      border-radius: 14px;
      border: 1px solid #223041;
      background: #141b23;
      text-align: center;
      max-width: 420px;
    }}
    form {{
      display: grid;
      gap: 12px;
      margin-bottom: 16px;
    }}
    input[type="password"] {{
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid #223041;
      background: #0f141b;
      color: #e9eef5;
    }}
    button {{
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid #223041;
      background: #18222d;
      color: #e9eef5;
      cursor: pointer;
      font: inherit;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    p {{
      margin: 0 0 20px;
      color: #9fb0c0;
      font-size: 14px;
    }}
    .error {{
      margin: 6px 0 12px;
      color: #ff6b6b;
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>CAS Guard Admin</h1>
    <p>Choose a login method to access the dashboard.</p>
    {f"<div class='error'>{error_msg}</div>" if error_msg else ""}
    {"""
    <form action="/auth/token" method="post">
      <input type="password" name="token" placeholder="Admin token" required>
      <input type="hidden" name="next" value="%s">
      <button type="submit">Login with token</button>
    </form>
    """ % _esc(next_url) if "token" in modes else ""}
    {"""
    <script async src="https://telegram.org/js/telegram-widget.js?22"
      data-telegram-login="%s"
      data-size="large"
      data-userpic="false"
      data-auth-url="%s"
      data-request-access="write"></script>
    """ % (ADMIN_TELEGRAM_BOT_USERNAME, auth_url) if "telegram" in modes else ""}
  </div>
</body>
</html>
"""
    return HTMLResponse(html)


@app.get("/auth/telegram")
def auth_telegram(request: Request):
    if not ADMIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin disabled")
    if "telegram" not in _auth_modes():
        raise HTTPException(status_code=404, detail="Telegram login disabled")
    if not ADMIN_SESSION_SECRET:
        raise HTTPException(status_code=503, detail="ADMIN_SESSION_SECRET not set")
    if not ADMIN_TELEGRAM_IDS:
        raise HTTPException(status_code=503, detail="ADMIN_TELEGRAM_IDS not set")
    payload = dict(request.query_params)
    if not _verify_telegram_payload(payload):
        raise HTTPException(status_code=403, detail="Invalid auth payload")
    if not _is_recent_auth_date(payload):
        raise HTTPException(status_code=403, detail="Expired auth payload")
    user_id = payload.get("id")
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing id")
    try:
        user_id_int = int(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid id")
    allowed_ids = _parse_admin_ids(ADMIN_TELEGRAM_IDS)
    if allowed_ids and user_id_int not in allowed_ids:
        raise HTTPException(status_code=403, detail="User not allowed")
    issued_ts = int(time.time())
    session_value = _sign_session(user_id_int, issued_ts)
    next_url = _safe_next(request.query_params.get("next", "/"))
    resp = RedirectResponse(url=next_url, status_code=302)
    resp.set_cookie(
        "admin_session",
        session_value,
        httponly=True,
        max_age=ADMIN_SESSION_TTL_SEC,
        samesite="lax",
        secure=True,
    )
    return resp


@app.post("/auth/token")
async def auth_token(request: Request):
    if not ADMIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin disabled")
    if "token" not in _auth_modes():
        raise HTTPException(status_code=404, detail="Token login disabled")
    form = await request.form()
    token = (form.get("token") or "").strip()
    if not token or token != ADMIN_TOKEN:
        return RedirectResponse(url="/login?error=invalid_token", status_code=302)
    if not ADMIN_SESSION_SECRET:
        raise HTTPException(status_code=503, detail="ADMIN_SESSION_SECRET not set")
    issued_ts = int(time.time())
    session_value = _sign_session(0, issued_ts)
    next_url = _safe_next(form.get("next") or "/")
    resp = RedirectResponse(url=next_url, status_code=302)
    resp.set_cookie(
        "admin_session",
        session_value,
        httponly=True,
        max_age=ADMIN_SESSION_TTL_SEC,
        samesite="lax",
        secure=True,
    )
    return resp


@app.get("/logout")
def logout(request: Request):
    if not ADMIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin disabled")
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("admin_session")
    return resp


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    try:
        _require_auth(request)
    except HTTPException as e:
        if e.status_code in (401, 403):
            return RedirectResponse(url="/login", status_code=302)
        raise
    with _connect() as conn:
        chats = _list_chats(conn)
        day = _since(86400)
        week = _since(7 * 86400)
        month = _since(30 * 86400)

        global_day = _stats_all(conn, day)
        global_week = _stats_all(conn, week)
        global_month = _stats_all(conn, month)

        deltas = _fetch_time_to_action(conn, week)
        p50 = _percentile(deltas, 50)
        p95 = _percentile(deltas, 95)

        recent_actions = _recent_actions(conn, limit=25)
        recent_errors = _recent_errors(conn, limit=20)
        source_updates = _get_source_updates(conn)

        per_chat = []
        for chat_id, title in chats:
            per_chat.append(
                {
                    "chat_id": chat_id,
                    "title": title or "-",
                    "day": _stats_for_chat(conn, chat_id, day),
                    "week": _stats_for_chat(conn, chat_id, week),
                    "month": _stats_for_chat(conn, chat_id, month),
                }
            )

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CAS Guard Admin</title>
  <style>
    :root {{
      --bg: #0f141b;
      --card: #141b23;
      --muted: #9fb0c0;
      --text: #e9eef5;
      --accent: #36c2b4;
      --accent-2: #f0b65a;
      --danger: #ff6b6b;
      --border: #223041;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Trebuchet MS", "Verdana", "Tahoma", sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 600px at 10% -10%, #1b2b3a 0%, transparent 60%),
        radial-gradient(900px 500px at 90% 0%, #142238 0%, transparent 65%),
        var(--bg);
    }}
    header {{
      padding: 32px 24px 8px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 28px;
      letter-spacing: 0.5px;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
    }}
    .actions {{
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .actions a,
    .actions button {{
      color: var(--text);
      text-decoration: none;
      padding: 8px 12px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: var(--card);
      cursor: pointer;
      font: inherit;
    }}
    .actions a:hover,
    .actions button:hover {{
      border-color: var(--accent);
    }}
    body.light {{
      --bg: #f4f1ea;
      --card: #ffffff;
      --muted: #4f5d6b;
      --text: #1a2230;
      --accent: #2b7a78;
      --accent-2: #8a5a11;
      --danger: #b0232a;
      --border: #d7dee6;
      background:
        radial-gradient(1200px 600px at 10% -10%, #e7e1d6 0%, transparent 60%),
        radial-gradient(900px 500px at 90% 0%, #efe9dd 0%, transparent 65%),
        var(--bg);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
      padding: 16px 24px 24px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }}
    body.light .card {{
      box-shadow: 0 10px 24px rgba(20, 30, 40, 0.08);
    }}
    .card h3 {{
      margin: 0 0 8px;
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--muted);
    }}
    .card .value {{
      font-size: 18px;
      font-weight: 700;
    }}
    .section {{
      padding: 8px 24px 24px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      letter-spacing: 0.3px;
    }}
    .pill {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 12px;
      background: #1f2b38;
      color: var(--muted);
      border: 1px solid var(--border);
    }}
    body.light .pill {{
      background: #eef2f6;
      color: #40505f;
      border-color: #d3dde6;
    }}
    .tag-local {{ color: var(--accent); }}
    .tag-cas {{ color: var(--accent-2); }}
    .tag-err {{ color: var(--danger); }}
    .muted {{ color: var(--muted); }}
    @media (max-width: 820px) {{
      header {{
        flex-direction: column;
        align-items: flex-start;
      }}
      .grid {{
        grid-template-columns: 1fr;
        padding: 12px 16px 20px;
      }}
      .section {{
        padding: 8px 16px 20px;
      }}
      .card {{
        padding: 14px;
      }}
      table {{
        display: block;
        overflow-x: auto;
        white-space: nowrap;
      }}
      .card .value {{
        font-size: 16px;
      }}
      h1 {{
        font-size: 24px;
      }}
    }}
    @media (max-width: 520px) {{
      .actions {{
        width: 100%;
        justify-content: flex-start;
        flex-wrap: wrap;
      }}
      .actions a,
      .actions button {{
        width: auto;
      }}
      .pill {{
        font-size: 11px;
      }}
      th, td {{
        padding: 8px 6px;
        font-size: 12px;
      }}
    }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>CAS Guard Admin</h1>
      <div class="sub">Read-only dashboard · data source: {_esc(DB_PATH)}</div>
    </div>
    <div class="actions">
      <button type="button" id="theme-toggle">Theme</button>
      <a href="/logout">Logout</a>
    </div>
  </header>

  <div class="grid">
    <div class="card">
      <h3>Last 24h</h3>
      <div class="value" id="stats-day">{_fmt_stats_line(global_day)}</div>
    </div>
    <div class="card">
      <h3>Last 7d</h3>
      <div class="value" id="stats-week">{_fmt_stats_line(global_week)}</div>
    </div>
    <div class="card">
      <h3>Last 30d</h3>
      <div class="value" id="stats-month">{_fmt_stats_line(global_month)}</div>
    </div>
    <div class="card">
      <h3>Time To Action (7d)</h3>
      <div class="value">p50: {p50 or '-'}s · p95: {p95 or '-'}s</div>
    </div>
    <div class="card">
      <h3>Sources</h3>
      <div class="value" id="sources-meta">
        Export: {_esc(_fmt_ts(source_updates.get("export", {}).get("last_ts")))}<br>
        LOLS: {_esc(_fmt_ts(source_updates.get("lols", {}).get("last_ts")))}<br>
        Total IDs: {_esc(source_updates.get("total", {}).get("count", "-"))}
      </div>
      <div class="muted" id="sources-next">
        Next export: - · Next lols: -
      </div>
    </div>
  </div>

  <div class="section card">
    <h3>Chats</h3>
    <table>
      <thead>
        <tr>
          <th>Chat ID</th>
          <th>Title</th>
          <th>24h</th>
          <th>7d</th>
          <th>30d</th>
        </tr>
      </thead>
      <tbody>
        {''.join(
            f"<tr><td><span class='pill'>{c['chat_id']}</span></td>"
            f"<td class='muted'>{_esc(c['title'])}</td>"
            f"<td>{_fmt_stats_line(c['day'])}</td>"
            f"<td>{_fmt_stats_line(c['week'])}</td>"
            f"<td>{_fmt_stats_line(c['month'])}</td></tr>"
            for c in per_chat
        )}
      </tbody>
    </table>
  </div>

  <div class="section card">
    <h3>Recent Actions</h3>
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Chat</th>
          <th>User</th>
          <th>Action</th>
          <th>Source</th>
          <th>Reason</th>
        </tr>
      </thead>
      <tbody id="actions-body">
        {''.join(
            f"<tr>"
            f"<td class='muted'>{_fmt_ts(r['ts'])}</td>"
            f"<td>{r['chat_id']}</td>"
            f"<td>{r['user_id']}</td>"
            f"<td>{_esc(r['action'])}</td>"
            f"<td class='{_source_class(r['source'] or 'unknown')}'>{_esc(r['source'] or 'unknown')}</td>"
            f"<td class='muted'>{_esc(r['reason'])}</td>"
            f"</tr>"
            for r in recent_actions
        )}
      </tbody>
    </table>
  </div>

  <div class="section card">
    <h3>Recent Errors</h3>
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Source</th>
          <th>Chat</th>
          <th>User</th>
          <th>Message</th>
        </tr>
      </thead>
      <tbody>
        {''.join(
            f"<tr>"
            f"<td class='muted'>{_fmt_ts(e['ts'])}</td>"
            f"<td class='tag-err'>{_esc(e['source'])}</td>"
            f"<td>{e['chat_id'] or '-'}</td>"
            f"<td>{e['user_id'] or '-'}</td>"
            f"<td class='muted'>{_esc(e['message'])}</td>"
            f"</tr>"
            for e in recent_errors
        )}
      </tbody>
    </table>
  </div>
  <script>
    (function() {{
      const root = document.body;
      const btn = document.getElementById("theme-toggle");
      const key = "cas_admin_theme";
      const stored = localStorage.getItem(key);
      if (stored === "light") {{
        root.classList.add("light");
      }}
      btn.addEventListener("click", function () {{
        root.classList.toggle("light");
        localStorage.setItem(key, root.classList.contains("light") ? "light" : "dark");
      }});
    }})();

    function fmtTs(ts) {{
      if (!ts) return "-";
      const d = new Date(ts * 1000);
      return d.toISOString().replace("T", " ").slice(0, 19) + " UTC";
    }}
    function refreshSources() {{
      fetch("/api/sources", {{ credentials: "same-origin" }})
        .then(r => r.ok ? r.json() : null)
        .then(data => {{
          if (!data) return;
          const exportLast = fmtTs(data.export.last_ts);
          const lolsLast = fmtTs(data.lols.last_ts);
          const total = data.total.count ?? "-";
          const exportNextTs = data.export.next_ts ?? (data.export.last_ts && data.export_interval_sec ? data.export.last_ts + data.export_interval_sec : null);
          const lolsNextTs = data.lols.next_ts ?? (data.lols.last_ts && data.lols_interval_sec ? data.lols.last_ts + data.lols_interval_sec : null);
          const exportNext = fmtTs(exportNextTs);
          const lolsNext = fmtTs(lolsNextTs);
          const meta = document.getElementById("sources-meta");
          const next = document.getElementById("sources-next");
          if (meta) {{
            meta.innerHTML = "Export: " + exportLast + "<br>LOLS: " + lolsLast + "<br>Total IDs: " + total;
          }}
          if (next) {{
            next.innerHTML = "Next export: " + exportNext + "<br>Next lols: " + lolsNext;
          }}
        }})
        .catch(() => {{}});
    }}
    function refreshStats() {{
      fetch("/api/stats", {{ credentials: "same-origin" }})
        .then(r => r.ok ? r.json() : null)
        .then(data => {{
          if (!data) return;
          const day = document.getElementById("stats-day");
          const week = document.getElementById("stats-week");
          const month = document.getElementById("stats-month");
          if (day) day.textContent = fmtStats(data.day);
          if (week) week.textContent = fmtStats(data.week);
          if (month) month.textContent = fmtStats(data.month);
        }})
        .catch(() => {{}});
    }}
    function refreshActions() {{
      fetch("/api/actions", {{ credentials: "same-origin" }})
        .then(r => r.ok ? r.json() : null)
        .then(data => {{
          if (!data || !data.items) return;
          const body = document.getElementById("actions-body");
          if (!body) return;
          body.innerHTML = data.items.map(r => {{
            const sourceClass = r.source === "local" ? "tag-local" : (r.source === "cas" ? "tag-cas" : "muted");
            return "<tr>"
              + "<td class='muted'>" + fmtTs(r.ts) + "</td>"
              + "<td>" + r.chat_id + "</td>"
              + "<td>" + r.user_id + "</td>"
              + "<td>" + esc(r.action) + "</td>"
              + "<td class='" + sourceClass + "'>" + esc(r.source || "unknown") + "</td>"
              + "<td class='muted'>" + esc(r.reason || "") + "</td>"
              + "</tr>";
          }}).join("");
        }})
        .catch(() => {{}});
    }}
    function fmtStats(s) {{
      return "total " + s.total + " | notify " + s.notify + " | quickban " + s.quickban
        + " | local " + s.local + " | cas " + s.cas + " | unique " + s.unique;
    }}
    function esc(value) {{
      return String(value || "").replace(/[&<>"']/g, function (m) {{
        return {{
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;"
        }}[m];
      }});
    }}
    refreshSources();
    refreshStats();
    refreshActions();
    setInterval(function () {{
      refreshSources();
      refreshStats();
      refreshActions();
    }}, 15000);
  </script>
</body>
</html>
"""
    return HTMLResponse(html)


if __name__ == "__main__":
    uvicorn.run("admin.main:app", host="0.0.0.0", port=ADMIN_PORT)
