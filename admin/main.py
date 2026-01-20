import hashlib
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


@app.get("/login", response_class=HTMLResponse)
def login(request: Request):
    if not ADMIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin disabled")
    if "telegram" not in _auth_modes():
        raise HTTPException(status_code=404, detail="Telegram login disabled")
    if not ADMIN_TELEGRAM_BOT_USERNAME:
        return HTMLResponse("ADMIN_TELEGRAM_BOT_USERNAME is not set", status_code=503)
    if not ADMIN_PUBLIC_URL:
        return HTMLResponse("ADMIN_PUBLIC_URL is not set", status_code=503)
    if not ADMIN_SESSION_SECRET:
        return HTMLResponse("ADMIN_SESSION_SECRET is not set", status_code=503)
    query = urlencode({"next": request.query_params.get("next", "/")})
    auth_url = f"{ADMIN_PUBLIC_URL}/auth/telegram?{query}"
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
    h1 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    p {{
      margin: 0 0 20px;
      color: #9fb0c0;
      font-size: 14px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>CAS Guard Admin</h1>
    <p>Login with Telegram to access the dashboard.</p>
    <script async src="https://telegram.org/js/telegram-widget.js?22"
      data-telegram-login="{ADMIN_TELEGRAM_BOT_USERNAME}"
      data-size="large"
      data-userpic="false"
      data-auth-url="{auth_url}"
      data-request-access="write"></script>
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
    payload = dict(request.query_params)
    if not _verify_telegram_payload(payload):
        raise HTTPException(status_code=403, detail="Invalid auth payload")
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
    resp = RedirectResponse(url=request.query_params.get("next", "/"), status_code=302)
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
    _require_auth(request)
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
  </style>
</head>
<body>
  <header>
    <div>
      <h1>CAS Guard Admin</h1>
      <div class="sub">Read-only dashboard · data source: {DB_PATH}</div>
    </div>
    <div class="actions">
      <button type="button" id="theme-toggle">Theme</button>
      <a href="/logout">Logout</a>
    </div>
  </header>

  <div class="grid">
    <div class="card">
      <h3>Last 24h</h3>
      <div class="value">{_fmt_stats_line(global_day)}</div>
    </div>
    <div class="card">
      <h3>Last 7d</h3>
      <div class="value">{_fmt_stats_line(global_week)}</div>
    </div>
    <div class="card">
      <h3>Last 30d</h3>
      <div class="value">{_fmt_stats_line(global_month)}</div>
    </div>
    <div class="card">
      <h3>Time To Action (7d)</h3>
      <div class="value">p50: {p50 or '-'}s · p95: {p95 or '-'}s</div>
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
            f"<td class='muted'>{c['title']}</td>"
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
      <tbody>
        {''.join(
            f"<tr>"
            f"<td class='muted'>{_fmt_ts(r['ts'])}</td>"
            f"<td>{r['chat_id']}</td>"
            f"<td>{r['user_id']}</td>"
            f"<td>{r['action']}</td>"
            f"<td class='{_source_class(r['source'] or 'unknown')}'>{r['source'] or 'unknown'}</td>"
            f"<td class='muted'>{r['reason']}</td>"
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
            f"<td class='tag-err'>{e['source']}</td>"
            f"<td>{e['chat_id'] or '-'}</td>"
            f"<td>{e['user_id'] or '-'}</td>"
            f"<td class='muted'>{e['message']}</td>"
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
  </script>
</body>
</html>
"""
    return HTMLResponse(html)


if __name__ == "__main__":
    uvicorn.run("admin.main:app", host="0.0.0.0", port=ADMIN_PORT)
