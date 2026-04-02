import html
import time
from aiogram import Router, F, Bot
from aiogram.types import Message, ChatMemberUpdated
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest

from .db import DB, MODE_NOTIFY, MODE_QUICKBAN
from .texts import msg_notify, msg_banned, msg_mode_set, msg_unban_ok, msg_not_admin
from .cas import CASClient, CASCircuitOpen
from .lols import LolsClient, LolsCircuitOpen
from .sources import LocalScamDB

router = Router()


def _source_result(state: str, cached: bool = False, detail: str = "") -> dict:
    return {"state": state, "cached": cached, "detail": detail}


def _parse_check_target(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        try:
            target_id = int(parts[1].strip())
        except ValueError:
            return None, "Usage: /check <userid> or reply with /check"
        if target_id <= 0:
            return None, "Usage: /check <userid> or reply with /check"
        return target_id, None

    reply = message.reply_to_message
    if reply and reply.from_user:
        if reply.from_user.id > 0:
            return reply.from_user.id, None

    return None, "Usage: /check <userid> or reply with /check"


def _format_check_source(label: str, result: dict) -> str:
    state = result["state"]
    if state == "banned":
        suffix = "cache" if result["cached"] else "live"
        return f"{label}: <b>banned</b> ({suffix})"
    if state == "clear":
        suffix = "cache" if result["cached"] else "live"
        return f"{label}: <b>clear</b> ({suffix})"
    detail = html.escape(result.get("detail") or "unavailable")
    return f"{label}: <b>unavailable</b> ({detail})"

def append_audit_line(log_path: str, chat_id: int, user_id: int, full_name: str, mode: str, reason: str, action: str):
    """
    action: "notify" or "quickban"
    """
    ts = int(time.time())
    line = f"{ts}\tchat={chat_id}\tuser={user_id}\tname={full_name}\tmode={mode}\taction={action}\treason={reason}\n"
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"

def format_stats_line(total: int, notify_count: int, quickban_count: int, unique_users: int) -> str:
    return (
        f"total={total}, notify={notify_count}, quickban={quickban_count}, unique_users={unique_users}"
    )

async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    m = await bot.get_chat_member(chat_id, user_id)
    return m.status in ("administrator", "creator")

async def check_user(
    chat_id: int,
    user_id: int,
    local_db: LocalScamDB,
    lols: LolsClient,
    cas: CASClient,
    db: DB,
    lols_cache_ttl_sec: int,
    cache_ttl_sec: int,
) -> tuple[bool, str, str]:
    """
    Returns (flagged, reason, source)
    """
    if local_db.contains(user_id):
        return True, "CAS export blacklist", "export"

    lols_result = await _check_lols_source(user_id, lols, db, lols_cache_ttl_sec, log_errors=True)
    if lols_result["state"] == "banned":
        return True, "lols.bot API (record found)", "lols"

    cas_result = await _check_cas_source(chat_id, user_id, cas, db, cache_ttl_sec, log_errors=True)
    if cas_result["state"] == "banned":
        return True, "CAS API (record found)", "cas"
    return False, "", ""


async def _check_lols_source(
    user_id: int,
    lols: LolsClient,
    db: DB,
    cache_ttl_sec: int,
    log_errors: bool,
) -> dict:
    now = int(time.time())
    cached = await db.get_lols_cache(user_id)
    if cached:
        last_ts, is_banned = cached
        if now - last_ts < cache_ttl_sec:
            return _source_result("banned" if is_banned else "clear", cached=True)

    try:
        is_banned = await lols.is_banned(user_id)
    except LolsCircuitOpen:
        return _source_result("unavailable", detail="cooldown active")
    except Exception as e:
        msg = f"LOLS down: {type(e).__name__}: {e}"
        if log_errors and lols.should_log_failure(msg, interval_sec=60):
            await db.add_error_log("lols", None, None, msg)
        return _source_result("unavailable", detail=msg)
    else:
        await db.set_lols_cache(user_id, is_banned)
        return _source_result("banned" if is_banned else "clear", cached=False)

async def _check_cas_source(
    chat_id: int,
    user_id: int,
    cas: CASClient,
    db: DB,
    cache_ttl_sec: int,
    log_errors: bool,
) -> dict:
    now = int(time.time())
    cached = await db.get_cas_cache(chat_id, user_id)
    if cached:
        last_ts, is_banned = cached
        if now - last_ts < cache_ttl_sec:
            return _source_result("banned" if is_banned else "clear", cached=True)

    try:
        is_banned = await cas.is_banned(user_id)
    except CASCircuitOpen:
        return _source_result("unavailable", detail="cooldown active")
    except Exception as e:
        msg = f"CAS down: {type(e).__name__}: {e}"
        if log_errors and cas.should_log_failure(msg, interval_sec=60):
            await db.add_error_log("cas", None, None, msg)
        return _source_result("unavailable", detail=msg)

    await db.set_cas_cache(chat_id, user_id, is_banned)
    return _source_result("banned" if is_banned else "clear", cached=False)


async def inspect_user(
    chat_id: int,
    user_id: int,
    local_db: LocalScamDB,
    lols: LolsClient,
    cas: CASClient,
    db: DB,
    lols_cache_ttl_sec: int,
    cas_cache_ttl_sec: int,
) -> dict:
    export_hit = local_db.contains(user_id)
    lols_result = await _check_lols_source(user_id, lols, db, lols_cache_ttl_sec, log_errors=False)
    cas_result = await _check_cas_source(chat_id, user_id, cas, db, cas_cache_ttl_sec, log_errors=False)
    whitelisted = await db.is_whitelisted(chat_id, user_id)
    actioned = await db.is_actioned(chat_id, user_id)

    flagged = export_hit or lols_result["state"] == "banned" or cas_result["state"] == "banned"
    inconclusive = (not flagged) and (
        lols_result["state"] == "unavailable" or cas_result["state"] == "unavailable"
    )
    final_source = ""
    final_reason = ""
    if export_hit:
        final_source = "export"
        final_reason = "CAS export blacklist"
    elif lols_result["state"] == "banned":
        final_source = "lols"
        final_reason = "lols.bot API (record found)"
    elif cas_result["state"] == "banned":
        final_source = "cas"
        final_reason = "CAS API (record found)"

    return {
        "user_id": user_id,
        "export_hit": export_hit,
        "lols": lols_result,
        "cas": cas_result,
        "whitelisted": whitelisted,
        "actioned": actioned,
        "flagged": flagged,
        "inconclusive": inconclusive,
        "final_source": final_source,
        "final_reason": final_reason,
        "would_act": flagged and not whitelisted and not actioned,
    }

async def act_on_spammer(
    bot: Bot,
    db: DB,
    chat_id: int,
    user_id: int,
    full_name: str,
    mode: str,
    reason: str,
    source: str,
    log_path: str,
    cache_limit: int,
):
    # whitelist check
    if await db.is_whitelisted(chat_id, user_id):
        return
    if await db.is_actioned(chat_id, user_id):
        return
    # atomic guard against parallel handlers
    if not await db.try_mark_actioned(chat_id, user_id):
        return

    if mode == MODE_NOTIFY:
        append_audit_line(log_path, chat_id, user_id, full_name, mode, reason, action="notify")
        await db.add_action_log(chat_id, user_id, action="notify", mode=mode, reason=reason, source=source)
        try:
            await bot.send_message(
                chat_id,
                msg_notify(full_name, user_id, reason),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramBadRequest as e:
            await db.add_error_log("telegram", chat_id, user_id, f"{type(e).__name__}: {e}")
        return

    # quickban
    append_audit_line(log_path, chat_id, user_id, full_name, mode, reason, action="quickban")
    await db.add_action_log(chat_id, user_id, action="quickban", mode=mode, reason=reason, source=source)

    try:
        await bot.ban_chat_member(chat_id, user_id)
    except TelegramBadRequest as e:
        await db.add_error_log("telegram", chat_id, user_id, f"{type(e).__name__}: {e}")

    # delete cached messages
    msg_ids = await db.get_cached_messages(chat_id, user_id)
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except TelegramBadRequest as e:
            await db.add_error_log("telegram", chat_id, user_id, f"{type(e).__name__}: {e}")

    await db.clear_cached_messages(chat_id, user_id)

    if not await db.get_silent(chat_id):
        try:
            await bot.send_message(
                chat_id,
                msg_banned(full_name, user_id, reason),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramBadRequest as e:
            await db.add_error_log("telegram", chat_id, user_id, f"{type(e).__name__}: {e}")

@router.message(Command("notify"))
async def cmd_notify(message: Message, bot: Bot, db: DB):
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(msg_not_admin())
        return
    await db.set_mode(message.chat.id, MODE_NOTIFY)
    await message.reply(msg_mode_set(MODE_NOTIFY), parse_mode=ParseMode.HTML)

@router.message(Command("quickban"))
async def cmd_quickban(message: Message, bot: Bot, db: DB):
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(msg_not_admin())
        return
    await db.set_mode(message.chat.id, MODE_QUICKBAN)
    await message.reply(msg_mode_set(MODE_QUICKBAN), parse_mode=ParseMode.HTML)

@router.message(Command("silent"))
async def cmd_silent(message: Message, bot: Bot, db: DB):
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(msg_not_admin())
        return

    mode = await db.get_mode(message.chat.id)
    if mode != MODE_QUICKBAN:
        await message.reply("⛔ /silent is available only in quickban mode.")
        return

    cur = await db.get_silent(message.chat.id)
    new_val = not cur
    await db.set_silent(message.chat.id, new_val)
    await message.reply(
        f"✅ Silent mode is now: <b>{'on' if new_val else 'off'}</b>",
        parse_mode=ParseMode.HTML,
    )

@router.message(Command("unban"))
async def cmd_unban(message: Message, bot: Bot, db: DB):
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(msg_not_admin())
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Usage: /unban <userid>")
        return

    try:
        target_id = int(parts[1])
    except ValueError:
        await message.reply("Usage: /unban <userid>")
        return

    await db.add_whitelist(message.chat.id, target_id)

    # try to unban in Telegram
    try:
        await bot.unban_chat_member(message.chat.id, target_id, only_if_banned=True)
    except TelegramBadRequest:
        pass

    await message.reply(msg_unban_ok(target_id), parse_mode=ParseMode.HTML)


@router.message(Command("check"))
async def cmd_check(
    message: Message,
    bot: Bot,
    db: DB,
    local_db: LocalScamDB,
    lols: LolsClient,
    cas: CASClient,
    lols_cache_ttl_sec: int,
    cas_cache_ttl_sec: int,
):
    if not message.from_user:
        return
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        await message.reply(msg_not_admin())
        return

    target_id, error_text = _parse_check_target(message)
    if error_text:
        await message.reply(error_text)
        return

    result = await inspect_user(
        message.chat.id,
        target_id,
        local_db,
        lols,
        cas,
        db,
        lols_cache_ttl_sec,
        cas_cache_ttl_sec,
    )

    if result["flagged"]:
        detection = f"<b>flagged</b> via <b>{html.escape(result['final_source'])}</b>"
        reason = html.escape(result["final_reason"])
    elif result["inconclusive"]:
        detection = "<b>inconclusive</b>"
        reason = "one or more remote sources are unavailable"
    else:
        detection = "<b>clean</b>"
        reason = "-"

    text = (
        "🔎 Check result\n"
        f"User: <code>{target_id}</code>\n"
        f"Detection: {detection}\n"
        f"Reason: <b>{reason}</b>\n"
        f"Whitelisted: <b>{'yes' if result['whitelisted'] else 'no'}</b>\n"
        f"Already actioned: <b>{'yes' if result['actioned'] else 'no'}</b>\n"
        f"Would act now: <b>{'yes' if result['would_act'] else 'no'}</b>\n"
        f"CAS export: <b>{'match' if result['export_hit'] else 'clear'}</b>\n"
        f"{_format_check_source('LOLS', result['lols'])}\n"
        f"{_format_check_source('CAS', result['cas'])}"
    )
    await message.reply(text, parse_mode=ParseMode.HTML)

@router.message(Command("status"))
async def cmd_status(
    message: Message,
    bot: Bot,
    db: DB,
    local_db: LocalScamDB,
    lols: LolsClient,
    recheck_interval_sec: int,
    update_export_interval_sec: int,
    seen_ttl_days: int,
    lols_cache_ttl_sec: int,
    cas_cache_ttl_sec: int,
):
    if not message.from_user:
        return
    chat_id = message.chat.id
    mode = await db.get_mode(chat_id)
    silent = await db.get_silent(chat_id)
    text = (
        "🟢 Bot status: online\n"
        f"Mode: <b>{mode}</b>\n"
        f"Silent: <b>{'on' if silent else 'off'}</b>\n"
        f"CAS export size: <b>{local_db.size()}</b>\n"
        f"Recheck interval: <b>{format_duration(recheck_interval_sec)}</b>\n"
        f"CAS export update: <b>{format_duration(update_export_interval_sec)}</b>\n"
        f"LOLS cache TTL: <b>{format_duration(lols_cache_ttl_sec)}</b>\n"
        f"CAS cache TTL: <b>{format_duration(cas_cache_ttl_sec)}</b>\n"
        f"Seen TTL: <b>{seen_ttl_days}d</b>"
    )
    await message.reply(text, parse_mode=ParseMode.HTML)

@router.message(Command("stats"))
async def cmd_stats(message: Message, db: DB):
    if not message.from_user:
        return
    chat_id = message.chat.id
    now = int(time.time())
    day = now - 86400
    week = now - 7 * 86400
    month = now - 30 * 86400

    d_total, d_notify, d_quickban, d_users = await db.get_action_stats(chat_id, day)
    w_total, w_notify, w_quickban, w_users = await db.get_action_stats(chat_id, week)
    m_total, m_notify, m_quickban, m_users = await db.get_action_stats(chat_id, month)

    text = (
        "📊 Actions stats\n"
        f"Last 24h: <b>{format_stats_line(d_total, d_notify, d_quickban, d_users)}</b>\n"
        f"Last 7d: <b>{format_stats_line(w_total, w_notify, w_quickban, w_users)}</b>\n"
        f"Last 30d: <b>{format_stats_line(m_total, m_notify, m_quickban, m_users)}</b>"
    )
    await message.reply(text, parse_mode=ParseMode.HTML)

@router.chat_member()
async def on_chat_member_update(
    event: ChatMemberUpdated,
    bot: Bot,
    db: DB,
    cas: CASClient,
    lols: LolsClient,
    local_db: LocalScamDB,
    cache_limit: int,
    banned_log_path: str,
    lols_cache_ttl_sec: int,
    cas_cache_ttl_sec: int,
):
    # user joined becomes member/restricted
    new_status = event.new_chat_member.status
    if new_status not in ("member", "restricted"):
        return

    joining = event.new_chat_member.user
    chat_id = event.chat.id
    if event.chat.title:
        await db.upsert_chat_info(chat_id, event.chat.title)
    user_id = joining.id
    full_name = joining.full_name or str(user_id)

    await db.touch_seen(chat_id, user_id)

    if await db.is_whitelisted(chat_id, user_id):
        return
    if await db.is_actioned(chat_id, user_id):
        return

    flagged, reason, source = await check_user(
        chat_id,
        user_id,
        local_db,
        lols,
        cas,
        db,
        lols_cache_ttl_sec,
        cas_cache_ttl_sec,
    )
    if not flagged:
        return

    mode = await db.get_mode(chat_id)
    await act_on_spammer(
        bot=bot,
        db=db,
        chat_id=chat_id,
        user_id=user_id,
        full_name=full_name,
        mode=mode,
        reason=reason,
        source=source,
        log_path=banned_log_path,
        cache_limit=cache_limit,
    )

@router.message(F.from_user)
async def on_any_message(
    message: Message,
    bot: Bot,
    db: DB,
    cas: CASClient,
    lols: LolsClient,
    local_db: LocalScamDB,
    cache_limit: int,
    banned_log_path: str,
    lols_cache_ttl_sec: int,
    cas_cache_ttl_sec: int,
):
    chat_id = message.chat.id
    if message.chat.title:
        await db.upsert_chat_info(chat_id, message.chat.title)
    user_id = message.from_user.id
    full_name = message.from_user.full_name or str(user_id)

    await db.touch_seen(chat_id, user_id)

    # cache message_id for deletions
    await db.add_message_id(chat_id, user_id, message.message_id, cache_limit)

    if await db.is_whitelisted(chat_id, user_id):
        return
    if await db.is_actioned(chat_id, user_id):
        return

    flagged, reason, source = await check_user(
        chat_id,
        user_id,
        local_db,
        lols,
        cas,
        db,
        lols_cache_ttl_sec,
        cas_cache_ttl_sec,
    )
    if not flagged:
        return

    mode = await db.get_mode(chat_id)
    await act_on_spammer(
        bot=bot,
        db=db,
        chat_id=chat_id,
        user_id=user_id,
        full_name=full_name,
        mode=mode,
        reason=reason,
        source=source,
        log_path=banned_log_path,
        cache_limit=cache_limit,
    )
