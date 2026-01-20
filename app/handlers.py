import time
from aiogram import Router, F, Bot
from aiogram.types import Message, ChatMemberUpdated
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest

from .db import DB, MODE_NOTIFY, MODE_QUICKBAN
from .texts import msg_notify, msg_banned, msg_mode_set, msg_unban_ok, msg_not_admin
from .cas import CASClient
from .sources import LocalScamDB

router = Router()

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

async def check_user(user_id: int, local_db: LocalScamDB, cas: CASClient) -> tuple[bool, str]:
    """
    Returns (flagged, reason)
    """
    if local_db.contains(user_id):
        return True, "Local blacklist (CAS export / lols)"
    if await cas.is_banned(user_id):
        return True, "CAS API (record found)"
    return False, ""

async def act_on_spammer(
    bot: Bot,
    db: DB,
    chat_id: int,
    user_id: int,
    full_name: str,
    mode: str,
    reason: str,
    log_path: str,
    cache_limit: int,
):
    # whitelist check
    if await db.is_whitelisted(chat_id, user_id):
        return
    if await db.is_actioned(chat_id, user_id):
        return

    if mode == MODE_NOTIFY:
        append_audit_line(log_path, chat_id, user_id, full_name, mode, reason, action="notify")
        await db.add_action_log(chat_id, user_id, action="notify", mode=mode, reason=reason)
        await bot.send_message(
            chat_id,
            msg_notify(full_name, user_id, reason),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await db.mark_actioned(chat_id, user_id)
        return

    # quickban
    append_audit_line(log_path, chat_id, user_id, full_name, mode, reason, action="quickban")
    await db.add_action_log(chat_id, user_id, action="quickban", mode=mode, reason=reason)

    try:
        await bot.ban_chat_member(chat_id, user_id)
    except TelegramBadRequest:
        pass

    # delete cached messages
    msg_ids = await db.get_cached_messages(chat_id, user_id)
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except TelegramBadRequest:
            pass

    await db.clear_cached_messages(chat_id, user_id)

    await bot.send_message(
        chat_id,
        msg_banned(full_name, user_id, reason),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    await db.mark_actioned(chat_id, user_id)

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

@router.message(Command("status"))
async def cmd_status(
    message: Message,
    bot: Bot,
    db: DB,
    local_db: LocalScamDB,
    recheck_interval_sec: int,
    update_export_interval_sec: int,
    update_lols_interval_sec: int,
    seen_ttl_days: int,
):
    if not message.from_user:
        return
    chat_id = message.chat.id
    mode = await db.get_mode(chat_id)
    text = (
        "🟢 Bot status: online\n"
        f"Mode: <b>{mode}</b>\n"
        f"Local blacklist size: <b>{local_db.size()}</b>\n"
        f"Recheck interval: <b>{format_duration(recheck_interval_sec)}</b>\n"
        f"Source update: export={format_duration(update_export_interval_sec)}, lols={format_duration(update_lols_interval_sec)}\n"
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
    local_db: LocalScamDB,
    cache_limit: int,
    banned_log_path: str,
):
    # user joined becomes member/restricted
    new_status = event.new_chat_member.status
    if new_status not in ("member", "restricted"):
        return

    joining = event.new_chat_member.user
    chat_id = event.chat.id
    user_id = joining.id
    full_name = joining.full_name or str(user_id)

    await db.touch_seen(chat_id, user_id)

    if await db.is_whitelisted(chat_id, user_id):
        return
    if await db.is_actioned(chat_id, user_id):
        return

    flagged, reason = await check_user(user_id, local_db, cas)
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
        log_path=banned_log_path,
        cache_limit=cache_limit,
    )

@router.message(F.from_user)
async def on_any_message(
    message: Message,
    bot: Bot,
    db: DB,
    cas: CASClient,
    local_db: LocalScamDB,
    cache_limit: int,
    banned_log_path: str,
):
    chat_id = message.chat.id
    user_id = message.from_user.id
    full_name = message.from_user.full_name or str(user_id)

    await db.touch_seen(chat_id, user_id)

    # cache message_id for deletions
    await db.add_message_id(chat_id, user_id, message.message_id, cache_limit)

    if await db.is_whitelisted(chat_id, user_id):
        return
    if await db.is_actioned(chat_id, user_id):
        return

    flagged, reason = await check_user(user_id, local_db, cas)
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
        log_path=banned_log_path,
        cache_limit=cache_limit,
    )
