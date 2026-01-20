import asyncio
import time
import logging
import aiohttp

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from .config import load_config
from .db import DB
from .cas import CASClient
from .sources import LocalScamDB, refresh_sources
from .scheduler import run_periodic
from .handlers import router


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("cas-guard")

    cfg = load_config()

    bot = Bot(
        token=cfg.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()
    dp.include_router(router)

    db = DB(cfg.db_path)
    await db.open()

    local_db = LocalScamDB()

    session = aiohttp.ClientSession()
    cas = CASClient(session, timeout_seconds=cfg.http_timeout_seconds)

    # DI values for handlers
    dp.workflow_data["db"] = db
    dp.workflow_data["cas"] = cas
    dp.workflow_data["local_db"] = local_db
    dp.workflow_data["cache_limit"] = cfg.message_cache_limit
    dp.workflow_data["banned_log_path"] = cfg.banned_log_path
    dp.workflow_data["recheck_interval_sec"] = cfg.recheck_interval_sec
    dp.workflow_data["update_export_interval_sec"] = cfg.update_export_interval_sec
    dp.workflow_data["update_lols_interval_sec"] = cfg.update_lols_interval_sec
    dp.workflow_data["seen_ttl_days"] = cfg.seen_ttl_days
    dp.workflow_data["cas_cache_ttl_sec"] = cfg.cas_cache_ttl_sec

    async def task_refresh_sources():
        try:
            total_ids, export_ids, lols_ids = await refresh_sources(session, local_db, cfg.http_timeout_seconds)
            log.info("Sources refreshed: total=%s export=%s lols=%s", total_ids, export_ids, lols_ids)
            await db.upsert_source_update("export", export_ids)
            await db.upsert_source_update("lols", lols_ids)
            await db.upsert_source_update("total", total_ids)
        except Exception as e:
            log.exception("Failed to refresh sources: %s", e)

    async def task_recheck_seen():
        try:
            now = int(time.time())
            min_ts = now - cfg.seen_ttl_days * 86400
            await db.prune_seen_users(min_ts)

            seen = await db.list_seen_users(min_ts=min_ts)
            if seen:
                log.info("Recheck: seen_users=%s", len(seen))

            for chat_id, user_id, _ in seen:
                if await db.is_whitelisted(chat_id, user_id):
                    continue
                if await db.is_actioned(chat_id, user_id):
                    continue

                flagged = local_db.contains(user_id)
                source = "local" if flagged else ""
                if not flagged:
                    cached = await db.get_cas_cache(chat_id, user_id)
                    if cached:
                        last_ts, is_banned = cached
                        if now - last_ts < cfg.cas_cache_ttl_sec:
                            flagged = bool(is_banned)
                            source = "cas" if flagged else ""
                    if not flagged:
                        try:
                            flagged = await cas.is_banned(user_id)
                        except Exception as e:
                            await db.add_error_log("cas", chat_id, user_id, f"{type(e).__name__}: {e}")
                            continue
                        await db.set_cas_cache(chat_id, user_id, flagged)
                        if not flagged:
                            continue
                        source = "cas"

                mode = await db.get_mode(chat_id)

                full_name = str(user_id)
                try:
                    member = await bot.get_chat_member(chat_id, user_id)
                    full_name = member.user.full_name or full_name
                except Exception:
                    pass

                reason = "Local blacklist (CAS export / lols)" if local_db.contains(user_id) else "CAS API (record found)"

                from .handlers import act_on_spammer
                await act_on_spammer(
                    bot=bot,
                    db=db,
                    chat_id=chat_id,
                    user_id=user_id,
                    full_name=full_name,
                    mode=mode,
                    reason=reason,
                    source=source,
                    log_path=cfg.banned_log_path,
                    cache_limit=cfg.message_cache_limit,
                )
        except Exception as e:
            log.exception("Failed to recheck seen users: %s", e)

    log.info("Starting bot polling...")
    await task_refresh_sources()

    update_interval = min(cfg.update_export_interval_sec, cfg.update_lols_interval_sec)
    asyncio.create_task(run_periodic("refresh_sources", update_interval, task_refresh_sources))
    asyncio.create_task(run_periodic("recheck_seen", cfg.recheck_interval_sec, task_recheck_seen))

    try:
        await dp.start_polling(bot)
    finally:
        await session.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
