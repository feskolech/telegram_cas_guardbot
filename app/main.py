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
from .lols import LolsClient
from .sources import LocalScamDB, refresh_sources
from .scheduler import run_periodic
from .handlers import router, check_user


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
    lols = LolsClient(session, timeout_seconds=cfg.http_timeout_seconds, cooldown_seconds=cfg.lols_cooldown_sec)
    cas = CASClient(session, timeout_seconds=cfg.http_timeout_seconds, cooldown_seconds=cfg.cas_cooldown_sec)

    # DI values for handlers
    dp.workflow_data["db"] = db
    dp.workflow_data["cas"] = cas
    dp.workflow_data["lols"] = lols
    dp.workflow_data["local_db"] = local_db
    dp.workflow_data["cache_limit"] = cfg.message_cache_limit
    dp.workflow_data["banned_log_path"] = cfg.banned_log_path
    dp.workflow_data["recheck_interval_sec"] = cfg.recheck_interval_sec
    dp.workflow_data["update_export_interval_sec"] = cfg.update_export_interval_sec
    dp.workflow_data["seen_ttl_days"] = cfg.seen_ttl_days
    dp.workflow_data["lols_cache_ttl_sec"] = cfg.lols_cache_ttl_sec
    dp.workflow_data["cas_cache_ttl_sec"] = cfg.cas_cache_ttl_sec

    async def task_refresh_sources():
        try:
            total_ids, export_ids = await refresh_sources(session, local_db, cfg.http_timeout_seconds)
            log.info("Sources refreshed: total=%s export=%s", total_ids, export_ids)
            await db.upsert_source_update("export", export_ids)
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

                flagged, reason, source = await check_user(
                    chat_id,
                    user_id,
                    local_db,
                    lols,
                    cas,
                    db,
                    cfg.lols_cache_ttl_sec,
                    cfg.cas_cache_ttl_sec,
                )
                if not flagged:
                    continue

                mode = await db.get_mode(chat_id)

                full_name = str(user_id)
                try:
                    member = await bot.get_chat_member(chat_id, user_id)
                    full_name = member.user.full_name or full_name
                except Exception:
                    pass

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

    asyncio.create_task(run_periodic("refresh_sources", cfg.update_export_interval_sec, task_refresh_sources))
    asyncio.create_task(run_periodic("recheck_seen", cfg.recheck_interval_sec, task_recheck_seen))

    try:
        await dp.start_polling(bot)
    finally:
        await session.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
