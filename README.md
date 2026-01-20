# TG CAS Guard Bot

Anti-spam bot for Telegram groups with hybrid detection:
- Local blacklists (CAS export.csv + lols.bot scammers.txt)
- CAS API check on demand

## Features
- Per-chat modes:
  - /notify (report only)
  - /quickban (ban + delete cached messages)
- Updates sources every 30m (configurable)
- Rechecks seen users every N (e.g. 15m)
- Deduplicates actions per user (no repeated notifications/bans)
- Whitelist/unban:
  - /unban <userid> (adds to whitelist for this chat + tries to unban)
- Status:
  - /status (bot status, mode, intervals, local DB size)
- Stats:
  - /stats (24h / 7d / 30d counts)
- CAS checks are cached for a short TTL to reduce API load.

## Requirements (Telegram)
Bot must be admin with:
- Ban users
- Delete messages

Also disable Privacy Mode in BotFather to receive all messages:
BotFather -> /setprivacy -> Disable

## Run (Docker)
1) Copy `.env.example` to `.env`, set BOT_TOKEN
2) `docker compose up -d --build`

Logs:
- SQLite: ./data/bot.sqlite3
- Ban/notify audit log: ./data/banned.txt

Stats:
- Stored in SQLite action_log table and pruned with seen TTL.
