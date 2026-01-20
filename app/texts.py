def cas_link(user_id: int) -> str:
    return f"https://api.cas.chat/check?user_id={user_id}"

def msg_notify(full_name: str, user_id: int, reason: str) -> str:
    return (
        f"⚠️ Suspicious account detected: <b>{full_name}</b> (ID: <code>{user_id}</code>). "
        f"Reason: <b>{reason}</b>. Details: <a href=\"{cas_link(user_id)}\">CAS check</a>."
    )

def msg_banned(full_name: str, user_id: int, reason: str) -> str:
    return (
        f"🛡 Removed <b>{full_name}</b> (ID: <code>{user_id}</code>) — "
        f"Reason: <b>{reason}</b>. Details: <a href=\"{cas_link(user_id)}\">CAS check</a>."
    )

def msg_mode_set(mode: str) -> str:
    return f"✅ Mode set to: <b>{mode}</b>"

def msg_unban_ok(user_id: int) -> str:
    return f"✅ User <code>{user_id}</code> added to whitelist for this chat (bot will ignore)."

def msg_not_admin() -> str:
    return "⛔ This command is available only for chat administrators."
