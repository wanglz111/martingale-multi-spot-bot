from __future__ import annotations

from typing import Dict, Optional

from notifiers.base import CompositeNotifier, Notifier, PrintNotifier
from notifiers.telegram import TelegramNotifier


def build_notifier(config: Optional[Dict]) -> Notifier:
    if not config:
        return PrintNotifier()

    notifiers = []

    telegram_cfg = config.get("telegram", {})
    if telegram_cfg.get("enabled"):
        token = telegram_cfg.get("bot_token")
        chat_id = telegram_cfg.get("chat_id")
        if token and chat_id:
            notifiers.append(TelegramNotifier(token, chat_id))

    if not notifiers:
        notifiers.append(PrintNotifier())

    if len(notifiers) == 1:
        return notifiers[0]
    return CompositeNotifier(notifiers)
