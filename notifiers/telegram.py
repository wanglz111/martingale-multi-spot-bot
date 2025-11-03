from __future__ import annotations

import logging
from typing import Optional

import requests

from notifiers.base import Notifier


class TelegramNotifier(Notifier):
    def __init__(self, bot_token: str, chat_id: str, parse_mode: Optional[str] = None, timeout: int = 10):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.parse_mode = parse_mode or "Markdown"
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

    @property
    def _endpoint(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def _post(self, text: str) -> None:
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": self.parse_mode,
        }
        try:
            response = requests.post(self._endpoint, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except Exception as exc:
            self.logger.error("Telegram send failure: %s", exc)

    def send_trade(self, payload: dict) -> None:
        text = (
            f"*Trade Executed*\n"
            f"Side: `{payload.get('side')}`\n"
            f"Qty: `{payload.get('qty')}`\n"
            f"Price: `{payload.get('price')}`\n"
            f"Status: `{payload.get('status')}`\n"
            f"Order ID: `{payload.get('order_id')}`"
        )
        self._post(text)

    def send_alert(self, message: str, extra: Optional[dict] = None) -> None:
        text = f"*Alert*\n{message}"
        if extra:
            details = "\n".join(f"- `{k}`: `{v}`" for k, v in extra.items())
            text = f"{text}\n{details}"
        self._post(text)
