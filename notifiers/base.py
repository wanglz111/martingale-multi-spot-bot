from __future__ import annotations

from typing import Iterable, Optional


class Notifier:
    def send_trade(self, payload: dict) -> None:
        """Send trade execution notification."""

    def send_alert(self, message: str, extra: Optional[dict] = None) -> None:
        """Send generic alert."""


class PrintNotifier(Notifier):
    def send_trade(self, payload: dict) -> None:
        print(f"[TRADE] {payload}")

    def send_alert(self, message: str, extra: Optional[dict] = None) -> None:
        print(f"[ALERT] {message} | {extra or {}}")


class CompositeNotifier(Notifier):
    def __init__(self, notifiers: Iterable[Notifier]):
        self._notifiers = list(notifiers)

    def send_trade(self, payload: dict) -> None:
        for notifier in self._notifiers:
            notifier.send_trade(payload)

    def send_alert(self, message: str, extra: Optional[dict] = None) -> None:
        for notifier in self._notifiers:
            notifier.send_alert(message, extra)
