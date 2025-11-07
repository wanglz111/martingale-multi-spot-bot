from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import AsyncIterator, Callable, Optional

from core.engine import TradingEngine
from core.types import BarData

logger = logging.getLogger(__name__)


class LiveTradingController:
    """Coordinates price monitoring, trading, and state synchronization."""

    def __init__(
        self,
        *,
        symbol: str,
        price_stream_factory: Callable[[], AsyncIterator],
        engine: TradingEngine,
        account_sync_task,
        reconnect_interval: int = 5,
        price_transform: Optional[Callable[[float], BarData]] = None,
        ticker_stream_factory: Optional[Callable[[], AsyncIterator]] = None,
        ticker_transform: Optional[Callable[[float], Optional[BarData]]] = None,
        price_hook: Optional[Callable[[float], None]] = None,
    ) -> None:
        self._symbol = symbol.upper()
        self._price_stream_factory = price_stream_factory
        self._engine = engine
        self._account_sync_task = account_sync_task
        self._reconnect_interval = reconnect_interval
        self._price_transform = price_transform
        self._ticker_stream_factory = ticker_stream_factory
        self._ticker_transform = ticker_transform
        self._price_hook = price_hook
        self._stop_event = asyncio.Event()
        self._sync_task: Optional[asyncio.Task] = None
        self._price_task: Optional[asyncio.Task] = None
        self._ticker_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        logger.info("Live controller starting | symbol=%s", self._symbol)
        self._stop_event.clear()
        self._sync_task = asyncio.create_task(self._account_sync_task(self._stop_event))
        self._price_task = asyncio.create_task(self._price_loop())
        tasks = [self._sync_task, self._price_task]
        if self._ticker_stream_factory and self._ticker_transform:
            self._ticker_task = asyncio.create_task(self._ticker_loop())
            tasks.append(self._ticker_task)
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._stop_event.is_set():
            return
        logger.info("Stopping live controller | symbol=%s", self._symbol)
        self._stop_event.set()
        for task in (self._price_task, self._ticker_task, self._sync_task):
            if task:
                task.cancel()
        for task in (self._price_task, self._ticker_task, self._sync_task):
            if task:
                with suppress(asyncio.CancelledError):
                    await task
        self._price_task = None
        self._ticker_task = None
        self._sync_task = None

    async def request_reconnect(self) -> None:
        logger.warning("Manual reconnect requested | symbol=%s", self._symbol)
        for task in (self._price_task, self._ticker_task):
            if task:
                task.cancel()
        if self._price_task:
            self._price_task = asyncio.create_task(self._price_loop())
        if self._ticker_task and self._ticker_stream_factory and self._ticker_transform:
            self._ticker_task = asyncio.create_task(self._ticker_loop())

    async def _price_loop(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            attempt += 1
            try:
                stream = self._price_stream_factory()
                async for payload in stream:
                    if self._stop_event.is_set():
                        break
                    bar = payload if isinstance(payload, BarData) else self._ensure_bar(payload)
                    if self._price_hook:
                        self._price_hook(bar.close)
                    self._engine.process_bar(bar)
                attempt = 0
            except Exception as exc:
                logger.exception(
                    "Price stream failed | symbol=%s attempt=%s error=%s",
                    self._symbol,
                    attempt,
                    exc,
                )
                await asyncio.sleep(min(self._reconnect_interval * attempt, 30))

    def _ensure_bar(self, payload) -> BarData:
        if isinstance(payload, BarData):
            return payload
        if self._price_transform is None:
            raise TypeError("Price transform is required when the stream does not yield BarData")
        return self._price_transform(payload)

    async def _ticker_loop(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            attempt += 1
            try:
                stream = self._ticker_stream_factory()
                async for price in stream:
                    if self._stop_event.is_set():
                        break
                    if self._ticker_transform:
                        bar = self._ticker_transform(price)
                        if bar is not None:
                            if self._price_hook:
                                self._price_hook(bar.close)
                            self._engine.process_bar(bar)
                attempt = 0
            except Exception as exc:
                logger.exception(
                    "Ticker stream failed | symbol=%s attempt=%s error=%s",
                    self._symbol,
                    attempt,
                    exc,
                )
                await asyncio.sleep(min(self._reconnect_interval * attempt, 30))


__all__ = ["LiveTradingController"]
