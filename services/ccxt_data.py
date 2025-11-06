from __future__ import annotations

import asyncio
import csv
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Deque, Iterable, List, Optional

import ccxt

from core.types import BarData

logger = logging.getLogger(__name__)


@dataclass
class CCXTOptions:
    """Configuration options for CCXT-driven kline synchronisation."""

    exchange_id: str = "binance"
    limit: int = 720
    storage_dir: Path = Path("data/ccxt")
    poll_interval: Optional[float] = None
    enable_rate_limit: bool = True


class CCXTKlineDatabase:
    """Maintain a rolling OHLCV cache using ccxt and persist it locally."""

    def __init__(self, symbol: str, interval: str, options: Optional[CCXTOptions] = None):
        self.symbol = symbol.upper()
        self.interval = interval
        self.options = options or CCXTOptions()
        self._bars: Deque[BarData] = deque(maxlen=self.options.limit)
        self._lock = asyncio.Lock()
        self._initialised = False

        self.options.storage_dir.mkdir(parents=True, exist_ok=True)
        self._storage_path = self.options.storage_dir / f"{self.symbol}_{self.interval}.csv"

        self._exchange = self._build_exchange(self.options.exchange_id, self.options.enable_rate_limit)
        self._timeframe_ms = self._derive_timeframe_ms(self.interval)
        self._poll_interval = self._derive_poll_interval(self.options.poll_interval, self._timeframe_ms)

        self._load_existing_cache()

    async def stream(self) -> AsyncIterator[BarData]:
        await self._ensure_initial_sync()
        for bar in list(self._bars):
            yield bar

        while True:
            await asyncio.sleep(self._poll_interval)
            new_bars = await self._sync()
            if not new_bars:
                continue
            for bar in new_bars:
                yield bar

    async def close(self) -> None:
        if hasattr(self._exchange, "close"):
            await asyncio.to_thread(self._exchange.close)

    async def _ensure_initial_sync(self) -> None:
        if self._initialised:
            return
        await self._sync(force_full=not self._bars)
        self._initialised = True

    async def _sync(self, force_full: bool = False) -> List[BarData]:
        async with self._lock:
            since: Optional[int] = None
            limit: Optional[int] = None

            if force_full or not self._bars:
                limit = self.options.limit
            else:
                last_timestamp = int(self._bars[-1].timestamp.timestamp() * 1000)
                since = last_timestamp - self._timeframe_ms

            try:
                raw_klines = await asyncio.to_thread(
                    self._fetch_ohlcv,
                    since,
                    limit,
                )
            except ccxt.BaseError as exc:
                logger.warning(
                    "Failed to fetch OHLCV data via ccxt | symbol=%s | interval=%s | error=%s",
                    self.symbol,
                    self.interval,
                    exc,
                )
                return []

            new_bars: List[BarData] = []
            now_ms = int(time.time() * 1000)

            for entry in raw_klines:
                if len(entry) < 6:
                    continue
                open_time = int(entry[0])
                close_time = open_time + self._timeframe_ms
                if close_time > now_ms:
                    # Ignore incomplete candles.
                    continue
                bar_time = datetime.utcfromtimestamp(close_time / 1000)
                if self._bars and bar_time <= self._bars[-1].timestamp:
                    continue

                bar = BarData(
                    symbol=self.symbol,
                    timestamp=bar_time,
                    open=float(entry[1]),
                    high=float(entry[2]),
                    low=float(entry[3]),
                    close=float(entry[4]),
                    volume=float(entry[5]),
                )
                self._bars.append(bar)
                new_bars.append(bar)

            if new_bars:
                await asyncio.to_thread(self._persist_cache)
                logger.info(
                    "Updated %s OHLCV cache via ccxt | interval=%s | new_bars=%d | total=%d",
                    self.symbol,
                    self.interval,
                    len(new_bars),
                    len(self._bars),
                )

            return new_bars

    def _fetch_ohlcv(self, since: Optional[int], limit: Optional[int]) -> Iterable[List[float]]:
        params = {}
        if since is not None:
            params["since"] = since
        if limit is not None:
            params["limit"] = limit
        return self._exchange.fetch_ohlcv(
            self.symbol,
            timeframe=self.interval,
            since=params.get("since"),
            limit=params.get("limit"),
        )

    def _load_existing_cache(self) -> None:
        if not self._storage_path.exists():
            return
        try:
            with self._storage_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)[-self.options.limit :]
        except (OSError, csv.Error) as exc:
            logger.warning(
                "Failed to load cached OHLCV data | path=%s | error=%s",
                self._storage_path,
                exc,
            )
            return

        for row in rows:
            try:
                timestamp = datetime.fromisoformat(row["timestamp"])
                bar = BarData(
                    symbol=self.symbol,
                    timestamp=timestamp,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
            except (KeyError, ValueError) as exc:
                logger.debug("Skipping malformed cached OHLCV row | error=%s", exc)
                continue
            self._bars.append(bar)

    def _persist_cache(self) -> None:
        temp_path = self._storage_path.with_suffix(".tmp")
        try:
            with temp_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
                for bar in self._bars:
                    writer.writerow(
                        [
                            bar.timestamp.isoformat(),
                            f"{bar.open:.10f}",
                            f"{bar.high:.10f}",
                            f"{bar.low:.10f}",
                            f"{bar.close:.10f}",
                            f"{bar.volume:.10f}",
                        ]
                    )
            temp_path.replace(self._storage_path)
        except OSError as exc:
            logger.warning(
                "Failed to persist OHLCV cache | path=%s | error=%s",
                self._storage_path,
                exc,
            )

    @staticmethod
    def _build_exchange(exchange_id: str, enable_rate_limit: bool):
        try:
            exchange_cls = getattr(ccxt, exchange_id)
        except AttributeError as exc:
            raise ValueError(f"Unsupported ccxt exchange: {exchange_id}") from exc
        return exchange_cls({"enableRateLimit": enable_rate_limit})

    @staticmethod
    def _derive_timeframe_ms(interval: str) -> int:
        units = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
        }
        try:
            value = int(interval[:-1])
            unit = interval[-1]
            seconds = value * units[unit]
        except (ValueError, KeyError):
            seconds = 60
        return seconds * 1000

    @staticmethod
    def _derive_poll_interval(poll_interval: Optional[float], timeframe_ms: int) -> float:
        if poll_interval is not None and poll_interval > 0:
            return float(poll_interval)
        seconds = timeframe_ms / 1000
        derived = max(5.0, seconds / 4)
        return float(min(derived, 60.0))


__all__ = ["CCXTKlineDatabase", "CCXTOptions"]

