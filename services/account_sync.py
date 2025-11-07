from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from core.portfolio import PortfolioManager

logger = logging.getLogger(__name__)


@dataclass
class AccountSnapshot:
    base_free: float
    quote_free: float
    update_time: datetime


class AccountSynchronizer:
    """Keeps the in-memory portfolio aligned with exchange balances."""

    def __init__(
        self,
        exchange,
        portfolio: PortfolioManager,
        *,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        tolerance: float = 1e-6,
        poll_interval: int = 60,
        state_persistence=None,
    ) -> None:
        self._exchange = exchange
        self._portfolio = portfolio
        self._symbol = symbol.upper()
        self._base_asset = base_asset.upper()
        self._quote_asset = quote_asset.upper()
        self._tolerance = tolerance
        self._poll_interval = poll_interval
        self._state_persistence = state_persistence
        self._last_snapshot: Optional[AccountSnapshot] = None
        self._lock = asyncio.Lock()
        self._last_price: Optional[float] = None

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.sync()
            except Exception:
                logger.exception("Account synchronization failed | symbol=%s", self._symbol)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)
            except asyncio.TimeoutError:
                continue

    async def sync(self) -> None:
        async with self._lock:
            snapshot = await asyncio.to_thread(self._fetch_balances)
            self._reconcile(snapshot)
            self._last_snapshot = snapshot
            if self._state_persistence:
                price = self._last_price if self._last_price is not None else self._portfolio.state.avg_price
                state = {
                    "symbol": self._symbol,
                    "portfolio": self._portfolio.snapshot(price),
                    "balances": {
                        "base": snapshot.base_free,
                        "quote": snapshot.quote_free,
                        "updated_at": snapshot.update_time.isoformat(),
                    },
                    "market_price": price,
                }
                try:
                    self._state_persistence.persist_state(state)
                except Exception:
                    logger.exception("Unable to persist synchronized state to remote storage")

    def _fetch_balances(self) -> AccountSnapshot:
        account = self._exchange.get_account_balances(
            base_asset=self._base_asset,
            quote_asset=self._quote_asset,
        )
        timestamp = datetime.utcnow()
        return AccountSnapshot(
            base_free=float(account["base"].get("free", 0.0)),
            quote_free=float(account["quote"].get("free", 0.0)),
            update_time=timestamp,
        )

    def _reconcile(self, snapshot: AccountSnapshot) -> None:
        position_diff = abs(self._portfolio.state.position - snapshot.base_free)
        cash_diff = abs(self._portfolio.state.cash - snapshot.quote_free)

        if position_diff > self._tolerance or cash_diff > self._tolerance:
            logger.warning(
                "Portfolio state drift detected | symbol=%s pos_diff=%.6f cash_diff=%.6f",
                self._symbol,
                position_diff,
                cash_diff,
            )
            self._portfolio.state.position = snapshot.base_free
            self._portfolio.state.cash = snapshot.quote_free

    @property
    def last_snapshot(self) -> Optional[AccountSnapshot]:
        return self._last_snapshot

    def update_market_price(self, price: float) -> None:
        self._last_price = price

    def bootstrap_state(self, state: Dict) -> None:
        portfolio_state = state.get("portfolio") or {}
        self._portfolio.restore_snapshot(portfolio_state)

        balances = state.get("balances") or {}
        updated_at = balances.get("updated_at")
        timestamp = None
        if updated_at:
            try:
                timestamp = datetime.fromisoformat(updated_at)
            except ValueError:
                timestamp = None
        base_balance = balances.get("base")
        quote_balance = balances.get("quote")
        try:
            base_value = float(base_balance) if base_balance is not None else 0.0
        except (TypeError, ValueError):
            base_value = 0.0
        try:
            quote_value = float(quote_balance) if quote_balance is not None else 0.0
        except (TypeError, ValueError):
            quote_value = 0.0
        if base_balance is not None or quote_balance is not None:
            self._last_snapshot = AccountSnapshot(
                base_free=base_value,
                quote_free=quote_value,
                update_time=timestamp or datetime.utcnow(),
            )

        market_price = state.get("market_price")
        if market_price is not None:
            try:
                self._last_price = float(market_price)
            except (TypeError, ValueError):
                self._last_price = None


class RemoteStatePersistence:
    def __init__(self, storage, key: str) -> None:
        self._storage = storage
        self._key = key

    def persist_state(self, state: Dict) -> None:
        self._storage.save_json(self._key, state)

    def load_state(self) -> Optional[Dict]:
        try:
            return self._storage.load_json(self._key)
        except RuntimeError as exc:
            message = str(exc)
            if "NoSuchKey" in message or "Not Found" in message:
                return None
            raise


__all__ = ["AccountSynchronizer", "RemoteStatePersistence", "AccountSnapshot"]
