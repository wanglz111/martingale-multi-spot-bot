from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from core.types import BarData, TradeSignal


class BaseStrategy(ABC):
    def __init__(self, symbol: str, params: Optional[Dict] = None):
        self.symbol = symbol
        self.params = params or {}
        self.state: Dict[str, Any] = {}

    def setup(self) -> None:
        """Hook executed once after instantiation."""

    def teardown(self) -> None:
        """Hook executed when strategy stops."""

    @abstractmethod
    def on_bar(self, bar: BarData) -> TradeSignal:
        """Return the desired action for the current bar."""

    def on_order_fill(self, order: Any) -> None:
        """Receive fills or order updates."""

    def reset(self) -> None:
        """Clear state between backtests."""
        self.state.clear()
