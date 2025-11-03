from __future__ import annotations

from typing import Iterable, Optional

from core.portfolio import PortfolioManager
from core.types import BarData, OrderRequest, OrderResult, TradeSignal


class TradingEngine:
    def __init__(
        self,
        strategy,
        portfolio: PortfolioManager,
        exchange,
        notifier=None,
    ):
        self.strategy = strategy
        self.portfolio = portfolio
        self.exchange = exchange
        self.notifier = notifier

    def process_bar(self, bar: BarData) -> None:
        signal: TradeSignal = self.strategy.on_bar(bar)
        orders: Iterable[OrderRequest] = self.portfolio.process_signal(signal, bar.close, bar.timestamp)
        for order in orders:
            result: OrderResult = self.exchange.execute_order(order, bar)
            self.portfolio.apply_fill(result, bar.close, bar.timestamp)
            self.strategy.on_order_fill(result)
            self._notify_trade(result)

    def _notify_trade(self, result: OrderResult) -> None:
        if self.notifier is None:
            return
        payload = {
            "order_id": result.order_id,
            "side": result.side.value,
            "qty": result.filled_qty,
            "price": result.avg_price,
            "status": result.status,
        }
        self.notifier.send_trade(payload)

    def run_backtest(self, bars: Iterable[BarData]) -> None:
        for bar in bars:
            self.process_bar(bar)

    async def run_live(self, stream) -> None:
        async for bar in stream:
            self.process_bar(bar)
