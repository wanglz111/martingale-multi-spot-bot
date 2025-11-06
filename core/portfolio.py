from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional

from core.types import OrderRequest, OrderResult, OrderSide, SignalAction, TradeSignal


@dataclass
class PortfolioState:
    cash: float
    position: float = 0.0
    avg_price: float = 0.0
    base_unit: float = 0.0
    levels: int = 0  # number of filled martingale levels
    last_entry_time: Optional[datetime] = None
    last_exit_time: Optional[datetime] = None

    def equity(self, price: float) -> float:
        return self.cash + self.position * price


class PortfolioManager:
    def __init__(self, symbol: str, initial_cash: float, strategy_params: Dict, risk_params: Optional[Dict] = None):
        self.symbol = symbol
        self.params = strategy_params
        self.state = PortfolioState(cash=initial_cash)
        self.risk = risk_params or {}
        self.qty_precision = self.params.get("quantity_precision", 6)
        self.min_qty = 10 ** (-self.qty_precision) if self.qty_precision >= 0 else 0.0

    def _cooldown_ok(self, timestamp: datetime) -> bool:
        cooldown = self.risk.get("cooldown_minutes", 0)
        if cooldown <= 0:
            return True
        if self.state.last_exit_time is None:
            return True
        return timestamp - self.state.last_exit_time >= timedelta(minutes=cooldown)

    def _max_notional(self) -> float:
        value = self.risk.get("max_notional")
        return float(value) if value else 0.0

    def _round_qty(self, qty: float) -> float:
        if qty <= 0:
            return 0.0
        if self.qty_precision <= 0:
            return float(int(qty))
        quant = Decimal("1") / (Decimal("10") ** self.qty_precision)
        return float(Decimal(str(qty)).quantize(quant, rounding=ROUND_DOWN))

    def _calc_entry_qty(self, price: float) -> float:
        if self.params.get("fixed_position", False):
            return float(self.params.get("start_position_size", 0.0))

        pct = float(self.params.get("base_position_pct", 0.0))
        if pct <= 0:
            return 0.0
        equity = self.state.equity(price)
        return equity * pct / price

    def _calc_add_qty(self, next_level: int) -> float:
        if self.state.base_unit <= 0:
            return 0.0
        mult = float(self.params.get("martingale_mult", 1.0))
        exponent = max(next_level - 1, 0)
        return self.state.base_unit * (mult ** exponent)

    def _would_exceed_notional(self, price: float, qty: float) -> bool:
        limit = self._max_notional()
        if limit <= 0:
            return False
        projected = (self.state.position + qty) * price
        return projected > limit

    def process_signal(self, signal: TradeSignal, price: float, timestamp: datetime) -> List[OrderRequest]:
        orders: List[OrderRequest] = []
        action = signal.action

        if action == SignalAction.ENTER and self.state.position == 0:
            if not self._cooldown_ok(timestamp):
                return orders
            raw_qty = self._calc_entry_qty(price)
            qty = self._round_qty(raw_qty)
            if qty <= 0 and raw_qty > 0:
                qty = raw_qty
            if qty <= 0 or self._would_exceed_notional(price, qty):
                return orders
            orders.append(OrderRequest(symbol=self.symbol, side=OrderSide.BUY, quantity=qty))
        elif action == SignalAction.ADD and self.state.position > 0:
            next_level = self.state.levels + 1
            if next_level > int(self.params.get("max_levels", 1)):
                return orders
            raw_qty = self._calc_add_qty(next_level)
            qty = self._round_qty(raw_qty)
            if qty <= 0 and raw_qty > 0:
                qty = raw_qty
            if qty <= 0 or self._would_exceed_notional(price, qty):
                return orders
            orders.append(OrderRequest(symbol=self.symbol, side=OrderSide.BUY, quantity=qty))
        elif action == SignalAction.EXIT and self.state.position > 0:
            qty = self._round_qty(self.state.position)
            remainder = self.state.position - qty
            if (qty <= 0 or 0 < remainder < self.min_qty) and self.state.position > 0:
                qty = self.state.position
            if qty > 0:
                orders.append(OrderRequest(symbol=self.symbol, side=OrderSide.SELL, quantity=qty))

        return orders

    def apply_fill(self, order: OrderResult, price: float, timestamp: datetime) -> None:
        if order.side == OrderSide.BUY:
            execution_price = order.avg_price or price
            cost = execution_price * order.filled_qty
            self.state.cash -= cost
            new_position = self.state.position + order.filled_qty
            if new_position > 0:
                weighted_cost = self.state.avg_price * self.state.position + execution_price * order.filled_qty
                self.state.avg_price = weighted_cost / new_position
            self.state.position = new_position
            self.state.last_entry_time = timestamp
            if self.state.levels == 0:
                self.state.base_unit = order.filled_qty
                self.state.levels = 1
            else:
                self.state.levels += 1
        elif order.side == OrderSide.SELL:
            execution_price = order.avg_price or price
            proceeds = execution_price * order.filled_qty
            self.state.cash += proceeds
            new_position = self.state.position - order.filled_qty
            self.state.position = max(new_position, 0.0)
            if self.state.position <= 1e-10:
                self.state.position = 0.0
            elif 0 < self.state.position < self.min_qty:
                extra_qty = self.state.position
                self.state.cash += execution_price * extra_qty
                order.filled_qty += extra_qty
                self.state.position = 0.0
            if self.state.position == 0.0:
                self.state.avg_price = 0.0
                self.state.base_unit = 0.0
                self.state.levels = 0
                self.state.last_exit_time = timestamp

    def snapshot(self, price: float) -> Dict:
        return {
            "cash": self.state.cash,
            "position": self.state.position,
            "avg_price": self.state.avg_price,
            "equity": self.state.equity(price),
            "levels": self.state.levels,
        }
