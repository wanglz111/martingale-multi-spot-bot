from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

import pandas as pd

from core.types import BarData, OrderResult, OrderSide, SignalAction, TradeSignal
from strategies.base import BaseStrategy


@dataclass
class PositionState:
    size: float = 0.0
    avg_price: float = 0.0
    levels: int = 0


logger = logging.getLogger(__name__)


class MartingaleStrategy(BaseStrategy):
    DEFAULTS: Dict = {
        "entry_logic": "MACD",
        "take_profit_percent": 5.0,
        "martingale_trigger": 10.0,
        "martingale_mult": 2.5,
        "base_position_pct": 0.05,
        "fixed_position": False,
        "start_position_size": 10.0,
        "max_levels": 4,
        "symbol": "BTCUSDT",
    }

    def __init__(self, **params):
        merged = {**self.DEFAULTS, **params}
        super().__init__(symbol=merged["symbol"], params=merged)
        self.history: Deque[BarData] = deque(maxlen=500)
        self.position = PositionState()
        self.last_signal: Optional[TradeSignal] = None

    def reset(self) -> None:
        super().reset()
        self.history.clear()
        self.position = PositionState()
        self.last_signal = None

    def _get_history_frame(self) -> Optional[pd.DataFrame]:
        if len(self.history) < 5:
            return None
        data = {
            "close": [bar.close for bar in self.history],
            "high": [bar.high for bar in self.history],
            "low": [bar.low for bar in self.history],
        }
        return pd.DataFrame(data)

    def _macd_signal(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
        closes = df["close"]
        ema_fast = closes.ewm(span=12, adjust=False).mean()
        ema_slow = closes.ewm(span=26, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        if len(macd_line) < 2:
            return False, {}
        decision = bool(macd_line.iloc[-1] > signal_line.iloc[-1] and macd_line.iloc[-2] <= signal_line.iloc[-2])
        metrics = {
            "macd": float(macd_line.iloc[-1]),
            "signal": float(signal_line.iloc[-1]),
            "prev_macd": float(macd_line.iloc[-2]),
            "prev_signal": float(signal_line.iloc[-2]),
        }
        return decision, metrics

    def _stoch_rsi_signal(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
        closes = df["close"]
        delta = closes.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        period = 14
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        stoch_rsi = (rsi - rsi.rolling(window=period).min()) / (rsi.rolling(window=period).max() - rsi.rolling(window=period).min())
        stoch_rsi = stoch_rsi * 100
        stoch_signal = stoch_rsi.rolling(window=3).mean()
        if len(stoch_signal) < 2:
            return False, {}
        current = stoch_rsi.iloc[-1]
        prev_signal = stoch_signal.iloc[-2]
        if pd.isna(current) or pd.isna(prev_signal):
            return False, {}
        decision = current > 20 and prev_signal <= 20
        metrics = {
            "stoch_rsi": float(current),
            "signal": float(stoch_signal.iloc[-1]),
            "prev_signal": float(prev_signal),
        }
        return decision, metrics

    def _atr_trend_signal(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
        closes = df["close"]
        ema_fast = closes.ewm(span=10, adjust=False).mean()
        ema_slow = closes.ewm(span=30, adjust=False).mean()
        if len(ema_fast) < 2:
            return False, {}
        decision = bool(ema_fast.iloc[-1] > ema_slow.iloc[-1] and ema_fast.iloc[-2] <= ema_slow.iloc[-2])
        metrics = {
            "ema_fast": float(ema_fast.iloc[-1]),
            "ema_slow": float(ema_slow.iloc[-1]),
            "prev_fast": float(ema_fast.iloc[-2]),
            "prev_slow": float(ema_slow.iloc[-2]),
        }
        return decision, metrics

    def _should_add_position(self, price: float) -> bool:
        trigger = self.params["martingale_trigger"]
        if self.position.size <= 0 or self.position.avg_price <= 0:
            return False
        drop_pct = (price - self.position.avg_price) / self.position.avg_price * 100
        if drop_pct > -trigger:
            return False
        if self.position.levels >= self.params["max_levels"]:
            return False
        return True

    def _should_take_profit(self, price: float) -> bool:
        take_profit = self.params["take_profit_percent"]
        if self.position.size <= 0 or self.position.avg_price <= 0:
            return False
        profit_pct = (price - self.position.avg_price) / self.position.avg_price * 100
        return profit_pct >= take_profit

    def on_bar(self, bar: BarData) -> TradeSignal:
        self.history.append(bar)
        df = self._get_history_frame()
        if df is None:
            logger.info(
                "Signal warm-up | symbol=%s | interval_history=%d | high=%.2f | low=%.2f | close=%.2f",
                self.symbol,
                len(self.history),
                bar.high,
                bar.low,
                bar.close,
            )
            return TradeSignal(action=SignalAction.HOLD)

        logic = self.params["entry_logic"].upper()
        buy_signal = False
        metrics: Dict[str, float] = {}
        if logic == "MACD":
            buy_signal, metrics = self._macd_signal(df)
        elif logic in {"STOCH", "STOCHRSI", "STOCH_RSI"}:
            buy_signal, metrics = self._stoch_rsi_signal(df)
        elif logic == "ATR":
            buy_signal, metrics = self._atr_trend_signal(df)

        price = bar.close
        if self.position.size <= 0:
            if buy_signal:
                signal = TradeSignal(action=SignalAction.ENTER, info={"reason": logic})
            else:
                signal = TradeSignal(action=SignalAction.HOLD)
        else:
            if self._should_take_profit(price):
                signal = TradeSignal(action=SignalAction.EXIT, info={"reason": "take_profit"})
            elif self._should_add_position(price):
                signal = TradeSignal(action=SignalAction.ADD, info={"level": self.position.levels + 1})
            else:
                signal = TradeSignal(action=SignalAction.HOLD)

        self.last_signal = signal
        logger.info(
            "Signal snapshot | symbol=%s | logic=%s | price=%.2f | metrics=%s | buy_signal=%s | action=%s",
            self.symbol,
            logic,
            price,
            metrics if metrics else {},
            buy_signal,
            signal.action.name,
        )
        return signal

    def on_order_fill(self, order: OrderResult) -> None:
        if order.side == OrderSide.BUY:
            total_cost = self.position.avg_price * self.position.size + (order.avg_price or 0) * order.filled_qty
            new_size = self.position.size + order.filled_qty
            avg_price = total_cost / new_size if new_size > 0 else 0
            self.position.size = new_size
            self.position.avg_price = avg_price
            self.position.levels += 1
        elif order.side == OrderSide.SELL:
            self.position = PositionState()
