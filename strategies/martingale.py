from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque, Dict, Optional, Tuple

import pandas as pd

from core.types import BarData, OrderResult, OrderSide, SignalAction, TradeSignal
from strategies.base import BaseStrategy


@dataclass
class PositionState:
    size: float = 0.0
    avg_price: float = 0.0
    levels: int = 0
    entry_timestamp: Optional[datetime] = None


logger = logging.getLogger(__name__)


class MartingaleStrategy(BaseStrategy):
    DEFAULTS: Dict = {
        "entry_logic": "MACD",
        "take_profit_percent": 5.0,
        "take_profit_min_percent": 2.0,
        "take_profit_decay_hours": 240.0,
        "martingale_trigger": 10.0,
        "martingale_mult": 2.5,
        "base_position_pct": 0.05,
        "fixed_position": False,
        "start_position_size": 10.0,
        "max_levels": 4,
        "symbol": "ETHUSDT",
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

    @staticmethod
    def _rma(series: pd.Series, length: int) -> pd.Series:
        return series.ewm(alpha=1 / length, adjust=False).mean()

    @staticmethod
    def _rsi(series: pd.Series, length: int) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = MartingaleStrategy._rma(gain, length)
        avg_loss = MartingaleStrategy._rma(loss, length)
        rs = avg_gain / avg_loss.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _atr(df: pd.DataFrame, period: int) -> pd.Series:
        high = df["high"]
        low = df["low"]
        close = df["close"]
        prev_close = close.shift(1)
        tr_components = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        )
        true_range = tr_components.max(axis=1)
        atr = MartingaleStrategy._rma(true_range, period)
        return atr

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
        closes = df["close"].reset_index(drop=True)
        rsi = self._rsi(closes, 14)
        lowest = rsi.rolling(window=14).min()
        highest = rsi.rolling(window=14).max()
        denom = highest - lowest
        stoch = (rsi - lowest) / denom.replace(0, pd.NA)
        k = stoch.rolling(window=3).mean() * 100
        d = k.rolling(window=3).mean()
        if len(k) < 2 or len(d) < 2:
            return False, {}
        current_k = k.iloc[-1]
        prev_d = d.iloc[-2]
        current_d = d.iloc[-1]
        if pd.isna(current_k) or pd.isna(prev_d) or pd.isna(current_d):
            return False, {}
        decision = bool(current_k > 20 and prev_d <= 20 and current_d > 20)
        metrics = {
            "k": float(current_k),
            "d": float(current_d),
            "prev_d": float(prev_d),
        }
        return decision, metrics

    def _atr_trend_signal(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
        closes = df["close"].reset_index(drop=True)
        atr1 = self._atr(df, 5).reset_index(drop=True).fillna(0)
        atr2 = self._atr(df, 10).reset_index(drop=True).fillna(0)
        sl1 = 0.5 * atr1
        sl2 = 2.3 * atr2
        sl3 = 0.275 * 2.3 * atr2

        trail1_vals = []
        trail2_vals = []
        trail3_vals = []
        for idx, price in enumerate(closes):
            prev_price = closes.iloc[idx - 1] if idx > 0 else price

            prev_trail1 = trail1_vals[-1] if trail1_vals else 0.0
            prev_trail2 = trail2_vals[-1] if trail2_vals else 0.0
            prev_trail3 = trail3_vals[-1] if trail3_vals else 0.0

            sl1_val = float(sl1.iloc[idx]) if not pd.isna(sl1.iloc[idx]) else 0.0
            sl2_val = float(sl2.iloc[idx]) if not pd.isna(sl2.iloc[idx]) else 0.0
            sl3_val = float(sl3.iloc[idx]) if not pd.isna(sl3.iloc[idx]) else 0.0

            if price > prev_trail1 and prev_price > prev_trail1:
                trail1 = max(prev_trail1, price - sl1_val)
            elif price < prev_trail1 and prev_price < prev_trail1:
                trail1 = min(prev_trail1, price + sl1_val)
            elif price > prev_trail1:
                trail1 = price - sl1_val
            else:
                trail1 = price + sl1_val
            trail1_vals.append(trail1)

            if price > prev_trail2 and prev_price > prev_trail2:
                trail2 = max(prev_trail2, price - sl2_val)
            elif price < prev_trail2 and prev_price < prev_trail2:
                trail2 = min(prev_trail2, price + sl2_val)
            elif price > prev_trail2:
                trail2 = price - sl2_val
            else:
                trail2 = price + sl2_val
            trail2_vals.append(trail2)

            if price > prev_trail2 and prev_price > prev_trail2:
                trail3 = max(prev_trail3, price + sl3_val)
            elif price < prev_trail2 and prev_price < prev_trail2:
                trail3 = min(prev_trail3, price - sl3_val)
            elif price > prev_trail2:
                trail3 = price + sl3_val
            else:
                trail3 = price + sl3_val
            trail3_vals.append(trail3)

        hst = pd.Series(trail1_vals) - pd.Series(trail2_vals)
        sig = hst.ewm(span=9, adjust=False).mean()

        atr_blue = (hst < 0) & (hst > sig)
        atr_green = (hst > 0) & (hst > sig)
        atr_red = (hst < 0) & (hst < sig)

        def bars_since(series: pd.Series) -> pd.Series:
            counts = []
            last = float("inf")
            for value in series:
                if bool(value):
                    last = 0
                else:
                    last = last + 1 if last != float("inf") else float("inf")
                counts.append(last)
            return pd.Series(counts)

        bars_since_green = bars_since(atr_green)
        bars_since_red = bars_since(atr_red)
        atr_bear = bars_since_red < bars_since_green
        atr_buy_series = atr_green & atr_bear.shift(1).fillna(False)

        if atr_buy_series.empty:
            return False, {}

        decision = bool(atr_buy_series.iloc[-1])
        metrics = {
            "hst": float(hst.iloc[-1]),
            "sig": float(sig.iloc[-1]),
            "trail1": float(trail1_vals[-1]),
            "trail2": float(trail2_vals[-1]),
            "trail3": float(trail3_vals[-1]),
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

    def _current_take_profit_threshold(self, current_time: datetime) -> float:
        take_profit = float(self.params["take_profit_percent"])
        entry_time = self.position.entry_timestamp
        if entry_time is None or current_time <= entry_time:
            return take_profit

        tau = float(self.params.get("take_profit_decay_hours", 0) or 0)
        tp_min = float(self.params.get("take_profit_min_percent", take_profit))
        if tau <= 0:
            return max(tp_min, take_profit)

        elapsed_hours = (current_time - entry_time).total_seconds() / 3600
        if elapsed_hours <= 0:
            return take_profit

        decayed = take_profit * math.exp(-elapsed_hours / tau)
        return max(tp_min, decayed)

    def _should_take_profit(self, price: float, current_time: datetime) -> bool:
        take_profit = self._current_take_profit_threshold(current_time)
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
            if self._should_take_profit(price, bar.timestamp):
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
            previous_size = self.position.size
            total_cost = self.position.avg_price * previous_size + (order.avg_price or 0) * order.filled_qty
            new_size = previous_size + order.filled_qty
            avg_price = total_cost / new_size if new_size > 0 else 0
            self.position.size = new_size
            self.position.avg_price = avg_price
            self.position.levels += 1
            if new_size > 0:
                if self.position.entry_timestamp is None or previous_size <= 0:
                    entry_timestamp = order.timestamp
                else:
                    weight_new = order.filled_qty / new_size
                    prev_entry = self.position.entry_timestamp
                    entry_timestamp = prev_entry + (order.timestamp - prev_entry) * weight_new
                self.position.entry_timestamp = entry_timestamp
        elif order.side == OrderSide.SELL:
            self.position = PositionState()
