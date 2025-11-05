from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import List

import matplotlib.pyplot as plt
import pandas as pd

from core.config import load_config
from core.engine import TradingEngine
from core.portfolio import PortfolioManager
from core.types import BarData, OrderResult, OrderSide, SignalAction
from strategies.martingale import MartingaleStrategy


@dataclass
class BacktestMetrics:
    equity_curve: List[float]
    timestamps: List[datetime]
    trades: int
    final_equity: float
    return_pct: float
    max_drawdown: float


class BacktestExchange:
    def __init__(self):
        self._order_id = 0
        self.last_price = None

    def execute_order(self, request, bar):
        self._order_id += 1
        price = bar.close
        self.last_price = price
        return OrderResult(
            order_id=f"bt-{self._order_id}",
            side=request.side,
            status="filled",
            filled_qty=request.quantity,
            avg_price=price,
            timestamp=bar.timestamp,
            raw={"source": "backtest"},
        )


def _compute_drawdown(equity: List[float]) -> float:
    peak = float("-inf")
    max_dd = 0.0
    for value in equity:
        if value > peak:
            peak = value
        drawdown = (value - peak) / peak * 100 if peak > 0 else 0
        if drawdown < max_dd:
            max_dd = drawdown
    return abs(max_dd)


def _plot_results(
    bars: List[BarData],
    equity: List[float],
    buy_events: List[dict],
    output: str,
) -> None:
    if not bars or not equity:
        return

    timestamps = [bar.timestamp for bar in bars]
    closes = [bar.close for bar in bars]

    fig, (ax_price, ax_equity) = plt.subplots(2, 1, sharex=True, figsize=(12, 6))

    ax_price.plot(timestamps, closes, label="Close Price", color="tab:blue")
    for event in buy_events:
        ax_price.scatter(
            event["timestamp"],
            event["price"],
            marker="^",
            color="tab:green",
            s=60,
            label="Buy" if event is buy_events[0] else "",
        )
        annotation = f"{event['price']:.2f}\n{event['reason']}"
        ax_price.annotate(
            annotation,
            (event["timestamp"], event["price"]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=8,
            color="tab:green",
        )
    ax_price.set_ylabel("Price")
    ax_price.legend(loc="upper left")
    ax_price.grid(True, linestyle="--", alpha=0.3)

    ax_equity.plot(timestamps, equity, label="Equity", color="tab:orange")
    ax_equity.set_ylabel("Equity")
    ax_equity.legend(loc="upper left")
    ax_equity.grid(True, linestyle="--", alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(output, dpi=120)
    plt.close(fig)


def run_backtest(config_path: str = "config/backtest.yaml") -> BacktestMetrics:
    cfg = load_config(config_path).raw
    exchange_cfg = cfg["exchange"]
    strategy_cfg = cfg["strategy"]
    risk_cfg = cfg.get("risk", {})

    data_file = exchange_cfg["data_file"]
    if not os.path.exists(data_file):
        raise FileNotFoundError(f"CSV data not found: {data_file}")

    df = pd.read_csv(data_file, parse_dates=[0])
    df.columns = [col.strip().lower() for col in df.columns]
    required_cols = {"datetime", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError(f"CSV must contain columns: {required_cols}")

    bars: List[BarData] = []
    symbol = strategy_cfg["params"].get("symbol", exchange_cfg.get("symbol", "BTCUSDT"))
    for _, row in df.iterrows():
        bars.append(
            BarData(
                symbol=symbol,
                timestamp=row["datetime"].to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
        )

    initial_cash = float(exchange_cfg.get("cash", 10000))
    strategy = MartingaleStrategy(**strategy_cfg["params"])
    portfolio = PortfolioManager(symbol, initial_cash, strategy.params, risk_cfg)
    exchange = BacktestExchange()
    engine = TradingEngine(strategy, portfolio, exchange)

    equity_curve: List[float] = []
    trades = 0
    buy_events: List[dict] = []

    for bar in bars:
        pre_avg = portfolio.state.avg_price
        pre_pos = portfolio.state.position
        engine.process_bar(bar)
        snapshot = portfolio.snapshot(bar.close)
        equity_curve.append(snapshot["equity"])
        if pre_pos > 0 and portfolio.state.position == 0 and pre_avg > 0:
            trades += 1
        if portfolio.state.position > pre_pos:
            reason = "BUY"
            signal = getattr(strategy, "last_signal", None)
            if signal is not None and signal.info:
                if signal.action == SignalAction.ENTER:
                    reason = signal.info.get("reason", "ENTER")
                elif signal.action == SignalAction.ADD:
                    level = signal.info.get("level")
                    reason = f"Add L{level}" if level is not None else "ADD"
                else:
                    reason = signal.info.get("reason", signal.action.name)
            elif signal is not None:
                reason = signal.action.name
            price = exchange.last_price if exchange.last_price is not None else bar.close
            buy_events.append({"timestamp": bar.timestamp, "price": price, "reason": reason})

    final_equity = equity_curve[-1] if equity_curve else initial_cash
    return_pct = (final_equity / initial_cash - 1) * 100 if initial_cash > 0 else 0.0
    max_drawdown = _compute_drawdown(equity_curve)

    output_path = "equity_martingale.png"
    _plot_results(bars, equity_curve, buy_events, output_path)

    print(f"Final equity: {final_equity:.2f}")
    print(f"Return: {return_pct:.2f}%")
    print(f"Max drawdown: {max_drawdown:.2f}%")
    print(f"Trades closed: {trades}")
    print(f"Equity curve saved: {output_path}")

    return BacktestMetrics(
        equity_curve=equity_curve,
        timestamps=[bar.timestamp for bar in bars],
        trades=trades,
        final_equity=final_equity,
        return_pct=return_pct,
        max_drawdown=max_drawdown,
    )


if __name__ == "__main__":
    run_backtest()
