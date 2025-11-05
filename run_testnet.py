import argparse
import asyncio
import logging
from copy import deepcopy

from core.config import load_config
from core.engine import TradingEngine
from core.portfolio import PortfolioManager
from notifiers import build_notifier
from services.binance_client import BinanceExchange
from strategies.martingale import MartingaleStrategy

logger = logging.getLogger(__name__)


async def run(config_path: str) -> None:
    cfg = load_config(config_path).raw
    exchange_cfg = cfg["exchange"]
    strategy_cfg = cfg["strategy"]
    risk_cfg = cfg.get("risk", {})
    notifications_cfg = cfg.get("notifications", {})

    exchange = BinanceExchange(
        api_key=exchange_cfg["api_key"],
        api_secret=exchange_cfg["api_secret"],
        testnet=exchange_cfg.get("mode", "").lower() == "testnet",
        recv_window=int(exchange_cfg.get("recv_window", 5000)),
    )
    notifier = build_notifier(notifications_cfg)
    symbols = exchange_cfg.get("symbols")
    if not symbols:
        fallback_symbol = strategy_cfg["params"].get("symbol") or exchange_cfg.get("symbol")
        if not fallback_symbol:
            raise ValueError("No symbols configured. Provide `exchange.symbols` or `strategy.params.symbol`.")
        symbols = [fallback_symbol]

    interval = exchange_cfg.get("interval", "1h")
    warmup_bars = int(exchange_cfg.get("warmup_bars", 4))
    cash_per_symbol = exchange_cfg.get("cash_per_symbol")
    total_cash = exchange_cfg.get("cash")

    tasks = []

    for symbol in symbols:
        params = deepcopy(strategy_cfg["params"])
        params["symbol"] = symbol
        strategy = MartingaleStrategy(**params)

        if cash_per_symbol is not None:
            initial_cash = float(cash_per_symbol)
        elif total_cash is not None:
            initial_cash = float(total_cash) / max(len(symbols), 1)
        else:
            initial_cash = 10000.0

        portfolio = PortfolioManager(strategy.symbol, initial_cash, strategy.params, risk_cfg)
        engine = TradingEngine(strategy, portfolio, exchange, notifier)

        logger.info(
            "Starting live martingale session | symbol=%s interval=%s testnet=%s cash=%.2f",
            symbol,
            interval,
            exchange.testnet,
            initial_cash,
        )
        tasks.append(
            asyncio.create_task(
                engine.run_live(
                    exchange.stream_klines(
                        symbol,
                        interval,
                        warmup_bars=warmup_bars,
                    )
                )
            )
        )

    try:
        await asyncio.gather(*tasks)
    except Exception:
        logger.exception("Live trading loop aborted due to exception.")
        for task in tasks:
            if not task.done():
                task.cancel()
        raise
    finally:
        logger.info("Shutting down exchange connection...")
        await exchange.close()
        logger.info("Exchange connection closed.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run martingale strategy on Binance testnet.")
    parser.add_argument(
        "-c",
        "--config",
        default="config/testnet.yaml",
        help="Path to testnet configuration file.",
    )
    args = parser.parse_args()
    try:
        logger.info("Loading configuration from %s", args.config)
        asyncio.run(run(args.config))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user, exiting...")


if __name__ == "__main__":
    main()
