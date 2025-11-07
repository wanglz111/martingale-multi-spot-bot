import argparse
import asyncio
import logging
from copy import deepcopy
from datetime import datetime
from typing import AsyncIterator, Callable, Dict, Optional

from core.config import load_config
from core.engine import TradingEngine
from core.portfolio import PortfolioManager
from core.live_controller import LiveTradingController
from core.types import BarData
from notifiers import build_notifier
from services.account_sync import AccountSynchronizer, RemoteStatePersistence
from services.binance_client import BinanceExchange
from services.storage import CloudflareR2Storage
from strategies.martingale import MartingaleStrategy

logger = logging.getLogger(__name__)


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key] = deep_merge(dict(base[key]), value)
        else:
            base[key] = value
    return base


async def run(config_path: str) -> None:
    cfg = load_config(config_path).raw
    storage_cfg = cfg.get("cloud_storage") or {}
    storage = None

    if storage_cfg.get("account_id"):
        required_keys = ["account_id", "access_key_id", "secret_access_key", "bucket"]
        missing = [key for key in required_keys if not storage_cfg.get(key)]
        if missing:
            raise ValueError(f"cloud_storage missing required keys: {', '.join(missing)}")
        storage = CloudflareR2Storage(
            account_id=storage_cfg["account_id"],
            access_key_id=storage_cfg["access_key_id"],
            secret_access_key=storage_cfg["secret_access_key"],
            bucket=storage_cfg["bucket"],
            region=storage_cfg.get("region", "auto"),
            endpoint=storage_cfg.get("endpoint"),
        )
        params_key = storage_cfg.get("parameters_key")
        if params_key:
            remote_cfg = storage.load_json(params_key)
            cfg = deep_merge(cfg, remote_cfg)

    exchange_cfg = cfg["exchange"]
    strategy_cfg = cfg["strategy"]
    risk_cfg = cfg.get("risk", {})
    notifications_cfg = cfg.get("notifications", {})

    data_source = exchange_cfg.get("data_source", "binance")
    exchange = BinanceExchange(
        api_key=exchange_cfg["api_key"],
        api_secret=exchange_cfg["api_secret"],
        testnet=False,
        recv_window=int(exchange_cfg.get("recv_window", 5000)),
        data_source=data_source,
        ccxt_options=exchange_cfg.get("ccxt", {}),
    )

    notifier = build_notifier(notifications_cfg)
    symbols = exchange_cfg.get("symbols")
    if not symbols:
        fallback_symbol = strategy_cfg["params"].get("symbol") or exchange_cfg.get("symbol")
        if not fallback_symbol:
            raise ValueError("No symbols configured for live trading.")
        symbols = [fallback_symbol]
    interval = exchange_cfg.get("interval", "1h")
    cash_per_symbol = exchange_cfg.get("cash_per_symbol")
    total_cash = exchange_cfg.get("cash")
    enable_ticker = bool(exchange_cfg.get("enable_ticker_stream", True))

    try:
        await asyncio.to_thread(exchange.verify_account_permissions)
    except Exception:
        logger.exception("Exchange credentials validation failed.")
        raise

    controllers = []
    account_sync_cfg = cfg.get("account_sync", {})
    tolerance = float(account_sync_cfg.get("tolerance", 1e-6))
    sync_interval = int(account_sync_cfg.get("interval", 60))

    for symbol in symbols:
        params = deepcopy(strategy_cfg["params"])
        params["symbol"] = symbol
        strategy = MartingaleStrategy(**params)

        try:
            await asyncio.to_thread(exchange.ensure_symbol_tradable, symbol)
        except Exception:
            logger.exception("Symbol validation failed | symbol=%s", symbol)
            raise

        if cash_per_symbol is not None:
            initial_cash = float(cash_per_symbol)
        elif total_cash is not None:
            initial_cash = float(total_cash) / max(len(symbols), 1)
        else:
            initial_cash = 10000.0

        portfolio = PortfolioManager(strategy.symbol, initial_cash, strategy.params, risk_cfg)
        engine = TradingEngine(strategy, portfolio, exchange, notifier)

        base_asset, quote_asset = await asyncio.to_thread(exchange.fetch_symbol_components, symbol)

        persistence = None
        restored_state: Optional[Dict] = None
        if storage and storage_cfg.get("state_key_template"):
            state_key = storage_cfg["state_key_template"].format(symbol=symbol.upper())
            persistence = RemoteStatePersistence(storage, state_key)
            try:
                restored_state = persistence.load_state()
            except Exception:
                logger.exception("Failed to load remote state for %s", symbol)
                restored_state = None

        synchronizer = AccountSynchronizer(
            exchange,
            portfolio,
            symbol=symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            tolerance=tolerance,
            poll_interval=sync_interval,
            state_persistence=persistence,
        )

        if restored_state:
            try:
                synchronizer.bootstrap_state(restored_state)
            except Exception:
                logger.exception("Failed to bootstrap synchronizer state for %s", symbol)

        def price_stream_factory(sym=symbol):
            return exchange.stream_klines(sym, interval)

        ticker_stream_factory: Optional[Callable[[], AsyncIterator[float]]] = None
        ticker_transform: Optional[Callable[[float], Optional[BarData]]] = None
        if enable_ticker:
            def ticker_stream_factory(sym=symbol):
                return exchange.stream_symbol_ticker(sym)

            def ticker_transform(price: float, sym=symbol, port=portfolio) -> Optional[BarData]:
                if port.state.position <= 0:
                    return None
                now = datetime.utcnow()
                return BarData(
                    symbol=sym.upper(),
                    timestamp=now,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=0.0,
                )

        controller = LiveTradingController(
            symbol=symbol,
            price_stream_factory=price_stream_factory,
            engine=engine,
            account_sync_task=synchronizer.run,
            reconnect_interval=int(exchange_cfg.get("reconnect_interval", 5)),
            ticker_stream_factory=ticker_stream_factory,
            ticker_transform=ticker_transform,
            price_hook=synchronizer.update_market_price,
        )

        controllers.append(controller.start())

    try:
        await asyncio.gather(*controllers)
    finally:
        await exchange.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run martingale strategy on Binance live environment.")
    parser.add_argument(
        "-c",
        "--config",
        default="config/live.yaml",
        help="Path to live trading configuration file.",
    )
    args = parser.parse_args()
    try:
        logger.info("Loading configuration from %s", args.config)
        asyncio.run(run(args.config))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user, exiting...")


if __name__ == "__main__":
    main()
