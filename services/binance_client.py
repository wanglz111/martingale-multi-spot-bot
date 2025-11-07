from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import AsyncIterator, Dict, Optional, Tuple

import time
from binance.async_client import AsyncClient
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.ws.streams import BinanceSocketManager

from core.types import BarData, OrderRequest, OrderResult
from services.ccxt_data import CCXTKlineDatabase, CCXTOptions

logger = logging.getLogger(__name__)


class BinanceExchange:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
        recv_window: int = 5000,
        data_source: str = "binance",
        ccxt_options: Optional[dict] = None,
    ):
        self.client = Client(api_key=api_key, api_secret=api_secret, testnet=testnet)
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.recv_window = recv_window
        self._async_client: Optional[AsyncClient] = None
        self._socket_manager: Optional[BinanceSocketManager] = None
        self.data_source = data_source.lower()
        self._ccxt_feeds: Dict[Tuple[str, str], CCXTKlineDatabase] = {}
        self._symbol_filters: Dict[str, Dict[str, dict]] = {}
        self._ccxt_options = self._build_ccxt_options(ccxt_options or {})
        if self.testnet:
            self.client.API_URL = "https://testnet.binance.vision/api"

    def execute_order(
        self,
        request: OrderRequest,
        bar: BarData,
        *,
        confirm_execution: bool = True,
        confirmation_retries: int = 3,
    ) -> OrderResult:
        quantity = self._prepare_quantity(
            symbol=request.symbol,
            quantity=request.quantity,
            price=bar.close,
            order_type=request.order_type,
        )

        if quantity is None:
            raise RuntimeError(
                "Unable to submit order because the calculated quantity does not satisfy Binance trading filters."
            )

        params = {
            "symbol": request.symbol,
            "side": request.side.value,
            "type": request.order_type,
            "quantity": quantity,
            "recvWindow": self.recv_window,
        }
        try:
            response = self.client.create_order(**params)
        except BinanceAPIException as exc:
            if exc.code == -2015:
                raise PermissionError(
                    "Binance rejected the order due to invalid API key, IP restriction, or missing trade permissions."
                ) from exc
            raise RuntimeError(f"Binance rejected the order: {exc.message}") from exc
        fills = response.get("fills", [])
        if fills:
            total_qty = sum(float(fill["qty"]) for fill in fills)
            total_cost = sum(float(fill["price"]) * float(fill["qty"]) for fill in fills)
            avg_price = total_cost / total_qty if total_qty else bar.close
        else:
            avg_price = bar.close

        order_id = str(response.get("orderId"))
        status = response.get("status", "FILLED")
        filled_qty = float(response.get("executedQty", request.quantity))
        if confirm_execution and order_id:
            verified = self._confirm_order(
                symbol=request.symbol,
                order_id=order_id,
                max_attempts=max(confirmation_retries, 1),
            )
            status = verified.get("status", status)
            filled_qty = float(verified.get("executedQty", filled_qty))
            if filled_qty <= 0:
                filled_qty = float(response.get("origQty", request.quantity))

        return OrderResult(
            order_id=order_id,
            side=request.side,
            status=status,
            filled_qty=filled_qty,
            avg_price=avg_price,
            timestamp=datetime.utcnow(),
            raw=response,
        )

    async def stream_klines(
        self,
        symbol: str,
        interval: str = "1h",
    ) -> AsyncIterator[BarData]:
        symbol_upper = symbol.upper()
        if self.data_source == "ccxt":
            feed = self._get_ccxt_feed(symbol_upper, interval)
            async for bar in feed.stream():
                yield bar
            return

        last_timestamp: Optional[datetime] = None

        async_client = await self._get_async_client()
        manager = self._get_socket_manager(async_client)
        socket = manager.kline_socket(symbol.lower(), interval=interval)
        async with socket as stream:
            while True:
                message = await stream.recv()
                if not isinstance(message, dict):
                    continue
                data = message.get("k", {})
                if not data or not data.get("x"):
                    continue
                timestamp = datetime.fromtimestamp(data["T"] / 1000)
                if last_timestamp and timestamp <= last_timestamp:
                    continue
                bar = BarData(
                    symbol=symbol_upper,
                    timestamp=timestamp,
                    open=float(data["o"]),
                    high=float(data["h"]),
                    low=float(data["l"]),
                    close=float(data["c"]),
                    volume=float(data["v"]),
                )
                last_timestamp = bar.timestamp
                yield bar

    async def stream_symbol_ticker(self, symbol: str) -> AsyncIterator[float]:
        async_client = await self._get_async_client()
        manager = self._get_socket_manager(async_client)
        socket = manager.symbol_ticker_socket(symbol.lower())
        async with socket as stream:
            while True:
                message = await stream.recv()
                if not isinstance(message, dict):
                    continue
                data = message.get("data") or message
                price = data.get("c") or data.get("lastPrice")
                if price is None:
                    continue
                yield float(price)

    def verify_account_permissions(self) -> None:
        try:
            account = self.client.get_account(recvWindow=self.recv_window)
        except BinanceAPIException as exc:
            if exc.code == -2015:
                raise PermissionError(
                    "Unable to verify Binance account permissions: invalid API key, IP restrictions, or missing trade rights."
                ) from exc
            raise RuntimeError(f"Unable to verify Binance account permissions: {exc.message}") from exc

        if not account.get("canTrade", True):
            raise PermissionError("The configured Binance account is not permitted to trade.")

        permissions = set(account.get("permissions", []))
        if permissions and "SPOT" not in permissions:
            raise PermissionError(
                "The configured Binance API key lacks SPOT trading permission. Enable 'Enable Spot & Margin Trading'."
            )

        logger.info("Binance API key verified successfully. Trading permissions confirmed.")

    def ensure_symbol_tradable(self, symbol: str) -> None:
        info = self.client.get_symbol_info(symbol.upper())
        if not info:
            raise ValueError(f"Symbol {symbol} is not available on Binance.")
        status = info.get("status")
        if status != "TRADING":
            raise PermissionError(f"Symbol {symbol} is not tradable on Binance (status={status}).")

    def fetch_symbol_components(self, symbol: str) -> Tuple[str, str]:
        info = self.client.get_symbol_info(symbol.upper())
        if not info:
            raise ValueError(f"Symbol {symbol} is not available on Binance.")
        return info.get("baseAsset"), info.get("quoteAsset")

    def get_account_balances(self, base_asset: str, quote_asset: str) -> Dict[str, dict]:
        account = self.client.get_account(recvWindow=self.recv_window)
        balances = account.get("balances", [])
        lookup = {item.get("asset"): item for item in balances}
        return {
            "base": lookup.get(base_asset.upper(), {"free": 0, "locked": 0}),
            "quote": lookup.get(quote_asset.upper(), {"free": 0, "locked": 0}),
        }

    def get_order(self, symbol: str, order_id: str) -> Dict:
        return self.client.get_order(symbol=symbol.upper(), orderId=order_id)

    def _get_ccxt_feed(self, symbol: str, interval: str) -> CCXTKlineDatabase:
        key = (symbol, interval)
        if key not in self._ccxt_feeds:
            options = CCXTOptions(
                exchange_id=self._ccxt_options.exchange_id,
                limit=self._ccxt_options.limit,
                storage_dir=self._ccxt_options.storage_dir,
                poll_interval=self._ccxt_options.poll_interval,
                enable_rate_limit=self._ccxt_options.enable_rate_limit,
            )
            self._ccxt_feeds[key] = CCXTKlineDatabase(symbol, interval, options)
        return self._ccxt_feeds[key]

    async def close(self) -> None:
        self._socket_manager = None
        if self._async_client:
            await self._async_client.close_connection()
            self._async_client = None
        if self._ccxt_feeds:
            for feed in self._ccxt_feeds.values():
                await feed.close()
            self._ccxt_feeds.clear()

    async def _get_async_client(self) -> AsyncClient:
        if self._async_client is None:
            self._async_client = await AsyncClient.create(
                api_key=self.api_key,
                api_secret=self.api_secret,
                testnet=self.testnet,
            )
        return self._async_client

    def _get_socket_manager(self, async_client: AsyncClient) -> BinanceSocketManager:
        if self._socket_manager is None:
            self._socket_manager = BinanceSocketManager(async_client)
        return self._socket_manager

    def _prepare_quantity(
        self, symbol: str, quantity: float, price: float, order_type: str
    ) -> Optional[str]:
        symbol_upper = symbol.upper()
        filters = self._get_symbol_filters(symbol_upper)

        qty = Decimal(str(quantity))
        if qty <= 0:
            return None

        lot_filter = self._select_lot_filter(filters, order_type)
        if lot_filter:
            qty = self._apply_lot_size(qty, lot_filter)
        if qty <= 0:
            return None

        notional_filter = self._select_notional_filter(filters, order_type)
        if notional_filter:
            qty = self._apply_min_notional(qty, Decimal(str(price)), lot_filter, notional_filter)
        if qty <= 0:
            return None

        formatted = self._format_quantity(symbol_upper, qty, lot_filter)
        return formatted if formatted else None

    def _get_symbol_filters(self, symbol: str) -> Dict[str, dict]:
        if symbol not in self._symbol_filters:
            info = self.client.get_symbol_info(symbol)
            if not info:
                raise ValueError(f"Symbol {symbol} is not available on Binance.")
            filters = {f.get("filterType"): f for f in info.get("filters", []) if f.get("filterType")}
            base_precision = int(info.get("baseAssetPrecision", 8))
            self._symbol_filters[symbol] = {"filters": filters, "base_precision": base_precision}
        return self._symbol_filters[symbol]

    @staticmethod
    def _select_lot_filter(filters: Dict[str, dict], order_type: str) -> Optional[dict]:
        filter_map = filters.get("filters", {})
        if order_type.upper() == "MARKET" and "MARKET_LOT_SIZE" in filter_map:
            return filter_map["MARKET_LOT_SIZE"]
        return filter_map.get("LOT_SIZE")

    @staticmethod
    def _select_notional_filter(filters: Dict[str, dict], order_type: str) -> Optional[dict]:
        filter_map = filters.get("filters", {})
        notional = filter_map.get("MIN_NOTIONAL") or filter_map.get("NOTIONAL")
        if notional is None:
            return None
        if order_type.upper() == "MARKET":
            if notional.get("applyToMarket", True):
                return notional
            return None
        return notional

    @staticmethod
    def _apply_lot_size(qty: Decimal, lot_filter: dict) -> Decimal:
        step = Decimal(str(lot_filter.get("stepSize", "0")))
        min_qty = Decimal(str(lot_filter.get("minQty", "0")))
        max_qty = Decimal(str(lot_filter.get("maxQty", "0")))

        if step > 0:
            quotient = (qty / step).to_integral_value(rounding=ROUND_DOWN)
            qty = (quotient * step).quantize(step, rounding=ROUND_DOWN)

        if min_qty > 0 and qty < min_qty:
            if step > 0:
                qty = BinanceExchange._round_to_step(min_qty, step, ROUND_UP)
            else:
                qty = min_qty

        if max_qty > 0 and qty > max_qty:
            qty = max_qty

        return qty

    @staticmethod
    def _apply_min_notional(
        qty: Decimal,
        price: Decimal,
        lot_filter: Optional[dict],
        notional_filter: dict,
    ) -> Decimal:
        min_notional_key = "minNotional" if "minNotional" in notional_filter else "notional"
        if notional_filter.get(min_notional_key) is None:
            return qty

        min_notional = Decimal(str(notional_filter[min_notional_key]))
        if min_notional <= 0:
            return qty

        notional = qty * price
        if notional >= min_notional:
            return qty

        required_qty = min_notional / price
        step = Decimal(str(lot_filter.get("stepSize", "0"))) if lot_filter else Decimal("0")
        if step > 0:
            qty = BinanceExchange._round_to_step(required_qty, step, ROUND_UP)
        else:
            qty = required_qty
        return qty

    @staticmethod
    def _round_to_step(value: Decimal, step: Decimal, rounding) -> Decimal:
        if step <= 0:
            return value
        quotient = (value / step).to_integral_value(rounding=rounding)
        rounded = (quotient * step).quantize(step, rounding=ROUND_DOWN)
        return rounded

    def _format_quantity(self, symbol: str, quantity: Decimal, lot_filter: Optional[dict]) -> str:
        precision = self._symbol_filters.get(symbol, {}).get("base_precision", 8)
        if lot_filter:
            step = Decimal(str(lot_filter.get("stepSize", "0")))
            if step > 0:
                precision = max(precision, -step.normalize().as_tuple().exponent)
        formatted = f"{quantity:.{precision}f}"
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted

    @staticmethod
    def _build_ccxt_options(raw: dict) -> CCXTOptions:
        options = CCXTOptions()
        if "exchange" in raw:
            options.exchange_id = str(raw["exchange"]).lower()
        if "limit" in raw:
            options.limit = max(int(raw["limit"]), 1)
        if "storage_dir" in raw:
            options.storage_dir = Path(raw["storage_dir"])
        if "poll_interval" in raw:
            try:
                options.poll_interval = float(raw["poll_interval"])
            except (TypeError, ValueError):
                options.poll_interval = None
        if "enable_rate_limit" in raw:
            options.enable_rate_limit = bool(raw["enable_rate_limit"])
        return options

    def _confirm_order(self, symbol: str, order_id: str, max_attempts: int) -> Dict:
        attempts = min(max_attempts, 5)
        last_exc: Optional[Exception] = None
        for _ in range(attempts):
            try:
                return self.get_order(symbol, order_id)
            except BinanceAPIException as exc:
                last_exc = exc
                time.sleep(0.5)
        if last_exc:
            raise last_exc
        return {}


async def run_stream(exchange: BinanceExchange, symbol: str, interval: str = "1h"):
    async for bar in exchange.stream_klines(symbol, interval):
        yield bar
