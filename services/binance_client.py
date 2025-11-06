from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import AsyncIterator, Dict, Optional, Tuple
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

    def execute_order(self, request: OrderRequest, bar: BarData) -> OrderResult:
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

        return OrderResult(
            order_id=str(response.get("orderId")),
            side=request.side,
            status=response.get("status", "FILLED"),
            filled_qty=float(response.get("executedQty", request.quantity)),
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

        qty = self._to_decimal(quantity)
        if qty <= 0:
            return None

        price_dec = self._to_decimal(price)
        lot_filter = self._select_lot_filter(filters, order_type)
        notional_filter = self._select_notional_filter(filters, order_type)

        qty = self._enforce_quantity_filters(qty, price_dec, lot_filter, notional_filter)
        if qty is None or qty <= 0:
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
    def _enforce_quantity_filters(
        self,
        qty: Decimal,
        price: Decimal,
        lot_filter: Optional[dict],
        notional_filter: Optional[dict],
    ) -> Optional[Decimal]:
        step = self._get_filter_decimal(lot_filter, "stepSize") if lot_filter else Decimal("0")
        min_qty = self._get_filter_decimal(lot_filter, "minQty") if lot_filter else Decimal("0")
        max_qty = self._get_filter_decimal(lot_filter, "maxQty") if lot_filter else Decimal("0")

        min_notional_qty = Decimal("0")
        if notional_filter:
            min_notional_value = self._get_notional_requirement(notional_filter)
            if min_notional_value > 0 and price > 0:
                min_notional_qty = min_notional_value / price

        target_qty = max(qty, min_qty, min_notional_qty)
        if step > 0:
            target_qty = self._round_to_step(target_qty, step, ROUND_UP)

        if max_qty > 0 and target_qty > max_qty:
            return None

        # Ensure notional requirement is still satisfied after rounding.
        if notional_filter:
            min_notional_value = self._get_notional_requirement(notional_filter)
            if min_notional_value > 0:
                target_qty = self._bump_until_notional(target_qty, price, step, min_notional_value, max_qty)
                if target_qty is None:
                    return None

        # Final guard to ensure minimum quantity is met.
        if min_qty > 0 and target_qty < min_qty:
            adjusted = self._round_to_step(min_qty, step, ROUND_UP) if step > 0 else min_qty
            if max_qty > 0 and adjusted > max_qty:
                return None
            target_qty = adjusted

        return target_qty

    @staticmethod
    def _round_to_step(value: Decimal, step: Decimal, rounding) -> Decimal:
        if step <= 0:
            return value
        quotient = (value / step).to_integral_value(rounding=rounding)
        rounded = (quotient * step).quantize(step, rounding=ROUND_DOWN)
        return rounded

    @staticmethod
    def _get_filter_decimal(filter_data: dict, key: str) -> Decimal:
        raw = filter_data.get(key)
        if raw in (None, ""):
            return Decimal("0")
        return Decimal(str(raw))

    @staticmethod
    def _get_notional_requirement(notional_filter: dict) -> Decimal:
        key = "minNotional" if "minNotional" in notional_filter else "notional"
        value = notional_filter.get(key)
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))

    @staticmethod
    def _bump_until_notional(
        qty: Decimal,
        price: Decimal,
        step: Decimal,
        min_notional: Decimal,
        max_qty: Decimal,
    ) -> Optional[Decimal]:
        if qty * price >= min_notional:
            return qty

        if price <= 0:
            return None

        if step <= 0:
            required = min_notional / price
            if max_qty > 0 and required > max_qty:
                return None
            return required

        current_qty = qty
        step_value = step
        increments_needed = ((min_notional - current_qty * price) / (price * step_value)).to_integral_value(
            rounding=ROUND_UP
        )
        current_qty += increments_needed * step_value
        current_qty = BinanceExchange._round_to_step(current_qty, step_value, ROUND_UP)

        if max_qty > 0 and current_qty > max_qty:
            return None

        if current_qty * price < min_notional:
            # Add one more step to ensure requirement due to rounding quirks.
            current_qty += step_value
            current_qty = BinanceExchange._round_to_step(current_qty, step_value, ROUND_UP)
            if max_qty > 0 and current_qty > max_qty:
                return None

        return current_qty

    @staticmethod
    def _to_decimal(value: float) -> Decimal:
        return Decimal(str(value))

    def _format_quantity(self, symbol: str, quantity: Decimal, lot_filter: Optional[dict]) -> str:
        precision = self._symbol_filters.get(symbol, {}).get("base_precision", 8)
        if lot_filter:
            step = self._get_filter_decimal(lot_filter, "stepSize")
            if step > 0:
                quantity = self._round_to_step(quantity, step, ROUND_DOWN)
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


async def run_stream(exchange: BinanceExchange, symbol: str, interval: str = "1h"):
    async for bar in exchange.stream_klines(symbol, interval):
        yield bar
