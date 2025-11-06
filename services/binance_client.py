from __future__ import annotations

import asyncio
import logging
from datetime import datetime
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
        self._ccxt_options = self._build_ccxt_options(ccxt_options or {})
        if self.testnet:
            self.client.API_URL = "https://testnet.binance.vision/api"

    def execute_order(self, request: OrderRequest, bar: BarData) -> OrderResult:
        params = {
            "symbol": request.symbol,
            "side": request.side.value,
            "type": request.order_type,
            "quantity": self._format_quantity(request.quantity),
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

    @staticmethod
    def _format_quantity(quantity: float) -> str:
        return f"{quantity:.8f}".rstrip("0").rstrip(".")

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
