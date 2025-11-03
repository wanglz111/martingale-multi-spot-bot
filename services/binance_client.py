from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator, Optional

from binance.async_client import AsyncClient
from binance.client import Client
from binance.ws.streams import BinanceSocketManager

from core.types import BarData, OrderRequest, OrderResult


class BinanceExchange:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
        recv_window: int = 5000,
    ):
        self.client = Client(api_key=api_key, api_secret=api_secret, testnet=testnet)
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.recv_window = recv_window
        self._async_client: Optional[AsyncClient] = None
        self._socket_manager: Optional[BinanceSocketManager] = None
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
        response = self.client.create_order(**params)
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

    async def stream_klines(self, symbol: str, interval: str = "1h") -> AsyncIterator[BarData]:
        symbol_upper = symbol.upper()
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
                yield BarData(
                    symbol=symbol_upper,
                    timestamp=datetime.fromtimestamp(data["T"] / 1000),
                    open=float(data["o"]),
                    high=float(data["h"]),
                    low=float(data["l"]),
                    close=float(data["c"]),
                    volume=float(data["v"]),
                )

    async def close(self) -> None:
        self._socket_manager = None
        if self._async_client:
            await self._async_client.close_connection()
            self._async_client = None

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


async def run_stream(exchange: BinanceExchange, symbol: str, interval: str = "1h"):
    async for bar in exchange.stream_klines(symbol, interval):
        yield bar
