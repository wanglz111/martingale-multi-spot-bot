from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from datetime import datetime


class SignalAction(Enum):
    ENTER = auto()
    ADD = auto()
    EXIT = auto()
    HOLD = auto()


@dataclass
class BarData:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class TradeSignal:
    action: SignalAction
    size: float = 0.0
    price: Optional[float] = None
    info: Optional[dict] = None


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class OrderRequest:
    symbol: str
    side: OrderSide
    quantity: float
    order_type: str = "MARKET"


@dataclass
class OrderResult:
    order_id: str
    side: OrderSide
    status: str
    filled_qty: float
    avg_price: Optional[float]
    timestamp: datetime
    raw: dict
