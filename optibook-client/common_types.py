from datetime import datetime
from typing import List


class SingleSidedBooking:
    def __init__(self):
        self.username: str = ''
        self.instrument_id: str = ''
        self.price: float = 0.0
        self.volume: int = 0
        self.action: str = ''


class TradeTick:
    def __init__(self, *, timestamp=None, instrument_id=None, price=None, volume=None, aggressor_side=None, buyer=None, seller=None):
        self.timestamp: datetime = datetime(1970, 1, 1) if not timestamp else timestamp
        self.instrument_id: str = '' if not instrument_id else instrument_id
        self.price: float = 0.0 if not price else price
        self.volume: int = 0 if not volume else volume
        self.aggressor_side: str = '' if not aggressor_side else aggressor_side
        self.buyer: str = '' if not buyer else buyer
        self.seller: str = '' if not seller else seller


class PriceVolume:
    def __init__(self, price, volume):
        self.price = price
        self.volume = volume

    def __repr__(self):
        return f"[price_volume] price={str(self.price)}, volume={str(self.volume)}"

    def __eq__(self, other):
        if not isinstance(other, PriceVolume):
            return NotImplemented
        return self.price == other.price and self.volume == other.volume


class PriceBook:
    def __init__(self, *, timestamp=None, instrument_id=None, bids=None, asks=None):
        self.timestamp: datetime = datetime(1970, 1, 1) if not timestamp else timestamp
        self.instrument_id: str = '' if not instrument_id else instrument_id
        self.bids: List[PriceVolume] = [] if not bids else bids
        self.asks: List[PriceVolume] = [] if not asks else asks

    def __eq__(self, other):
        if not isinstance(other, PriceBook):
            return NotImplemented
        return self.instrument_id == other.instrument_id and self.bids == other.bids and self.asks == other.asks


class Trade:
    def __init__(self):
        self.order_id: int = 0
        self.instrument_id: str = ''
        self.price: float = 0.0
        self.volume: int = 0
        self.side: str = ''


class OrderStatus:
    def __init__(self):
        self.order_id: int = 0
        self.instrument_id: str = ''
        self.price: float = 0.0
        self.volume: int = 0
        self.side: str = ''


class Instrument:
    def __init__(self):
        self.id: str = ''
        self.tick_size: float = 0.0
        self.extra_info: dict = {}
        self.paused: bool = False
