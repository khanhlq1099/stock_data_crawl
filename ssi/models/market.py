from typing import List, Optional, Dict
from datetime import date, datetime

from pydantic import BaseModel


class Security(BaseModel):
    market: Optional[str]
    symbol: str
    stock_name: Optional[str]
    stock_en_name: Optional[str]


class DailyStockPrice(BaseModel):
    symbol: str
    trading_date: date
    price_change: float
    per_price_change: float
    ceiling_price: float
    floor_price: float
    ref_price: float
    open_price: float
    highest_price: float
    lowest_price: float
    close_price: float
    average_price: float
    close_price_adjusted: float
    total_match_volume: float
    total_match_value: float
    total_deal_volume: float
    total_deal_value: float
    foreign_current_room: float
    foreign_buy_volume: float
    foreign_buy_value: float
    foreign_sell_volume: float
    foreign_sell_value: float
    total_buy_trade_volume: float
    total_buy_trade_value: float
    total_sell_trade_volume: float
    total_sell_trade_value: float
    net_buy_sell_volume: float
    net_buy_sell_value: float
    total_traded_volume: float
    total_traded_value: float

    etl_date: date
