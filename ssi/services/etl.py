from datetime import date, datetime, timedelta
from math import floor
from typing import List, Optional, Any
import time
from pyodbc import Connection, Cursor

import ssi.services.market as market_service
import ssi.services.db as db
from ssi.models.constants import PERIOD_TYPE
from ssi.models.market import Security, DailyStockPrice
import ssi.helpers.datetime_helper as datetime_helper
from ssi.config import SSI_API_CONFIG, DB_CONFIG

# --- BEGIN: ETL Securities --- #


def etl_securities(market: str = None):
    #total_securities: List(Security) = []
    total_records: int
    total_pages: int

    page_index: int = 0
    cumulative_records: int = 0

    conn, cursor = db.open_session(
        connection_string=DB_CONFIG.CONNECTION_STRING)
    try:
        create_securities_table(cursor)

        page_index += 1
        res = market_service.get_securities(
            market=market, page_index=page_index)
        if(res and res["success"] and res["data"] and res["totalRecord"]):
            total_records = res["totalRecord"]

            securities = [
                Security(
                    market=di["Market"],
                    symbol=di["Symbol"],
                    stock_name=di["StockName"],
                    stock_en_name=di["StockEnName"]
                ) for di in res["data"]]

            # total_securities.extend(securities)
            total_pages = floor(total_records / len(securities)) + 1
            cumulative_records += len(securities)

            load_securities_data(cursor=cursor, securities=securities)
            time.sleep(SSI_API_CONFIG.DEFAULT_REQUEST_API_DELAY_TIME)
            print(f"Page Index: {page_index}/{total_pages}")
            print(f"Records: {cumulative_records}/{total_records}")
        else:
            return

        while cumulative_records < total_records:
            page_index += 1
            res = market_service.get_securities(
                market=market, page_index=page_index)
            if(res and res["success"] and res["data"]):
                securities = [
                    Security(
                        market=di["Market"],
                        symbol=di["Symbol"],
                        stock_name=di["StockName"],
                        stock_en_name=di["StockEnName"]
                    ) for di in res["data"]]

                # total_securities.extend(securities)
                cumulative_records += len(securities)

                load_securities_data(cursor=cursor, securities=securities)
                time.sleep(SSI_API_CONFIG.DEFAULT_REQUEST_API_DELAY_TIME)
                print(f"Page Index: {page_index}/{total_pages}")
                print(f"Records: {cumulative_records}/{total_records}")
    except Exception as e:
        raise e
    finally:
        db.close_session(conn=conn, cursor=cursor)


def create_securities_table(cursor: Cursor):
    cursor.execute("""
    --- create schema stg
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='stg')
    EXEC ('CREATE SCHEMA stg');
    """)

    cursor.execute("""
    --- create table stg.security
    IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'stg' AND name like 'security')
    CREATE TABLE stg.security (
        id int identity(1,1) primary key,
        market nvarchar(20),
        symbol nvarchar(20),
        stock_name nvarchar(200),
        stock_en_name nvarchar(200)
    )
    """)
    cursor.commit()


def load_securities_data(cursor: Cursor, securities: List[Security]):
    cursor.execute("BEGIN TRANSACTION")
    for di in securities:
        cursor.execute("""
        INSERT INTO stg.security(market, symbol, stock_name, stock_en_name) VALUES
        (?,?,?,?)
        """, di.market, di.symbol, di.stock_name, di.stock_en_name)
    cursor.execute("COMMIT TRANSACTION")
    cursor.commit()


def get_securities_data(cursor: Cursor) -> List[str]:
    cursor.execute("SELECT symbol FROM stg.security")
    symbols = [row[0] for row in cursor.fetchall()]
    return symbols
# --- END: ETL Securities --- #

# --- BEGIN: ETL Daily Stock Price --- #


def etl_daily_stock_price(business_date: date, period_type: PERIOD_TYPE, from_date: Optional[date] = None, to_date: Optional[date] = None):
    conn, cursor = db.open_session(
        connection_string=DB_CONFIG.CONNECTION_STRING)
    symbols = get_securities_data(cursor)
    db.close_session(conn, cursor)

    for symbol in symbols:
        etl_daily_symbol_price(symbol, business_date,
                               period_type, from_date, to_date)


def etl_daily_symbols_price(symbols: List[str], business_date: date, period_type: PERIOD_TYPE, from_date: Optional[date] = None, to_date: Optional[date] = None):
    for symbol in symbols:
        etl_daily_symbol_price(symbol, business_date,
                               period_type, from_date, to_date)


def etl_daily_symbol_price(symbol: str, business_date: date, period_type: PERIOD_TYPE, from_date: Optional[date] = None, to_date: Optional[date] = None):
    # calculate and validate from_date, to_date
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(
            business_date=business_date, period_type=period_type)

    print(f"Symbol: {symbol}\r\nFrom Date: {from_date}\r\nTo Date: {to_date}")
    if not (from_date and to_date):
        return

    # from_date: date = datetime.strptime("2022-01-01", "%Y-%m-%d")
    # to_date: date = datetime.strptime("2022-01-31", "%Y-%m-%d")
    # periods = datetime_helper.get_n_days_timespan_periods(from_date, to_date, timespan_days=7)
    periods = datetime_helper.get_1_month_timespan_periods(
        from_date, to_date)

    conn, cursor = db.open_session(
        connection_string=DB_CONFIG.CONNECTION_STRING)
    try:
        create_daily_stock_price_table(cursor)

        for period in periods:  # get data from api in a period
            print(f"Period: {period['from_date']} {period['to_date']}")

            res = market_service.get_daily_stock_price(
                symbol, period['from_date'], period['to_date'])

            if(res and res["success"] and res["data"] and res["totalRecord"]):
                data_items = [
                    DailyStockPrice(
                        symbol=di["Symbol"],
                        trading_date=datetime.strptime(
                            di["TradingDate"], "%d/%m/%Y"),
                        price_change=di["PriceChange"],
                        per_price_change=di["PerPriceChange"],
                        ceiling_price=di["CeilingPrice"],
                        floor_price=di["FloorPrice"],
                        ref_price=di["RefPrice"],
                        open_price=di["OpenPrice"],
                        highest_price=di["HighestPrice"],
                        lowest_price=di["LowestPrice"],
                        close_price=di["ClosePrice"],
                        average_price=di["AveragePrice"],
                        close_price_adjusted=di["ClosePriceAdjusted"],
                        total_match_volume=di["TotalMatchVol"],
                        total_match_value=di["TotalMatchVal"],
                        total_deal_volume=di["TotalDealVol"],
                        total_deal_value=di["TotalDealVal"],
                        foreign_current_room=di["ForeignCurrentRoom"],
                        foreign_buy_volume=di["ForeignBuyVolTotal"],
                        foreign_buy_value=di["ForeignBuyValTotal"],
                        foreign_sell_volume=di["ForeignSellVolTotal"],
                        foreign_sell_value=di["ForeignSellValTotal"],
                        total_buy_trade_volume=di["TotalBuyTradeVol"],
                        total_buy_trade_value=di["TotalBuyTrade"],
                        total_sell_trade_volume=di["TotalSellTradeVol"],
                        total_sell_trade_value=di["TotalSellTrade"],
                        net_buy_sell_volume=di["NetBuySellVol"],
                        net_buy_sell_value=di["NetBuySellVal"],
                        total_traded_volume=di["TotalTradedVol"],
                        total_traded_value=di["TotalTradedValue"],

                        etl_date=date.today()
                    ) for di in res["data"]]

                data_items.sort(key=sort_daily_stock_price)

                delete_daily_symbol_price_data(
                    cursor, symbol, period['from_date'], period['to_date'])

                load_daily_symbol_price_data(
                    cursor=cursor, symbol=symbol, data_items=data_items)
            else:
                pass
                # print(res["data"])
            time.sleep(SSI_API_CONFIG.DEFAULT_REQUEST_API_DELAY_TIME)

    except Exception as e:
        print(e)
    finally:
        db.close_session(conn, cursor)


def create_daily_stock_price_table(cursor: Cursor):
    cursor.execute("""
    --- create schema stg
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='stg')
    EXEC ('CREATE SCHEMA stg');
    """)

    cursor.execute("""
    --- create table stg.daily_stock_price
    IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'stg' AND name like 'daily_stock_price')
    CREATE TABLE stg.daily_stock_price (
        id int identity(1,1) primary key,
        symbol nvarchar(20),
        trading_date date,
        price_change decimal(18,2),
        per_price_change decimal(18,2),
        ceiling_price decimal(18,2),
        floor_price decimal(18,2),
        ref_price decimal(18,2),
        open_price decimal(18,2),
        highest_price decimal(18,2),
        lowest_price decimal(18,2),
        close_price decimal(18,2),
        average_price decimal(18,2),
        close_price_adjusted decimal(18,2),
        total_match_volume decimal(18,2),
        total_match_value decimal(18,2),
        total_deal_volume decimal(18,2),
        total_deal_value decimal(18,2),
        foreign_current_room decimal(18,2),
        foreign_buy_volume decimal(18,2),
        foreign_buy_value decimal(18,2),
        foreign_sell_volume decimal(18,2),
        foreign_sell_value decimal(18,2),
        total_buy_trade_volume decimal(18,2),
        total_buy_trade_value decimal(18,2),
        total_sell_trade_volume decimal(18,2),
        total_sell_trade_value decimal(18,2),
        net_buy_sell_volume decimal(18,2),
        net_buy_sell_value decimal(18,2),
        total_traded_volume decimal(18,2),
        total_traded_value decimal(18,2),

        etl_date date
    )""")

    cursor.commit()


def delete_daily_symbol_price_data(cursor: Cursor, symbol: str, from_date: date, to_date: date):
    cursor.execute(
        "DELETE FROM stg.daily_stock_price WHERE symbol=? and trading_date >= ? and trading_date <= ?", symbol, from_date, to_date)
    cursor.commit()


def load_daily_symbol_price_data(cursor: Cursor, symbol: str, data_items: List[DailyStockPrice]):
    cursor.execute("BEGIN TRANSACTION")
    for di in data_items:
        cursor.execute("""
        INSERT INTO stg.daily_stock_price(symbol,trading_date,price_change,per_price_change,ceiling_price,floor_price,ref_price,open_price,highest_price,lowest_price,close_price,average_price,close_price_adjusted,
                    total_match_volume,total_match_value,total_deal_volume,total_deal_value,
                    foreign_current_room,foreign_buy_volume,foreign_buy_value,foreign_sell_volume,foreign_sell_value,
                    total_buy_trade_volume,total_buy_trade_value,total_sell_trade_volume,total_sell_trade_value,
                    net_buy_sell_volume,net_buy_sell_value,total_traded_volume,total_traded_value) VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                       di.symbol, di.trading_date, di.price_change, di.per_price_change, di.ceiling_price, di.floor_price, di.ref_price, di.open_price, di.highest_price, di.lowest_price, di.close_price, di.average_price, di.close_price_adjusted,
                       di.total_match_volume, di.total_match_value, di.total_deal_volume, di.total_deal_value,
                       di.foreign_current_room, di.foreign_buy_volume, di.foreign_buy_value, di.foreign_sell_volume, di.foreign_sell_value,
                       di.total_buy_trade_volume, di.total_buy_trade_value, di.total_sell_trade_volume, di.total_sell_trade_value,
                       di.net_buy_sell_volume, di.net_buy_sell_value, di.total_traded_volume, di.total_traded_value)

    cursor.execute("COMMIT TRANSACTION")
    cursor.commit()


def sort_daily_stock_price(e: DailyStockPrice):
    return e.trading_date
# --- END: ETL Daily Stock Price --- #


def etl_daily_index(business_date: date, period_type: PERIOD_TYPE, from_date: Optional[date] = None, to_date: Optional[date] = None):
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(
            business_date=business_date, period_type=period_type)
    else:
        pass
    if not (from_date and to_date):
        return
    print(f"From Date: {from_date}\r\nTo Date: {to_date}")
