
from datetime import date, datetime
import time
from typing import Optional

import pandas as pd
from pyodbc import Connection, Cursor

from stock.config import ROOT_DIR, SQL_SERVER_CONFIG
from stock.module.ssi.constants import MARKET_API
from stock.lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE
import stock.lib.core.service as stock_service
import stock.lib.datetime_helper as datetime_helper
import stock.lib.sql_server as db
import stock.module.ssi.api as ssi_api


def etl_daily_stock_price(data_destination_type: DATA_DESTINATION_TYPE,
                          period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                          from_date: Optional[date] = None, to_date: Optional[date] = None):
    """
    Task: ETL Daily Stock Price
    ✓ Thu thập thông tin giá cổ phiếu/chỉ số hàng ngày (số liệu chốt cuối ngày) của các mã trên thị trường từ Cafef.
    ✓ Dữ liệu có thể lấy theo chu kỳ tháng, quý, năm, từ đầu tháng đến hiện tại, từ đầu năm đến hiện tại, từ đầu quý đến hiện tại.
    ✓ Lưu trữ dữ liệu thu thập được vào cơ sở dữ liệu SQL Server
    - Lưu trữ dữ liệu thu thập được vào Sharepoint
    ✗ Lưu trữ dữ liệu thu thập được vào Local Storage
    """
    print(f"---Task: ETL Daily Stock Price Data---")
    start_time = datetime.now()
    start_timestamp = datetime_helper.get_utc_timestamp(start_time)
    # symbols = get_symbols()
    symbols = "PVI,PRE,BVH,BMI,PTI,PGI,MIG,VNR,OPC,DVN,VLB,SHI,VNINDEX,VN30INDEX,VN100-INDEX,HNX-INDEX,HNX30-INDEX"
    # symbols = "PVI"
    symbols = symbols.split(",")
    print(symbols)

    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for symbol in symbols:  # collect symbol data and store data to sql server
            etl_daily_symbol_price_to_sql_server(symbol, period_type, business_date,from_date, to_date)

    end_time = datetime.now()
    end_timestamp = datetime_helper.get_utc_timestamp(start_time)
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")

def etl_daily_symbol_price_to_sql_server(symbol: str, period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                                         from_date: Optional[date] = None, to_date: Optional[date] = None):
    start_time = datetime.now()

    # calculate and validate from_date, to_date
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=business_date, period_type=period_type)

    if not (from_date and to_date):
        return

    periods = datetime_helper.get_1_month_timespan_periods(from_date, to_date)

    def handle_delete_data(cursor: Cursor, symbol: str, from_date: date, to_date: date):
        cursor.execute("""
            DELETE 
            FROM ssi.daily_stock_price 
            WHERE symbol=? and trading_date >= ? and trading_date <= ?
        """, symbol, from_date, to_date)
        cursor.commit()

    def handle_insert_data(cursor: Cursor, df: pd.DataFrame):
        cursor.execute("BEGIN TRANSACTION")
        for idx, di in df.iterrows():
            print(di)
            cursor.execute("""
            INSERT INTO ssi.daily_stock_price(
                symbol,trading_date,price_change,per_price_change,ceiling_price,floor_price,ref_price,open_price,highest_price,lowest_price,close_price,average_price,close_price_adjusted,
                total_match_volume,total_match_value,total_deal_volume,total_deal_value,
                foreign_current_room,foreign_buy_volume,foreign_buy_value,foreign_sell_volume,foreign_sell_value,
                total_buy_trade_volume,total_buy_trade_value,total_sell_trade_volume,total_sell_trade_value,
                net_buy_sell_volume,net_buy_sell_value,total_traded_volume,total_traded_value,
                etl_date,etl_datetime) VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?)""",
                di["symbol"], di["trading_date"], di["price_change"], di["per_price_change"], di["ceiling_price"], di["floor_price"], di["ref_price"], di["open_price"], di["highest_price"], di["lowest_price"], di["close_price"], di["average_price"], di["close_price_adjusted"],
                di["total_match_volume"], di["total_match_value"], di["total_deal_volume"], di["total_deal_value"],
                di["foreign_current_room"], di["foreign_buy_volume"], di["foreign_buy_value"], di["foreign_sell_volume"], di["foreign_sell_value"],
                di["total_buy_trade_volume"], di["total_buy_trade_value"], di["total_sell_trade_volume"], di["total_sell_trade_value"],
                di["net_buy_sell_volume"], di["net_buy_sell_value"], di["total_traded_volume"], di["total_traded_value"],
                di["etl_date"], di["etl_datetime"])

        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()

    conn, cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        result = []
        for period in periods:  # get data from api in a period
            print(f"Period: {period['from_date']} {period['to_date']}")
            res = ssi_api.get_daily_stock_price(symbol, period['from_date'], period['to_date'])
            # print(res["data"])
            if(res and res["success"] and res["data"] and res["totalRecord"]):
               
                data_items = [{
                    "symbol": di["Symbol"],
                    "trading_date": datetime.strptime(di["TradingDate"], "%d/%m/%Y"),
                    "price_change": di["PriceChange"],
                    "per_price_change": di["PerPriceChange"],
                    "ceiling_price": di["CeilingPrice"],
                    "floor_price": di["FloorPrice"], 
                    "ref_price": di["RefPrice"],
                    "open_price": di["OpenPrice"],
                    "highest_price": di["HighestPrice"],
                    "lowest_price": di["LowestPrice"],
                    "close_price": di["ClosePrice"],
                    "average_price": di["AveragePrice"],
                    "close_price_adjusted": di["ClosePriceAdjusted"],
                    "total_match_volume": di["TotalMatchVol"],
                    "total_match_value": di["TotalMatchVal"],
                    "total_deal_volume": di["TotalDealVol"],
                    "total_deal_value": di["TotalDealVal"],
                    "foreign_current_room": di["ForeignCurrentRoom"],
                    "foreign_buy_volume": di["ForeignBuyVolTotal"],
                    "foreign_buy_value": di["ForeignBuyValTotal"],
                    "foreign_sell_volume": di["ForeignSellVolTotal"],
                    "foreign_sell_value": di["ForeignSellValTotal"],
                    "total_buy_trade_volume": di["TotalBuyTradeVol"],
                    "total_buy_trade_value": di["TotalBuyTrade"],
                    "total_sell_trade_volume": di["TotalSellTradeVol"],
                    "total_sell_trade_value": di["TotalSellTrade"],
                    "net_buy_sell_volume": di["NetBuySellVol"],
                    "net_buy_sell_value": di["NetBuySellVal"],
                    "total_traded_volume": di["TotalTradedVol"],
                    "total_traded_value": di["TotalTradedValue"],

                    "etl_date": date.today(),
                    "etl_datetime": datetime.now()
                } for di in res["data"]]
                result.extend(data_items)
            
            time.sleep(MARKET_API.DEFAULT_REQUEST_API_DELAY_TIME)
        # print(result)
        df = pd.DataFrame(result)
        # print(df)
        if df.shape[0] >= 1:
            handle_delete_data(cursor, symbol, from_date, to_date)
            handle_insert_data(cursor, df)
    except Exception as e:
        print(e)
    finally:
        db.close_session(conn, cursor)

    end_time = datetime.now()
    print(f"Symbol: {symbol} From Date: {from_date} - To Date: {to_date} StartTime: {start_time} Duration: {end_time - start_time}")

def etl_get_securities(data_destination_type: DATA_DESTINATION_TYPE,
                          period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                          from_date: Optional[date] = None, to_date: Optional[date] = None):
    print(f"---Task: ETL Securities Data---")
    start_time = datetime.now()
    symbols = "PVI,PRE" #,BVH,BMI,PTI,PGI,MIG,VNR,OPC,DVN,VLB,SHI,VNINDEX,VN30INDEX,VN100-INDEX,HNX-INDEX,HNX30-INDEX"
    symbols = symbols.split(",")
    print(symbols)
    
    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for symbol in symbols:
            etl_get_securities_symbol_to_sql_server(symbol,period_type,from_date, to_date)

    end_time = datetime.now()
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")

def etl_get_securities_symbol_to_sql_server(symbol: str,period_type: PERIOD_TYPE, bussiness_date: Optional[date] = None,
                                            from_date: Optional[date] = None,to_date: Optional[date] = None):
    start_time = datetime.now()

    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=bussiness_date,period_type=period_type)

    if not(from_date and to_date):
        return

    periods = datetime_helper.get_1_month_timespan_periods(from_date,to_date)

    def handle_insert_data(cursor: Cursor, df: pd.DataFrame):
        cursor.execute("BEGIN TRANSACTION")
        for di in df.iterrows():
            print(di)
            cursor.execute("""
            INSERT INTO ssi.security(
                symbol,market,stock_name,stock_en_name,etl_date,etl_datetime) VALUES
                (?,?,?,?,?,?)""",
                di["symbol"],di["market"],di["stock_name"],di["stock_en_name"],di["etl_date"], di["etl_datetime"])

        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()

    def handle_update_data(cursor: Cursor, df: pd.DataFrame):
        try:
            sql_query = pd.read_sql_query(""""
            SELECT symbol,market,stock_name,stock_en_name
            FROM ssi.security """)

            df1 = pd.DataFrame(sql_query,columns=['symbol','market','stock_name','stock_en_name'])
            print(df1)
        except:
            print("Unable to convert the data")

    conn, cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        result = []
        for period in periods:  # get data from api in a period
            print(f"Period: {period['from_date']} {period['to_date']}")
            res = ssi_api.get_securities(symbol, period['from_date'], period['to_date'])
            # print(res["data"])
            if(res and res["success"] and res["data"] and res["totalRecord"]):
               
                data_items = [{
                    "symbol": di["Symbol"],
                    "market": di["Market"],
                    "stock_name": di["StockName"],
                    "stock_en_name": di["StockEnName"],

                    "etl_date": date.today(),
                    "etl_datetime": datetime.now()
                } for di in res["data"]]
                result.extend(data_items)
            
            time.sleep(MARKET_API.DEFAULT_REQUEST_API_DELAY_TIME)
        # print(result)
        df = pd.DataFrame(result)
        # print(df)
        if df.shape[0] >= 1:
            handle_insert_data(cursor, df)
            #handle_update_data(cursor, df)
    except Exception as e:
        print(e)
    finally:
        db.close_session(conn, cursor)

    end_time = datetime.now()
    print(f"Symbol: {symbol} From Date: {from_date} - To Date: {to_date} StartTime: {start_time} Duration: {end_time - start_time}")
