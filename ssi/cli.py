from datetime import datetime, date
import typer
from typing import Optional

import ssi.services.etl as etl_service
from ssi.models.constants import PERIOD_TYPE
from ssi.helpers import datetime_helper

app = typer.Typer()


@app.command()
def hello(name: str, lastname: str = "", intro: Optional[str] = None):
    typer.echo(f"Hello {name}")


@app.command()
def etl_securities():
    """
    Đồng bộ dữ liệu danh sách tất cả mã cổ phiếu từ SSI API. 
    """
    etl_service.etl_securities()
    # try:
    #     etl_service.etl_securities()
    # except Exception as e:
    #     print(e)


@app.command()
def etl_daily_symbols_price(symbols: str, period_type: PERIOD_TYPE, business_date: Optional[datetime] = None, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    """
    Đồng bộ dữ liệu giá cổ phiểu đầu ngày, cuối ngày từ SSI API.

    Cho phép nhập nhiều mã cổ phiểu
    """
    symbols = symbols.split(",")
    etl_service.etl_daily_symbols_price(
        symbols, business_date, period_type, from_date, to_date)
    # try:
    #     etl_service.etl_daily_stock_price(symbols, period_type,business_date,from_date,to_date)
    # except Exception as e:
    #     print(e)


@app.command()
def etl_daily_stock_price(period_type: PERIOD_TYPE, business_date: Optional[datetime] = None, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    """
    
    """
    etl_service.etl_daily_stock_price(business_date, period_type, from_date, to_date)

# @app.command()
# def etl_daily_index(business_date: str, period_type: PERIOD_TYPE, from_date: Optional[date] = None, to_date: Optional[date] = None):
#     try:
#         business_date = datetime.strptime(business_date, "%d/%m/%Y")
#         etl_service.etl_daily_index(
#             business_date=business_date, period_type=period_type, from_date=from_date, to_date=to_date)
#     except:
#         print("Parameters Error")
