from enum import Enum

class PERIOD_TYPE(Enum):
    PERIOD = "PERIOD"
    YTD = "YTD"
    QTD = "QTD"
    MTD = "MTD"
    TODAY = "TODAY"
    YESTERDAY = "YESTERDAY"
    Y = "Y"
    Q = "Q"
    M = "M"
    # T2WTD = 7
    # T4WTD = 8
    # T8WTD = 9

class MARKET_API:
    ROOT_PATH = "https://fc-data.ssi.com.vn/api/v2/Market/"

    ACCESS_TOKEN_API = "AccessToken"
    SECURITIES_API = "Securities"
    SECURITIES_DETAILS_API = "SecuritiesDetails"
    INDEX_COMPONENTS_API = "IndexComponents"
    INDEX_LIST_API = "IndexList"
    DAILY_OHLC_API = "DailyOhlc"
    INTRADAY_OHLC_API = "IntradayOhlc"
    DAILY_INDEX_API = "DailyIndex"
    DAILY_STOCK_PRICE_API = "DailyStockPrice"
    BACKTEST_API = "BackTest"