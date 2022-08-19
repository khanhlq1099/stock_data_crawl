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

    DEFAULT_REQUEST_API_DELAY_TIME = 5 # s
    DEFAULT_PAGE_SIZE_1 = 1
    DEFAULT_PAGE_SIZE_10 = 10
    DEFAULT_PAGE_SIZE_100 = 100
    DEFAULT_PAGE_SIZE_1000 = 1000