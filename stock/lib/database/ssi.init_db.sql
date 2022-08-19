--- create schema stg
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='ssi')
    EXEC ('CREATE SCHEMA ssi');

--- create table ssi.security
IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'ssi' AND name like 'symbol')
CREATE TABLE ssi.symbol (
    symbol nvarchar(20),     
    market nvarchar(20),
    stock_name nvarchar(200),
    stock_en_name nvarchar(200),

    etl_date date, 
    etl_datetime datetime,

    CONSTRAINT pk__ssi_symbol PRIMARY KEY(symbol)
)

--- create table ssi.daily_stock_price
IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'ssi' AND name like 'daily_stock_price')
CREATE TABLE ssi.daily_stock_price (
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

    etl_date date,
    etl_datetime datetime,

    CONSTRAINT pk__ssi_daily_stock_price PRIMARY KEY(symbol,trading_date)
)

