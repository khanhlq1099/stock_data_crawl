from stock.lib.sql_server import open_session, close_session
from stock.config import SQL_SERVER_CONFIG



def init_db():
    conn, cursor = open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    cursor.execute("""
    --- create schema ssi
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='ssi')
    EXEC ('CREATE SCHEMA ssi');
    """)

    cursor.execute("""
    --- create schema cafef
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='cafef')
    EXEC ('CREATE SCHEMA cafef');
    """)

    cursor.execute("""
    --- create table cafef.daily_stock_price
    IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'daily_stock_price')
    BEGIN
    CREATE TABLE cafef.daily_stock_price (
        ma nvarchar(20),
        ngay date,
        gia_dieu_chinh float,
        gia_dong_cua float,
        gia_tri_thay_doi float,
        phan_tram_thay_doi float,
        khoi_luong_giao_dich_khop_lenh decimal(18,2),
        gia_tri_giao_dich_khop_lenh decimal(18,2),
        khoi_luong_giao_dich_thoa_thuan decimal(18,2),
        gia_tri_giao_dich_thoa_thuan decimal(18,2),
        gia_tham_chieu float,
        gia_mo_cua float,
        gia_cao_nhat float,
        gia_thap_nhat float,

        etl_date date,
        etl_datetime datetime

        CONSTRAINT pk__daily_stock_price PRIMARY KEY(ma,ngay)
    )
    --- ALTER TABLE cafef.daily_stock_price ADD CONSTRAINT u__cafef_daily_stock_price__1 UNIQUE(ma, ngay);
    END
    """)

    cursor.execute("""
    --- create table cafef.hourly_stock_price
    IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'hourly_stock_price')
    BEGIN
    CREATE TABLE cafef.hourly_stock_price (
        ma nvarchar(20),
        ngay date,
        thoi_gian time,
        gia float,
        gia_tri_thay_doi float,
        phan_tram_thay_doi float,
        khoi_luong_lo decimal(18,2),
        khoi_luong_tich_luy decimal(18,2), 
        
        etl_date date,
        etl_datetime datetime

        CONSTRAINT pk__hourly_stock_price PRIMARY KEY(ma,ngay,thoi_gian)
    )
    END
    """)

    cursor.execute("""
    --- create table ssi.security
    IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'ssi' AND name like 'security')
    CREATE TABLE ssi.security (
        symbol nvarchar(20) primary key,     
        market nvarchar(20),
        stock_name nvarchar(200),
        stock_en_name nvarchar(200),

        etl_date date, 
        etl_datetime datetime
    )   
    """)

    cursor.execute("""
    --- create table ssi.daily_stock_price
    IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'ssi' AND name like 'daily_stock_price')
    CREATE TABLE ssi.daily_stock_price (
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

        etl_date date,
        etl_datetime datetime
    )
    """)

    cursor.commit()

    close_session(conn, cursor)

def get_symbols():
    conn, cursor = open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)

    cursor.execute("SELECT symbol FROM ssi.security")
    symbols = [row[0] for row in cursor.fetchall()]

    close_session(conn, cursor)
    return symbols
   
        
