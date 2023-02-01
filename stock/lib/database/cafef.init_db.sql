USE dw_stock
GO

CREATE SCHEMA cafef;
GO

/* -- cafef.symbol -- */
IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'symbol')
BEGIN
CREATE TABLE cafef.symbol (
	symbol_id nvarchar(20) primary key,
	symbol_name nvarchar(200),
	market_id nvarchar(20),
	market_name nvarchar(200),
	md_symbol_id nvarchar(20) --- master data mapping 
)
END


/* -- create table cafef.daily_stock_price -- */
--DROP TABLE cafef.daily_stock_price 
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

    CONSTRAINT pk__cafef_daily_stock_price PRIMARY KEY(ma,ngay)
)
END

/* -- Create table cafef.daily_market_history_lookup_price -- */
-- DROP TABLE cafef.daily_market_history_lookup_price

IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'daily_market_history_lookup_price')
BEGIN
CREATE TABLE cafef.daily_market_history_lookup_price(
    market nvarchar(20), --Master Data
    ngay date,
    ma nvarchar(20),
    gia_dong_cua float,
    gia_tri_thay_doi float,
    phan_tram_thay_doi float,
    gia_tham_chieu float,
    gia_mo_cua float,
    gia_cao_nhat float, 
    gia_thap_nhat float,
    khoi_luong_giao_dich_khop_lenh decimal(18,2),
    gia_tri_giao_dich_khop_lenh decimal(18,2),
    khoi_luong_giao_dich_thoa_thuan decimal(18,2),
    gia_tri_giao_dich_thoa_thuan decimal(18,2),

    etl_date date,
    etl_datetime datetime,

    CONSTRAINT pk__cafef_daily_market_price PRIMARY KEY(market,ma,ngay)
)
END
GO
/* -- Create table cafef.daily_market_setting_command_price -- */
-- DROP TABLE cafef.daily_market_setting_command_price

IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'daily_market_setting_command_price')
BEGIN
CREATE TABLE dw_stock.cafef.daily_market_setting_command_price(
    market nvarchar(20), --Master Data
    ngay date,
    ma nvarchar(20),
    du_mua decimal(18,2),
    du_ban decimal(18,2),
    gia float,
    gia_tri_thay_doi float,
    phan_tram_thay_doi float,
    so_lenh_dat_mua decimal(18,2),
    khoi_luong_dat_mua decimal(18,2),
    kl_trung_binh_1_lenh_mua decimal(18,2),
    so_lenh_dat_ban decimal(18,2),
    khoi_luong_dat_ban decimal(18,2),
    kl_trung_binh_1_lenh_ban decimal(18,2),
    chenh_lech_mua_ban decimal(18,2),

    etl_date date,
    etl_datetime datetime,

    CONSTRAINT pk__cafef_daily_market_setting_command_price PRIMARY KEY(market,ma,ngay)
)
END
GO

/* -- Create table cafef.daily_market_foreign_transactions_price -- */
-- DROP TABLE dw_stock.cafef.daily_market_foreign_transactions_price

IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'daily_market_foreign_transactions_price')
BEGIN
CREATE TABLE dw_stock.cafef.daily_market_foreign_transactions_price(
    market nvarchar(20), --Master Data
    ngay date,
    ma nvarchar(20),
    khoi_luong_mua decimal(18,2),
    gia_tri_mua decimal(18,2),
    khoi_luong_ban decimal(18,2),
    gia_tri_ban decimal(18,2),
    khoi_luong_giao_dich_rong decimal(25,2),
    gia_tri_giao_dich_rong decimal(25,2),
    room_con_lai decimal(25,2),
    dang_so_huu float,

    etl_date date,
    etl_datetime datetime,

    CONSTRAINT pk__cafef_daily_market_foreign_transactions_price PRIMARY KEY(market,ma,ngay)
)
END
GO
 
/* -- create table cafef.hourly_stock_price -- */
-- DROP TABLE cafef.hourly_stock_price 
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

    CONSTRAINT pk__cafef_hourly_stock_price PRIMARY KEY(ma,ngay,thoi_gian)
)
END

/* -- create table cafef.hourly_exchange_rate -- */
-- DROP TABLE cafef.hourly_exchange_rate
IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'hourly_exchange_rate')
BEGIN
CREATE TABLE cafef.hourly_exchange_rate (
    ma nvarchar(20),
    ngay date,
    thoi_gian time,
    gia float,
    gia_tri_thay_doi float,
    phan_tram_thay_doi float,
    
    etl_datetime datetime

    CONSTRAINT pk__cafef_hourly_exchange_rate PRIMARY KEY(ma,ngay,thoi_gian)
)
END

USE dw_stock
GO
/* -- create table cafef.hourly_merchandise -- */
-- DROP TABLE dw_stock.cafef.hourly_merchandise
IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'hourly_merchandise')
BEGIN
CREATE TABLE cafef.hourly_merchandise (
    hang_hoa nvarchar(255),
    ngay date,
    thoi_gian time,
    gia_cuoi_cung float,
    gia_cao_nhat float,
    gia_thap_nhat float,
    gia_thay_doi float,
    phan_tram_thay_doi float,
    
    etl_datetime datetime

    -- CONSTRAINT pk__cafef_hourly_merchandise PRIMARY KEY(hang_hoa,etl_datetime)
)
END

USE dw_stock
GO
/* -- create table cafef.hourly_world_stock -- */
-- DROP TABLE dw_stock.cafef.hourly_world_stock
IF NOT EXISTS(SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'cafef' AND name like 'hourly_world_stock')
BEGIN
CREATE TABLE cafef.hourly_world_stock (
    chi_so nvarchar(255),
    ngay date,
    thoi_gian time,
    gia_cuoi_cung float,
    gia_thap_nhat float,
    gia_cao_nhat float,
    gia_thay_doi float,
    phan_tram_thay_doi float,
    
    etl_datetime datetime

    -- CONSTRAINT pk__cafef_hourly_merchandise PRIMARY KEY(hang_hoa,etl_datetime)
)
END

GO
CREATE SCHEMA ssi;
GO

