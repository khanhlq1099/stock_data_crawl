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

GO
CREATE SCHEMA ssi;
GO

