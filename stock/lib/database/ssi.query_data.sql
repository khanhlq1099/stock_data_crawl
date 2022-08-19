SELECT TOP(100) *
FROM ssi.daily_stock_price


SELECT COUNT(*)
FROM ssi.daily_stock_price

SELECT symbol, MIN(trading_date), MAX(trading_date)
FROM ssi.daily_stock_price
WHERE symbol='PVI'
GROUP BY symbol

SELECT * 
FROM ssi.daily_stock_price
WHERE symbol='PVI'
ORDER BY symbol,trading_date