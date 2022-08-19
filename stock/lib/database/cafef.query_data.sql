SELECT * FROM cafef.symbol ORDER BY symbol_id
SELECT TOP(100) * FROM cafef.daily_stock_price
SELECT TOP(100) * FROM cafef.hourly_stock_price

-- Lấy danh sách các mã/chỉ số thu thập được thông tin hàng ngày
SELECT ma
FROM cafef.daily_stock_price with(nolock)
GROUP BY ma
ORDER BY ma

-- Lấy danh sách các mã/chỉ số thu thập được thông tin hàng giờ
SELECT ma
FROM cafef.hourly_stock_price with(nolock)
GROUP BY ma
ORDER BY ma

-- Lấy danh sách các mã/chỉ số thu thập được thông tin hàng giờ
SELECT ma
FROM cafef.hourly_stock_price with(nolock)
GROUP BY ma
ORDER BY ma

-- Lấy thống kê về dữ liệu thu thập được
SELECT ma, YEAR(ngay) as nam, MIN(ngay) as ngay_bat_dau, MAX(ngay) as ngay_ket_thuc, COUNT(*) as so_dong 
FROM cafef.daily_stock_price with(nolock)
GROUP BY ma, YEAR(ngay)
ORDER BY ma, YEAR(ngay)

-- Lấy giá chứng khoán cập nhật hàng giờ của 3 ngày gần nhất
SELECT *
FROM cafef.hourly_stock_price with(nolock)
WHERE ngay >= DATEADD(DAY, -3, GETDATE())
ORDER BY ma,ngay,thoi_gian

SELECT *
FROM cafef.hourly_stock_price with(nolock)
WHERE ngay = '2022-08-04' and ma = 'DVN'
ORDER BY ma,ngay,thoi_gian


SELECT hp.*
FROM cafef.hourly_stock_price hp with(nolock)
JOIN (SELECT ma, max(ngay) as ngay_gan_nhat
    FROM cafef.hourly_stock_price with(nolock)
    GROUP BY ma
) md on hp.ma = md.ma and hp.ngay = md.ngay_gan_nhat
ORDER BY hp.ma, hp.ngay desc, hp.thoi_gian desc



