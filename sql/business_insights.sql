/* ====================================================================
DỰ ÁN: CUSTOMER BEHAVIOR ANALYSIS PIPELINE
File: business_insights.sql
Mục tiêu: Truy xuất dữ liệu từ bảng Data Mart để trả lời các câu hỏi kinh doanh.
==================================================================== */

USE big_data_analysis;

-- --------------------------------------------------------------------
-- 1. ĐÁNH GIÁ SỨC KHỎE TẬP KHÁCH HÀNG (Churn Analysis)
-- Insight: Kiểm tra xem nhóm "Rời bỏ" còn le lói hoạt động nào không để remarketing.
-- --------------------------------------------------------------------
SELECT 
    user_status,
    COUNT(user_id) AS User_Count,
    ROUND(AVG(Active), 2) AS Average_Active_Ratio,
    SUM(Total_Truyền_Hình + Total_Phim_Truyện + Total_Giải_Trí + Total_Thiếu_Nhi + Total_Thể_Thao) AS Total_Platform_Watch_Time
FROM customer_360_behavior
GROUP BY user_status
ORDER BY User_Count DESC;

-- --------------------------------------------------------------------
-- 2. PHÁT HIỆN XU HƯỚNG DỊCH CHUYỂN (Trend Shifting)
-- Insight: Bắt kịp Trend thay đổi gu từ tháng 6 sang tháng 7.
-- --------------------------------------------------------------------
SELECT 
    category_t6 AS Old_Taste_June, 
    category_t7 AS New_Taste_July, 
    COUNT(user_id) AS Shift_Volume
FROM customer_360_behavior
WHERE trending_types = 'changed'
GROUP BY category_t6, category_t7
ORDER BY Shift_Volume DESC
LIMIT 5;


-- --------------------------------------------------------------------
-- 3. TÌM KIẾM TẬP KHÁCH HÀNG VÀNG (VIP Segmentation)
-- Insight: Xuất danh sách 10 khách hàng xem nền tảng nhiều nhất với gu đa dạng.
-- --------------------------------------------------------------------
SELECT 
    user_id, 
    user_status, 
    Taste, 
    Active,
    (Total_Phim_Truyện + Total_Thể_Thao) AS Target_Watch_Time
FROM customer_360_behavior
WHERE Active > 0.8 
  AND Taste LIKE '%Phim Truyện%' 
  AND Taste LIKE '%Thể Thao%'
ORDER BY Target_Watch_Time DESC
LIMIT 10;
