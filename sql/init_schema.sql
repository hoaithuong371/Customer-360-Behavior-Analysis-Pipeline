CREATE DATABASE IF NOT EXISTS big_data_analysis;
USE big_data_analysis;

DROP TABLE IF EXISTS customer_360_behavior;

CREATE TABLE customer_360_behavior (
    user_id VARCHAR(50) PRIMARY KEY,
    category_t6 VARCHAR(50),
    category_t7 VARCHAR(50),
    user_status VARCHAR(50),
    trending_types VARCHAR(50),
    Previous TEXT,
    Total_Truyen_Hinh BIGINT DEFAULT 0,
    Total_Phim_Truyen BIGINT DEFAULT 0,
    Total_Giai_Tri BIGINT DEFAULT 0,
    Total_Thieu_Nhi BIGINT DEFAULT 0,
    Total_The_Thao BIGINT DEFAULT 0,
    MostWatch VARCHAR(50),
    Taste TEXT,
    Active DOUBLE DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Index để tối ưu hóa truy vấn cho Dashboard
CREATE INDEX idx_user_status ON customer_360_behavior(user_status);
CREATE INDEX idx_category_t7 ON customer_360_behavior(category_t7);



