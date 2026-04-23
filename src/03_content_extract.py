"""
PIPELINE 3: LOG CONTENT ETL (ENRICHMENT)
Mục tiêu:
1. Đọc dữ liệu JSON thô từ log_content.
2. Làm sạch, trích xuất thời gian và phân loại AppName thành các Thể loại.
3. Tính toán Tỷ lệ hoạt động (Active Ratio) và Pivot thời lượng xem.
4. Xác định MostWatch và Taste của từng user.
5. Lưu kết quả Enriched vào thư mục processed để chuẩn bị JOIN với log_search.
"""

import os
import sys

import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Khởi tạo findspark (Dành cho môi trường local)
findspark.init()

# =============================================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN & HẰNG SỐ
# =============================================================================
BASE_DIR = "/Users/hoaithuong/Desktop/Customer-Behavior-Analysis-Pipeline"
RAW_CONTENT_PATH = "/Users/hoaithuong/Desktop/DE CLASS/Dataset/log_content"
OUTPUT_PATH = f"{BASE_DIR}/data/processed/Master_Content_Enriched.parquet"

TOTAL_DAYS_IN_MONTH = 30 

# Định nghĩa Schema để ép kiểu chuẩn xác khi đọc JSON
CONTENT_SCHEMA = StructType([
    StructField("_source", StructType([
        StructField("Contract", StringType(), True),
        StructField("AppName", StringType(), True),
        StructField("TotalDuration", LongType(), True)
    ]), True)
])

def main():
    print("=== KHỞI ĐỘNG PIPELINE 3: CONTENT ETL ===")
    
    # Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("ETL_Content_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    print(">>> Spark Session đã sẵn sàng!")

    try:
        # --- BƯỚC 1: ĐỌC DỮ LIỆU ---
        print(f">>> Đang đọc dữ liệu từ: {RAW_CONTENT_PATH}")
        raw_df = spark.read.schema(CONTENT_SCHEMA).json(f"{RAW_CONTENT_PATH}/*.json")
        
        # --- BƯỚC 2: TIỀN XỬ LÝ (TRÍCH XUẤT NGÀY & ĐỔI TÊN CỘT) ---
        df_clean = raw_df.withColumn("file_name", F.input_file_name()) \
            .withColumn("DateStr", F.regexp_extract(F.col("file_name"), r"(\d{8})\.json$", 1)) \
            .withColumn("Date", F.to_date(F.col("DateStr"), "yyyyMMdd")) \
            .drop("file_name", "DateStr")
            
        # --- BƯỚC 3: MAPPING THỂ LOẠI (CATEGORY) ---
        print(">>> Đang làm sạch và phân loại (Mapping) Thể loại...")
        df_mapped = df_clean.select(
            F.col("_source.Contract").alias("user_id"),
            F.col("_source.TotalDuration").cast("long").alias("TotalDuration"),
            F.col("Date"),
            F.when(F.col("_source.AppName").isin('CHANNEL', 'DSHD', 'KPLUS', 'KPlus'), "Truyền Hình")
            .when(F.col("_source.AppName").isin('VOD', 'FIMS_RES', 'BHD_RES', 'VOD_RES', 'FIMS', 'BHD', 'DANET'), "Phim Truyện")
            .when(F.col("_source.AppName") == 'RELAX', "Giải Trí")
            .when(F.col("_source.AppName") == 'CHILD', "Thiếu Nhi")
            .when(F.col("_source.AppName") == 'SPORT', "Thể Thao")
            .otherwise("Other").alias("Type") 
        )

        # Lọc rác và cache dữ liệu
        df_clean = df_mapped.filter(
            (F.col("TotalDuration") > 0) & 
            (F.col("Type") != "Other") & 
            (F.col("user_id").rlike("^[a-zA-Z0-9]")) 
        ).cache()

        # --- BƯỚC 4: TÍNH TOÁN KPI (ACTIVE RATIO, MOST WATCH, TASTE) ---
        print(">>> Đang tính toán KPIs...")
        
        # 4.1. Tính Active Ratio
        df_active = df_clean.select("user_id", "Date").dropDuplicates() \
            .groupBy("user_id").agg(
                F.round((F.count("Date") / TOTAL_DAYS_IN_MONTH)*100, 2).alias("Active")
            )

        # 4.2. Pivot Duration
        categories = ["Truyền Hình", "Phim Truyện", "Giải Trí", "Thiếu Nhi", "Thể Thao"]
        df_pivot = df_clean.groupBy("user_id").pivot("Type", categories).sum("TotalDuration").fillna(0)

        # 4.3. Tìm MostWatch & Taste
        df_result = df_pivot.withColumn("max_val", F.greatest(*[F.col(c) for c in categories]))
        
        most_watch_expr = F.when(F.col("max_val") == 0, "None")
        for cat in categories:
            most_watch_expr = most_watch_expr.when(F.col("max_val") == F.col(cat), cat)

        final_df = df_result.withColumn("MostWatch", most_watch_expr.otherwise("Mixed")) \
                            .withColumn("Taste", F.concat_ws(" - ", *[F.when(F.col(c) > 0, F.lit(c)) for c in categories])) \
                            .join(df_active, on="user_id", how="inner") \
                            .drop("max_val")

        # 4.4. Đổi tên cột chuẩn mực
        for cat in categories:
            final_df = final_df.withColumnRenamed(cat, "Total_" + cat.replace(" ", "_"))

        print(">>> KẾT QUẢ CUỐI CÙNG (MASTER CONTENT ENRICHED):")
        final_df_sample = final_df.orderBy(F.rand()).limit(5000)
        final_df_sample.show(5, truncate=False)

        # --- BƯỚC 5: XUẤT DỮ LIỆU ---
        print(f">>> Đang lưu file Parquet tại: {OUTPUT_PATH}")
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        final_df_sample.write.mode("overwrite").parquet(OUTPUT_PATH)
        
        print("✅ PIPELINE 3 HOÀN TẤT THÀNH CÔNG!")

    except Exception as e:
        print(f"❌ Pipeline thất bại do lỗi: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()