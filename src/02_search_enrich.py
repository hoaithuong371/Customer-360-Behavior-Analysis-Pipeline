"""
PIPELINE 2: DATA ENRICHMENT & BUSINESS LOGIC
Mục tiêu:
1. Phục hồi dữ liệu từ Checkpoint của Pipeline 1.
2. Tích hợp từ điển từ kết quả Map tay (Human-in-the-loop).
3. Ánh xạ Thể loại (Category) vào 5.000 Users.
4. Tính toán trạng thái User (New, Churned, Retained) và sự chuyển dịch hành vi.
5. Xuất báo cáo Ma trận Chuyển dịch và Master Table đã làm giàu.
"""

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = "/Users/hoaithuong/Desktop/Customer-Behavior-Analysis-Pipeline"
CHECKPOINT_DIR = f"{BASE_DIR}/data/checkpoints"
MANUAL_FILE = f"{BASE_DIR}/data/mapping/Tu_Khoa_Da_Map_Tay.xlsx"
PROCESSED_DIR = f"{BASE_DIR}/data/processed"

FINAL_PARQUET = f"{PROCESSED_DIR}/Master_Table_Log_Search.parquet"
EXCEL_REPORT = f"{PROCESSED_DIR}/Bao_Cao_Chuyen_Dich_Thang_6_7.xlsx"

def get_spark_session():
    return SparkSession.builder \
        .appName("ETL_Pipeline_02_Enrich") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

def main():
    print("=== KHỞI ĐỘNG PIPELINE 2: ENRICH & BUSINESS LOGIC ===")
    spark = get_spark_session()
    
    try:
        # --- BƯỚC 1: PHỤC HỒI DỮ LIỆU TỪ CHECKPOINT ---
        print("Đang phục hồi dữ liệu từ Checkpoint...")
        df_5k = spark.read.parquet(f"{CHECKPOINT_DIR}/df_5k_checkpoint.parquet")
        df_regex_success = spark.read.parquet(f"{CHECKPOINT_DIR}/df_regex_success.parquet")
        
        # Định dạng lại để khớp cột với bảng Map tay
        df_regex_success = df_regex_success.select(
            F.col("keyword").alias("Keyword_Gốc"), 
            "category"
        )

        # --- BƯỚC 2: TÍCH HỢP DỮ LIỆU MAP TAY (HUMAN-IN-THE-LOOP) ---
        print("Đang nạp file kết quả Map tay của Data Engineer...")
        try:
            pdf_manual = pd.read_excel(MANUAL_FILE)
            # Làm sạch dữ liệu Excel trước khi đưa vào Spark để chống lỗi Join
            pdf_manual['Keyword_Gốc'] = pdf_manual['Keyword_Gốc'].astype(str).str.strip().str.lower()
            pdf_manual['category'] = pdf_manual['category'].astype(str).str.strip()
            
            df_manual_spark = spark.createDataFrame(pdf_manual[["Keyword_Gốc", "category"]])
            
            # Gộp thành Từ Điển Chuẩn (Master Dictionary)
            df_final_dictionary = df_regex_success.union(df_manual_spark)
            # Double check bằng Spark trim lần cuối
            df_final_dictionary = df_final_dictionary.withColumn(
                "Keyword_Gốc", F.trim(F.lower(F.col("Keyword_Gốc")))
            ).dropDuplicates(["Keyword_Gốc"])
            
            print(f"Đã tạo thành công Master Dictionary với {df_final_dictionary.count()} từ khóa.")
        except FileNotFoundError:
            print(f"❌ LỖI: Không tìm thấy file '{MANUAL_FILE}'. Vui lòng hoàn thành Map tay ở Pipeline 1!")
            return

        # --- BƯỚC 3: BULLETPROOF JOIN VÀO MASTER DATA ---
        print("Đang ánh xạ (Mapping) Thể loại vào 5.000 User...")
        
        # Đảm bảo Data gốc không chứa khoảng trắng tàng hình
        df_5k_safe = df_5k.withColumn("most_search_t6", F.trim(F.lower(F.col("most_search_t6")))) \
                          .withColumn("most_search_t7", F.trim(F.lower(F.col("most_search_t7"))))

        # Dùng Broadcast Join vì Dictionary thường nhỏ, giúp tăng tốc độ Join
        # Map cho tháng 6
        df_final = df_5k_safe.join(
            F.broadcast(df_final_dictionary).select(F.col("Keyword_Gốc").alias("kw_t6"), F.col("category").alias("category_t6")),
            df_5k_safe.most_search_t6 == F.col("kw_t6"),
            how="left"
        ).drop("kw_t6")

        # Map cho tháng 7
        df_final = df_final.join(
            F.broadcast(df_final_dictionary).select(F.col("Keyword_Gốc").alias("kw_t7"), F.col("category").alias("category_t7")),
            df_final.most_search_t7 == F.col("kw_t7"),
            how="left"
        ).drop("kw_t7")

        # --- BƯỚC 4: XỬ LÝ LOGIC NGHIỆP VỤ (BUSINESS LOGIC) ---
        print("Đang tính toán các chỉ số hành vi...")
        
        # Điền nhãn cho các trường hợp không search hoặc không map được
        df_final = df_final.fillna("Other", subset=["category_t6", "category_t7"])
        df_final = df_final.withColumn(
            "category_t6", F.when(F.col("most_search_t6").isNull(), "No_Search").otherwise(F.col("category_t6"))
        ).withColumn(
            "category_t7", F.when(F.col("most_search_t7").isNull(), "No_Search").otherwise(F.col("category_t7"))
        )

        # 1. User Status: Phân loại trạng thái người dùng
        # 2. Trending Types: Đánh giá xem user có đổi gu hay không
        # 3. Previous (Journey): Luồng chuyển dịch chi tiết
        df_final_enriched = df_final.withColumn(
            "user_status",
            F.when(F.col("category_t6") == "No_Search", "New_User")       
             .when(F.col("category_t7") == "No_Search", "Churned_User")   
             .otherwise("Retained_User")                                  
        ).withColumn(
            "trending_types",
            F.when(F.col("category_t6") == F.col("category_t7"), "unchanged").otherwise("changed")
        ).withColumn(
            "Previous",
            F.when(
                F.col("trending_types") == "unchanged", F.col("category_t6") 
            ).otherwise(
                F.concat(F.col("category_t6"), F.lit(" -> "), F.col("category_t7")) 
            )
        ).cache()
        df_final_enriched = df_final_enriched.select("user_id","most_search_t6", "category_t6", "most_search_t7", "category_t7", "user_status", "trending_types", "Previous")

        # --- BƯỚC 5: XUẤT BÁO CÁO & LƯU DỮ LIỆU ---
        print("Đang trích xuất Ma trận Chuyển dịch (Transition Matrix)...")
        df_transition_matrix = df_final_enriched.groupBy("category_t6").pivot("category_t7").count().fillna(0)
        
        # Tạo thư mục processed nếu chưa có
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        # Xuất file Excel báo cáo
        print(f"Đang lưu báo cáo Excel tại: {EXCEL_REPORT}")
        df_transition_matrix.toPandas().to_excel(EXCEL_REPORT, index=False)

        # Xuất Master Table vào thư mục data/processed
        print(f"Đang lưu Master Table tại: {FINAL_PARQUET}")
        df_final_enriched.write.mode("overwrite").parquet(FINAL_PARQUET)

        print("PIPELINE ĐÃ HOÀN TẤT THÀNH CÔNG!")
        df_final_enriched.select("user_id", "category_t6", "category_t7", "user_status", "trending_types").show(10)

    except Exception as e:
        print(f"❌ Pipeline thất bại do lỗi: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
