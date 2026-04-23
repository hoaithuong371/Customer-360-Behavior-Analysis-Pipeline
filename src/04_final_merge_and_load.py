import os
from dotenv import load_dotenv
import findspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

load_dotenv()

findspark.init()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEARCH_PATH = os.path.join(BASE_DIR, "data", "processed", "Master_Table_Log_Search.parquet")
CONTENT_PATH = os.path.join(BASE_DIR, "data", "processed", "Master_Content_Enriched.parquet")
TABLEAU_EXPORT_PATH = os.path.join(BASE_DIR, "data", "processed", "Customer_360_Tableau.xlsx")

JDBC_DRIVER_PATH = "/opt/homebrew/Cellar/apache-spark/4.0.1_1/libexec/jars/mysql-connector-java-8.0.28.jar"

def create_spark_session():
    """Khởi tạo và cấu hình Spark Session"""
    return SparkSession.builder \
        .appName("Customer_360_Merger") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN") 
    
    print(">>> [1/3] Spark Session đã sẵn sàng. Bắt đầu đọc dữ liệu...")
    
    try:
        # 1. Đọc dữ liệu
        df_search = spark.read.parquet(SEARCH_PATH)
        df_content = spark.read.parquet(CONTENT_PATH)
        
        print(f"    - Số lượng User bên Search: {df_search.count()}")
        print(f"    - Số lượng User bên Content: {df_content.count()}")

        print(">>> [2/3] Đang thực hiện Join dữ liệu theo row_idx...")
        
        # -------------------------------------------------------------------
        # LOGIC GHI ĐÈ: Join theo thứ tự dòng (row_idx)
        # -------------------------------------------------------------------
        # 1. Tạo một "khung" để đếm số thứ tự dòng
        windowSpec = Window.orderBy(F.monotonically_increasing_id())

        # 2. Đánh số thứ tự dòng cho từng bảng
        df_search_with_idx = df_search.withColumn("row_idx", F.row_number().over(windowSpec))
        
        # Kiểm tra xem df_content có cột user_id không để drop, tránh lỗi trùng cột
        if "user_id" in df_content.columns:
            df_content_with_idx = df_content.drop("user_id").withColumn("row_idx", F.row_number().over(windowSpec))
        else:
            df_content_with_idx = df_content.withColumn("row_idx", F.row_number().over(windowSpec))

        # 3. Join 2 bảng dựa trên cột 'row_idx', sau đó xóa cột này
        df_360 = df_search_with_idx.join(df_content_with_idx, on="row_idx", how="inner") \
                                   .drop("row_idx")
        
        # Cache lại data 
        df_360.cache()
        
        print(f">>> HOÀN TẤT! TỔNG SỐ KHÁCH HÀNG 360 ĐỘ (INNER JOIN): {df_360.count()}")
        print(">>> Xem thử 10 dòng tiêu biểu:")
        df_360.show(10, truncate=False)

        # Import CSV for Tableau Public
        df_360.toPandas().to_excel(TABLEAU_EXPORT_PATH, index=False, engine='openpyxl')
        print(f"Đã xuất file Excel (.xlsx) cho Tableau tại: {TABLEAU_EXPORT_PATH}")
    
        url = f"jdbc:mysql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'big_data_analysis')}"
        driver = "com.mysql.cj.jdbc.Driver"
        user = os.getenv('DB_USER', 'root')
        password = os.getenv('DB_PASS', '')

        df_360.write.format('jdbc') \
            .option('url', url) \
            .option('driver', driver) \
            .option('dbtable', 'customer_360_behavior') \
            .option('user', user) \
            .option('password', password) \
            .mode('overwrite').save()
            
        print("✅ Data Import Successfully")   

    except AnalysisException as e:
        print(f"Lỗi đọc file hoặc lỗi schema Spark: {e}")
    except Exception as e:
        print(f"Đã xảy ra lỗi không xác định: {e}")
    finally:
        spark.stop()
        print(">>> Spark Session đã đóng an toàn.")

if __name__ == "__main__":
    main()