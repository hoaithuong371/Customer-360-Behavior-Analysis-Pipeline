"""
PIPELINE 1: DATA EXTRACTION & AUTO CATEGORIZATION
Mục tiêu:
1. Trích xuất và làm sạch dữ liệu tìm kiếm tháng 6 & 7.
2. Chấm điểm ý định (Intent Score) và lọc ra Top 5.000 Users tiềm năng nhất.
3. Trích xuất tập Keyword duy nhất và sử dụng Rule Engine (Regex) để phân loại tự động.
4. Lưu trạng thái (Checkpoint) và xuất file Excel cho bước Map tay.
"""

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# 1. CẤU HÌNH HỆ THỐNG 
# =============================================================================

# TODO: Đổi đường dẫn này thành biến môi trường khi đưa lên Production (VD: s3://bucket-name/...)
BASE_PATH = "/Users/hoaithuong/Desktop/DE CLASS/Dataset/log_search"
CHECKPOINT_DIR = "data/checkpoints"
EXPORT_MANUAL_FILE = "data/mapping/Tu_Khoa_Can_Map_Tay.xlsx"

# =============================================================================
# 2. TỪ ĐIỂN REGEX (RULE ENGINE)
# =============================================================================
REGEX_RULES = {
    "System": r"(?i)\b(dang nhap|mat khau|app|vip|nap tien|dang ky|tik tok|youtube|ezviz|xmeye|tim kiem|cach|youtobe|you tube|fpt|zalo|facebook|google|tai|huong dan|hack|loi|cai dat|tai khoan)\b",
    "Sports": r"(?i)\b(bong da|cup|u19|u23|ngoai hang|ban ket|chung ket|truc tiep|the thao|world cup|man utd|mu|ronaldo|messi|liverpool|tran dau|fc|da bong|bong chuyen|cakhia|vebo|xoilac|mitom|tennis|nba|bong ro|ca long|cau long|bida|billiard|highlight)\b",
    "Anime": r"(?i)\b(anime|doraemon|conan|naruto|pokemon|titan|samurai|hai tac mu rom|overlord|than vuc|ngoc rong|chuyen sinh|fairy|boku|hiep si|kaisen|phap su|luoi guom|tokyo|totoro|erased|jojo|soma|vanitas|stone|manga|sensei|saku|sanji|doukyuusei|hoi p|yaiba|chu thuat|hero|sword art|ultraman|luffy|bungou|haikyuu|inuyasa|shikimori|bleach|haik|kake|tai nguc toi|ban gai thue|one piece|dragon ball|goku|boruto|kimetsu|demon slayer|spy x family|wibu|hoat hinh nhat)\b",
    "Cartoon_Kids": r"(?i)\b(hoat hinh|thieu nhi|halloween|sieu nhan|sonic|super sentai|cong chua|hoang tu|oddbods|bingo|animal|baby|bang bang|minions|masha|bach tuyet|lego|khung long|con vit|lo lem|nastya|biet doi|em be|yo yo|chu cho|robot|chu voi con|ngoi nha nao nhiet|meo con|gio phieu luu|pootle|su troi day|ve de|ba oi ba|minio|rua tay|tap the duc|doubutsu|tom|larva|mam choi|sakura|pucca|can cau|xe tai|nguoi dep toc may|khong lo xanh|harry|peppa|pororo|pinkfong|truyen co tich|ke chuyen|nhac thieu nhi|kids)\b",
    "Music": r"(?i)\b(nhac|bai hat|karaoke|mv|ca si|remix|mp3|son tung|den vau|bolero|lien khuc|tru tinh|nonstop|edm|sing|song|lyric|paris by night|cover|piano|khong loi|beat|hat|dan ca|nhac che|kpop|audio|bang kieu|nhac tre|zumba|anh tho|nhac trinh|ve que|gangnam style|dance|lofi|chill|acoustic|rap|hiphop|concert)\b",
    "Horror": r"(?i)\b(kinh di|ma|zombie|quy|xac song|scary|get out|childs play|pee nak|1408|loi nguyen|ma quai|train to busan|chuyen tau sinh tu|cuong thi|piranha|stree|annabelle|conjuring|the nun|exorcist|dracula|vampire|kinh hoang|doa ma|phim ma)\b",
    "Action": r"(?i)\b(hanh dong|ma tran|vo thuat|ban sung|marvel|dc|sieu anh hung|sat thu|cu dam|captain|lien minh|morbius|sat nhan|john wick|gangster|swat|iron|galaxy|jason|potter|jumanji|maleficent|nhiem vu|nguoi nhen|avatar|chien tranh|lucy|tron thoat|than chet|ky si|kungfu|nguy nguy hiem|tarzan|monster|aquaman|alice|matrix|toi pham|king kong|phim le|xa thu|batman|superman|avengers|fast and furious|tom cruise|die hard|007|james bond|vo thuat)\b",
    "C-Drama": r"(?i)\b(phim trung|trung quoc|hoa ngu|kiem hiep|co trang|ngon tinh|tong tai|xuyen khong|tien hiep|cung dau|mong hoa luc|goi toi la tong giam doc|ky|cong luoc|tinh ha xan lan|tram vun huong phai|tinh than bien|hien kim|tieu nu|chan tu dan|hoang tu ech|em dep nhat|lay danh nghia|ngươi tinh xa la|hanh phuc trong tam tay|huyen da|dong cung|nhiet huyet|thien cot|quat sinh hoai nam|den van gia|van tich truyen|cam tu duyen|tinh yeu va dinh menh|hong kong|thieu nien tu dai|tan nuong|do long ky|nhat kiem|song thanh|ji gong|tan bang phong than|tay du|sinh duyen|vi dieu|hoa ra thoi gian deu|sam sam|tien kiem|dien hy|le hap duong phen|my nhan|gia nam|vinh dieu|bach ngoc nho|liet hoa|chinh phuc sep|thanh tri doanh luy|yeu em luc ngay tho|hoa tieu thu|bat dau yeu|sao bang|khuynh tam|tu tao vi|ban cung phong|nhu y|truyen|tieu phat|hoi ban cuc pham|cuc tinh y|ca muc|ba kiep|chuyen tinh bien xanh|ban nhau tron doi|bang chung thep|tinh yeu nhi phan|thieu lam tu|thuy hu|man dem gon song|he thong|so than|10 nam 3 thang|minh nguyet|vo tac thien|mo nam chi|nuong|co nang tinh nghich|ho so trinh sat|boss muon cuoi|tran han du|bay luon theo gio|phu dao hoang hau|chi la quan he|sac khuy|tuyet lau|vo tinh|cao luong|yeu tham xa anh|ha tich|diem lam|huong mat|ngot ngao|an nhu co|yeu em tu|f4|ngung yeu em|tan thuy hoang|dep trai la|nua hiep co tri|tai thuong|thoi gian va em|dau dang giang ho|than vuong|thieu nien|bi mat noi|sao bac du|thanh thanh|thien ha|canh dep ngay vui|neu em binh yen|30 chua|nguoi con trai|suc manh tinh than|tinh han|centimet|khi man dem|chan tuong|chau tinh tri|ly lien kiet|thanh long|tvb)\b",
    "K-Drama": r"(?i)\b(phim han|han quoc|korea|kdrama|oppa|woo young woo|youth of may|nu luat su ky la|hoan hon|hen ho chon cong so|buoc duong cung|tien nu cu ta|dinh menh anh yeu em|mua he yeu dau|danh sach mua sam|penthouse|boys over|hau due mat troi|co tay ao|tham phan|ve dep dich thuc|bat ma pha an|nguoi thua ke|dang cap thuc khach|san quy va nau mi|chang hau|thu ky kim|thuong luu|ho ly|queen|co nang trong trang|ke quyen ru|ban gai toi|hoa du ky|bok joo|del luna|bac si ma|khach san ma|nhe nhang tan chay|an danh|yeu tinh|the heirs|buoc den om em|bien xanh|thien tai lang bam|gap anh ay|co nang manh me|vua truong hoc|yeu khong kiem soat|may hoa|eve|co gai mat troi|ha canh|lo lem thoi dai|ngo ngao|khoai tay|mat trang om|true beauty|mot lan nua|thanh tra|thua ke|co nang dep trai|gia dinh la|jong suk|hoa cua quy|cong to vien|nac thang|yo han|sinh trung|itaewon|mat danh|thanh xuan vat va|goblin|cach mang tinh yeu|chang hiep si|y duc|hai the gioi|bac si|nu luat su|vuot thoi gian|khong kiem soat|tuoi 18|hien nga bong dem|han co trang|doctor|squid game|tro choi con muc|di taewon)\b",
    "Thai-Drama": r"(?i)\b(phim thai|thai lan|thailan|boy love|bl|dam my|nguoc dong thoi gian de yeu anh|nhan duyen tien dinh|kinnporsche|nang ve si|yeu nham chi dau|co vo bat buoc|con tim dat da|duc vong tinh yeu|mat xich han thu|minh chau ruc ro|nabi toi|hoan menh|hoan kiep|nam giu sinh menh|bao cat|khat khao doi tra|nang ran|yeu tham anh xa|baifen|phim ma thai|bach hop)\b",
    "V-Drama": r"(?i)\b(phim viet|viet nam|phim truyen|huong ngay nang ve|giup viec|me ghe|yeu trong dau thuong|nam hoc te|ong co tao|cuoc chien cua nhung ngoi sao|vantientv|nha phuong|tran chien dieu ky|thuong ngay|nha khong ban|duoi bong moc mien|hoai linh|nguoi phan xu|nang 3|con ruot|canh hoa danh vong|thim hai lua|tho san ho ly|ma toi la dai gia|re nha nong|loa phuong|em ten gi|ai chet gio tay|anhbaphai|toi thay hoa vang|thuong ngay nang ve|huong vi tinh than|bo gia|lat mat|tran thanh|ly hai|truong giang|hai kich|ve nha di con|cay tao no hoa|gac kiem|phim rap viet)\b",
    "TV_Show": r"(?i)\b(game show|gameshow|truyen hinh|rap viet|chuong trinh|tv|ban muon hen|giong hat viet|street dance|thu thach|tai nang nhi|sieu tri tue|running|ngoi sao tinh yeu|mnet|nguoi ay la ai|jokers|producer|sao nhap ngu|tau hai|nhanh nhu chop|queeendom|comedy|show|vu dao|street|on gioi cau day roi|2 ngay 1 dem|thach thuc danh hai|guong mat than quen)\b",
    "News": r"(?i)\b(tin tuc|thoi su|tin nhanh|vtv1|vtv2|vtv3|vtv6|sapa tv|kenh|thoi tiet|thvl|htv|tin moi|chuyen dong 24h|an ninh|ban tin|thoisu|tintuc)\b",
    "Education": r"(?i)\b(hoc|tieng anh|ielts|giao trinh|bai giang|vlog|nuoi con|to mau|cuoc song|phd|trai dat|the gioi dong vat|ban do xay dung|cach lam|giao an|animal|dog|ant|ngoi truong|cam dong|niem phat|duong sinh|kham pha|khoa hoc|toan|ly|hoa|lich su|dia ly|ky nang|study|tu vung|english)\b",
    "Adult": r"(?i)\b(fifty|shades|hentai|hong kong sex|sex|phimset|xxx|50|nhay cam|18\+|nguoi lon|cap 3|jav)\b",
    "Movies_General": r"(?i)\b(phim|phim bo|phim le|phim chieu rap|phim hay|xem phim|phim moi|thuyet minh|long tieng|vietsub|tron bo|tap \d+)\b"
}

# =============================================================================
# 3. CÁC HÀM XỬ LÝ DỮ LIỆU (PROCESSING FUNCTIONS)
# =============================================================================
def get_spark_session():
    """Khởi tạo và trả về Spark Session"""
    return SparkSession.builder \
        .appName("ETL_Pipeline_01_AutoMap") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def extract_top_keyword(df, month_label):
    """Làm sạch và tìm keyword được search nhiều nhất của mỗi User trong tháng"""

    df_clean = df \
        .withColumn("dt_en", F.regexp_replace(F.col("datetime"), " CH", " PM")) \
        .withColumn("dt_en", F.regexp_replace(F.col("dt_en"), " SA", " AM"))
        
    # LƯỚI BẮT LỖI TỐI THƯỢNG CỦA SPARK 3.x
    df_clean = df_clean.withColumn("ts", F.coalesce(
        # Nhóm 1: Chuẩn 24h (Ký hiệu là HH)
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd HH:mm:ss.SSS')"), # Có 3 số lẻ
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd HH:mm:ss.SS')"),  # Có 2 số lẻ
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd HH:mm:ss.S')"),   # Có 1 số lẻ
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd HH:mm:ss')"),     # Không số lẻ
        
        # Nhóm 2: Chuẩn 12h AM/PM (Ký hiệu là h)
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd h:mm:ss.SSS a')"), 
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd h:mm:ss.SS a')"),   
        F.expr("try_to_timestamp(dt_en, 'yyyy-MM-dd h:mm:ss a')"),      
        
        # Mặc định cuối cùng
        F.expr("try_to_timestamp(dt_en)")
    ))
    df_clean = df_clean \
        .filter(F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != "")) \
        .filter(F.col("keyword").isNotNull() & (F.trim(F.col("keyword")) != "")) \
        .withColumn("keyword", F.trim(F.lower(F.col("keyword"))))
    
    df_clean = df_clean.repartition("user_id")
    
    df_counts = df_clean.groupBy("user_id", "keyword").agg(
        F.count("*").alias("count"),
        F.max("ts").alias("last_seen")
    )

    w_user = Window.partitionBy("user_id")
    df_with_total = df_counts.withColumn("total_monthly_searches", F.sum("count").over(w_user))

    w_rank = Window.partitionBy("user_id").orderBy(F.col("count").desc(), F.col("last_seen").desc())
    
    return df_with_total \
        .withColumn("rank", F.row_number().over(w_rank)) \
        .filter(F.col("rank") == 1) \
        .select(
            F.col("user_id"), 
            F.col("keyword").alias(f"most_search_{month_label}"),
            F.col("total_monthly_searches").alias(f"cnt_{month_label}") 
        )


def select_top_5k_users(df_behavior):
    """
    THUẬT TOÁN CHẤM ĐIỂM VÀ LỌC TOP 5.000 USERS TIỀM NĂNG
    Nguyên tắc: Ưu tiên User có tần suất hoạt động cao, ổn định và có ý định xem phim rõ ràng.
    """
    
    # Loại bỏ các từ khóa rác (chỉ 1 ký tự) hoặc các dãy số dài vô nghĩa (không phải tên phim)
    df_filtered = df_behavior.filter(
        (F.col("most_search_t7").isNull()) | (F.length(F.col("most_search_t7")) > 1)
    ).filter(
        (F.col("most_search_t7").isNull()) | (~F.col("most_search_t7").rlike("^[0-9]{5,}$"))
    )

    # Nhóm ý định cao: Chứa các từ khóa khẳng định việc tìm phim (tập, vietsub, thuyết minh...)
    high_intent = "(?i)(phim|tập|vietsub|thuyết minh|full|hd|review|trọn bộ|mới nhất|ss[0-9]+|mùa)"
    # Nhóm ý định theo thể loại: Chứa các từ khóa về dòng phim (hành động, kinh dị, anime...)
    genre_intent = "(?i)(hành động|kinh dị|ma|tình cảm|hoạt hình|anime|hài|chiếu rạp)"

    # Dựa trên từ khóa tháng 7 để đánh giá mức độ "khao khát" xem phim của User
    df_scored = df_filtered.withColumn(
        "word_count", F.size(F.split(F.col("most_search_t7"), " "))
    ).withColumn(
        "intent_multiplier",
        F.when(F.col("most_search_t7").rlike(high_intent), 3.0)      # Ưu tiên cao nhất (x3.0)
         .when(F.col("most_search_t7").rlike(genre_intent), 2.0)     # Ưu tiên trung bình (x2.0)
         .when((F.col("word_count") >= 2) & (F.col("word_count") <= 6), 1.5) # Giả định là tên phim (x1.5)
         .otherwise(1.0)                                             # Nhóm mặc định (x1.0)
    )

    # Công thức: (Tháng 7 * 1.5) + (Tháng 6 * 1.0) + Điểm thưởng nếu xuất hiện cả 2 tháng
    bonus_score = 50
    df_scored = df_scored.withColumn(
        "base_score",
        (F.coalesce(F.col("cnt_t7"), F.lit(0)) * 1.5) + 
        (F.coalesce(F.col("cnt_t6"), F.lit(0)) * 1.0) +
        F.when((F.col("cnt_t6") > 0) & (F.col("cnt_t7") > 0), bonus_score).otherwise(0)
    )

    # Điểm cuối cùng = Điểm cơ sở * Hệ số ý định
    df_final_scored = df_scored.withColumn(
        "movie_score", F.col("base_score") * F.col("intent_multiplier")
    )

    # Sắp xếp giảm dần theo điểm. Dùng user_id làm tie-breaker để đảm bảo kết quả ổn định (Deterministic)
    return df_final_scored.orderBy(F.col("movie_score").desc(), F.col("user_id").asc()) \
                          .limit(5000) \
                          .select("user_id", "most_search_t6", "most_search_t7")

def auto_categorize_keywords(df_5k):
    """Áp dụng Rule Engine để tự động phân loại tập Keyword duy nhất"""
    # 1. Trích xuất tập Keyword duy nhất
    kws_t6 = df_5k.select(F.col("most_search_t6").alias("keyword"))
    kws_t7 = df_5k.select(F.col("most_search_t7").alias("keyword"))
    df_unique_kws = kws_t6.union(kws_t7).filter(F.col("keyword").isNotNull()).dropDuplicates()

    # 2. Xây dựng chuỗi F.when() động từ DICTIONARY
    regex_expr = None
    for category, pattern in REGEX_RULES.items():
        if regex_expr is None:
            regex_expr = F.when(F.col("keyword").rlike(pattern), category)
        else:
            regex_expr = regex_expr.when(F.col("keyword").rlike(pattern), category)

    regex_expr = regex_expr.otherwise("Unmapped")
    df_auto_categorized = df_unique_kws.withColumn("category", regex_expr)

    # 3. Tách nhóm Thành công và Thất bại
    df_success = df_auto_categorized.filter(F.col("category") != "Unmapped")
    df_manual = df_auto_categorized.filter(F.col("category") == "Unmapped")
    
    return df_success, df_manual

def export_manual_mapping_file(df_need_manual, export_path):
    """Xuất file Excel cho thao tác map tay"""
    pdf_need_manual = df_need_manual.toPandas()
    pdf_need_manual = pdf_need_manual.rename(columns={"keyword": "Keyword_Gốc"})
    pdf_need_manual["Trưởng_Họ_(Family)"] = pdf_need_manual["Keyword_Gốc"]
    pdf_need_manual = pdf_need_manual[["Keyword_Gốc", "Trưởng_Họ_(Family)", "category"]]
    pdf_need_manual.to_excel(export_path, index=False, engine='openpyxl')

# =============================================================================
# 4. LUỒNG THỰC THI CHÍNH (MAIN PIPELINE)
# =============================================================================
def main():
    print("=== KHỞI ĐỘNG PIPELINE 1: EXTRACT & AUTO MAP ===")
    spark = get_spark_session()
    
    try:
        # Bước 1: Load Data
        print("Đang đọc dữ liệu thô...")
        df_t6 = spark.read.parquet(f"{BASE_PATH}/202206*/*.parquet").select("user_id", "keyword", "datetime")
        df_t7 = spark.read.parquet(f"{BASE_PATH}/202207*/*.parquet").select("user_id", "keyword", "datetime")

        # Bước 2: Tìm Top Keyword
        print("Tiền xử lý và tìm Keyword Top 1...")
        df_t6_clean = extract_top_keyword(df_t6, "t6")
        df_t7_clean = extract_top_keyword(df_t7, "t7")
        df_behavior = df_t6_clean.join(df_t7_clean, on="user_id", how="outer")

        # Bước 3: Lọc Top 5K User
        print("Chấm điểm và trích xuất Top 5.000 Users...")
        df_5k = select_top_5k_users(df_behavior).cache()
        print(f"Đã khóa chặt {df_5k.count()} users vào RAM.")

        # Bước 4: Chạy Auto Categorization (Regex)
        print("Áp dụng Rule Engine để tự động phân loại...")
        df_regex_success, df_need_manual = auto_categorize_keywords(df_5k)

        print(f"Hệ thống tự động Map thành công: {df_regex_success.count()} keywords.")
        print(f"Cần xử lý bằng tay: {df_need_manual.count()} keywords.")

        # Bước 5: Lưu Checkpoint và Export File
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        
        print("Đang lưu trạng thái Checkpoint cho Pipeline 2...")
        df_5k.write.mode("overwrite").parquet(f"{CHECKPOINT_DIR}/df_5k_checkpoint.parquet")
        df_regex_success.write.mode("overwrite").parquet(f"{CHECKPOINT_DIR}/df_regex_success.parquet")

        print("Đang xuất file Excel cho Data Engineer thao tác tay...")
        export_manual_mapping_file(df_need_manual, EXPORT_MANUAL_FILE)
        
        print(f"✅ PIPELINE 1 HOÀN TẤT! Vui lòng mở file '{EXPORT_MANUAL_FILE}', map tay và đổi tên thành 'Tu_Khoa_Da_Map_Tay.xlsx' trước khi chạy Pipeline 2.")

    except Exception as e:
        print(f"Pipeline thất bại do lỗi: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

# =============================================================================
# ĐIỂM KÍCH HOẠT CHƯƠNG TRÌNH (ENTRY POINT)
# =============================================================================
if __name__ == "__main__":
    main()