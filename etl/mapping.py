import json
import os
import shutil
import findspark
findspark.init()
from pyspark.sql import SparkSession
from openai import OpenAI

# ── SPARK ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Movie Classification") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# ── ĐƯỜNG DẪN ──────────────────────────────────────
path1        = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\most_search_t6"
path2        = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\most_search_t7"
output_path1 = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\search_category_t6.csv"
output_path2 = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\search_category_t7.csv"

# ── HÀM ĐỌC CSV ────────────────────────────────────
def read_csv(path):
    return spark.read.option("header", "true").csv(path)

# ── HÀM WRITE AN TOÀN (tránh Py4JJavaError) ────────
def write_csv(pandas_df, path):
    # Dùng Pandas để tránh lỗi Spark write trên OneDrive
    pandas_df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {path}")

# ── GEMMA LOCAL CLIENT ──────────────────────────────
client = OpenAI(
    base_url="http://127.0.0.1:1234/v1",
    api_key="lmstudio"
)

# ── HÀM CLASSIFY ───────────────────────────────────
def classify_batch(movie_list):
    if not movie_list:
        return {}
    prompt = f"""
    Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí.  
    Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan đến nội dung.

    ⚠️ Nguyên tắc quan trọng:
    - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa.  
    - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng.  
    - Nếu không chắc → chọn thể loại gần nhất (VD: từ mô tả tình cảm → Romance, tên địa danh thể thao → Sports, chương trình giải trí → Reality Show, v.v.)

    Nhiệm vụ của bạn:
    1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả (vd: "thuyếtminh" → "Thuyết minh", "tramnamu" → "Trăm năm hữu duyên", "capdoi" → "Cặp đôi").
    2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
    - Tên phim, series, show, chương trình
    - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
    - Từ khoá mô tả nội dung (→ phân loại theo ý nghĩa, ví dụ "thuyếtminh" → "Other" hoặc "Drama", "bigfoot" → "Horror")
    3. **Gán thể loại phù hợp nhất** trong các nhóm sau:  
    - Action  
    - Romance  
    - Comedy  
    - Horror  
    - Animation  
    - Drama  
    - C Drama  
    - K Drama  
    - Sports  
    - Music  
    - Reality Show  
    - TV Channel  
    - News  
    - Other

    Một số quy tắc gợi ý nhanh:
    - Có từ “VTV”, “HTV”, “Channel” → TV Channel  
    - Có “running”, “master key”, “reality” → Reality Show  
    - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News  
    - “sex”, “romantic”, “love” → Romance  
    - “potter”, “hogwarts” → Drama / Fantasy  
    - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

    Chỉ trả về **1 JSON object**.  
    Key = tên gốc trong danh sách.  
    Value = thể loại đã phân loại.

    Ví dụ:  
    {{
    "thuyếtminh": "Other",
    "bigfoot": "Horror",
    "capdoi": "Romance",
    "ARGEN": "Sports",
    "nhật ký": "Drama",
    "PENT": "C Drama",
    "running": "Reality Show",
    "VTV3": "TV Channel"
    }}

    Danh sách:
    {movie_list}

    """
    try:
        resp = client.chat.completions.create(
            model="google/gemma-3-4b",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=4000
        )
        text = resp.choices[0].message.content.strip()
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end == -1:
            return {m: "Other" for m in movie_list}
        parsed = json.loads(text[start:end+1])
        return {title: parsed.get(title, "Other") for title in movie_list}
    except Exception as e:
        print("Error:", e)
        return {m: "Other" for m in movie_list}

# ── XỬ LÝ THÁNG 6 ──────────────────────────────────
print("=== THÁNG 6 ===")
df_t6 = read_csv(path1)
pdf_t6 = df_t6.limit(100).toPandas()
movies_t6 = pdf_t6["keyword"].dropna().astype(str).tolist()
mapping_t6 = classify_batch(movies_t6)
pdf_t6["Genre"] = pdf_t6["keyword"].map(lambda x: mapping_t6.get(str(x), "Other"))
print(pdf_t6[["keyword", "Genre"]].head(10))
write_csv(pdf_t6, output_path1)

# ── XỬ LÝ THÁNG 7 ──────────────────────────────────
print("\n=== THÁNG 7 ===")
df_t7 = read_csv(path2)
pdf_t7 = df_t7.limit(100).toPandas()
movies_t7 = pdf_t7["keyword"].dropna().astype(str).tolist()
mapping_t7 = classify_batch(movies_t7)
pdf_t7["Genre"] = pdf_t7["keyword"].map(lambda x: mapping_t7.get(str(x), "Other"))
print(pdf_t7[["keyword", "Genre"]].head(10))
write_csv(pdf_t7, output_path2)

print("\n✅ HOÀN THÀNH!")
