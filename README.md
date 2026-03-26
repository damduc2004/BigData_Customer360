# Customer Behavior Analytics

Hệ thống phân tích hành vi khách hàng end-to-end cho nền tảng streaming FPT Play.  
Xử lý dữ liệu bằng **Apache PySpark**, lưu trữ **MySQL**, trực quan hóa bằng **Streamlit**.

---

## 🏗️ Kiến trúc hệ thống

```
Raw Data
├── log_content/   (JSON, daily)       → etl_content.py  → MySQL: customer_content_stats
├── log_search/    (Parquet, T6 & T7)  → etl_search.py   → MySQL: customer_search_stats
└── Customer_Transaction.csv           → etl_rfm.py      → MySQL: customer_rfm_stats
                                                               ↓
                                                        Streamlit Dashboard (3 tabs)
```

## 📂 Cấu trúc project

```
BigData_UserBehavior/
├── README.md
├── etl/
│   ├── content_analytics/
│   │   └── etl_content.py          # ETL log xem nội dung
│   ├── search_analytics/
│   │   ├── etl_log_search.py       # Trích xuất top keyword mỗi user
│   │   ├── etl_search_stats.py     # Mapping genre + trend T6→T7
│   │   └── mapping.py              # LLM-based keyword → genre classification
│   └── rfm_analytics/
│       ├── etl_rfm.py              # RFM scoring & segmentation
│       └── ETL_pipeline.ipynb      # Notebook phân tích (dev/exploration)
└── web_app/
    └── app.py                       # Streamlit dashboard
```

---

## 📦 3 Module ETL

### Module 1 — Content Analytics (`etl/content_analytics/etl_content.py`)

| Item | Chi tiết |
|------|----------|
| **Input** | `log_content/{YYYYMMDD}.json` |
| **Output** | MySQL → `customer_content_stats` |
| **Key columns** | Contract, Total_Devices, Total_Truyen_Hinh, Total_Phim_Truyen, Total_Giai_Tri, Total_The_Thao, Total_Thieu_Nhi, MostWatch, Taste, Active |

**Logic:**
1. Đọc JSON, flatten `_source.*`
2. Map `AppName` → 5 thể loại (Truyền Hình, Phim Truyện, Giải Trí, Thể Thao, Thiếu Nhi)
3. Pivot `TotalDuration` theo thể loại
4. `MostWatch` = thể loại có duration lớn nhất
5. `Taste` = concat các thể loại có duration > 0 (nối `-`)
6. `Active` = "High" nếu active_days > 4, ngược lại "Low"

### Module 2 — Search Analytics (`etl/search_analytics/`)

| Item | Chi tiết |
|------|----------|
| **Input** | Parquet `log_search/202206*`, `log_search/202207*` + CSV mapping |
| **Output** | MySQL → `customer_search_stats` |
| **Key columns** | user_id, Most_Searched_T6, Genre_T6, Most_Searched_T7, Genre_T7, Changing |

**Logic:**
1. Group `(user_id, keyword)` → đếm `search_count`
2. `row_number()` lấy top keyword mỗi user mỗi tháng
3. Join với mapping CSV (keyword → Genre, phân loại bằng LLM Gemma 3 4B)
4. Pivot theo tháng → `Most_Searched_T6/T7`, `Genre_T6/T7`
5. `Changing` = "No Change" nếu genre không đổi, ngược lại "{T6} -> {T7}"

### Module 3 — RFM Analysis (`etl/rfm_analytics/etl_rfm.py`)

| Item | Chi tiết |
|------|----------|
| **Input** | `Customer_Transaction.csv` (Transaction_ID, CustomerID, Purchase_Date, GMV) |
| **Output** | MySQL → `customer_rfm_stats` |
| **Key columns** | CustomerID, Recency, Frequency, Monetary, AOV, RFM_Score, Segment |

**Logic:**
1. Filter `GMV > 0` và `CustomerID ≠ 0`
2. Aggregate: Recency, Frequency, Monetary, AOV
3. Score bằng `ntile(5)` → R_Score (đảo ngược), F_Score, M_Score
4. `RFM_Score` = concat 3 score (VD: "543")
5. `FM_Score` = avg(F + M) → phân 11 segment (Champions → Lost)

---

## 🖥️ Dashboard (Streamlit)

Chạy:
```bash
streamlit run web_app/app.py
```

3 tabs:

| Tab | Nội dung |
|-----|----------|
| **💰 RFM Segments** | KPI cards, Segment distribution, Revenue by Segment, Treemap, Top RFM Scores |
| **🎬 Content Behavior** | Thời lượng theo thể loại, MostWatch, Active vs Low, Top Taste |
| **🔍 Search Trends** | Genre T6 vs T7, Top Changing trends, Sankey flow T6→T7 |

### 📸 Output Samples

#### 1) RFM Segments
![RFM Output](docs/images/output-rfm.png)

#### 2) Content Behavior
![Content Output](docs/images/output-content.png)

#### 3) Search Trends
![Search Output](docs/images/output-search.png)

---

## ⚙️ Cài đặt & Chạy

### Prerequisites
- Python 3.11+
- Java 8+ (cho PySpark)
- MySQL 8.0+

### 1. Cài thư viện
```bash
pip install pyspark mysql-connector-python streamlit plotly sqlalchemy
```

### 2. Tạo database MySQL
```sql
CREATE DATABASE IF NOT EXISTS bigdata;
```

### 3. Chạy ETL pipeline
```bash
# Module 1: Content
python etl/content_analytics/etl_content.py

# Module 2: Search
python etl/search_analytics/etl_log_search.py
python etl/search_analytics/etl_search_stats.py

# Module 3: RFM
python etl/rfm_analytics/etl_rfm.py
```

### 4. Khởi động Dashboard
```bash
streamlit run web_app/app.py
```

Truy cập: `http://localhost:8501`

---

## 🔧 Cấu hình

MySQL connection được cấu hình ở đầu mỗi file ETL và `web_app/app.py`:

```python
# ETL (JDBC)
MYSQL_URL = "jdbc:mysql://localhost:3306/bigdata"

# Streamlit (SQLAlchemy)
MYSQL_URI = "mysql+mysqlconnector://root:123456@localhost:3306/bigdata"
```

---

## 📊 MySQL Tables Schema

### customer_content_stats
| Column | Type | Description |
|--------|------|-------------|
| Contract | string | Mã hợp đồng khách hàng |
| Total_Devices | int | Số thiết bị sử dụng |
| Total_Truyen_Hinh | long | Thời lượng xem Truyền Hình |
| Total_Phim_Truyen | long | Thời lượng xem Phim Truyện |
| Total_Giai_Tri | long | Thời lượng xem Giải Trí |
| Total_The_Thao | long | Thời lượng xem Thể Thao |
| Total_Thieu_Nhi | long | Thời lượng xem Thiếu Nhi |
| MostWatch | string | Thể loại xem nhiều nhất |
| Taste | string | Các thể loại đã xem (VD: "Truyền Hình-Phim Truyện") |
| Active | string | "High" hoặc "Low" |

### customer_search_stats
| Column | Type | Description |
|--------|------|-------------|
| user_id | string | Mã khách hàng |
| Most_Searched_T6 | string | Keyword tìm nhiều nhất tháng 6 |
| Genre_T6 | string | Genre tháng 6 |
| Most_Searched_T7 | string | Keyword tìm nhiều nhất tháng 7 |
| Genre_T7 | string | Genre tháng 7 |
| Changing | string | "No Change" hoặc "Genre_T6 -> Genre_T7" |

### customer_rfm_stats
| Column | Type | Description |
|--------|------|-------------|
| CustomerID | int | Mã khách hàng |
| Recency | int | Số ngày từ lần mua gần nhất |
| Frequency | int | Số lần mua |
| Monetary | long | Tổng tiền chi |
| AOV | double | Giá trị đơn hàng trung bình |
| RFM_Score | string | Chuỗi 3 chữ số (VD: "543") |
| Segment | string | 1 trong 11 phân khúc |

---

# BigData_Customer360
