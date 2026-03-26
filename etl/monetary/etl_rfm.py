from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
CSV_PATH = r"C:\Users\Admin\Downloads\Customer_Transaction.csv"

MYSQL_URL = "jdbc:mysql://localhost:3306/bigdata"
MYSQL_PROPS = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver",
}
MYSQL_TABLE = "customer_rfm_stats"


# ── SPARK SESSION ─────────────────────────────────────────────────────────────
def get_spark():
    return (SparkSession.builder
            .config("spark.driver.memory", "8g")
            .config("spark.executor.cores", 8)
            .getOrCreate())


# ── EXTRACT ───────────────────────────────────────────────────────────────────
def read_transaction(spark):
    return spark.read.csv(CSV_PATH, header=True, inferSchema=True)


def clean_data(df):
    return df.filter((col("GMV") > 0) & (col("CustomerID") != 0))


# ── TRANSFORM ─────────────────────────────────────────────────────────────────
def compute_rfm(data):
    max_date = data.select(max("Purchase_Date")).collect()[0][0] + timedelta(days=1)

    df_total = (data
        .groupBy("CustomerID")
        .agg(
            date_diff(lit(max_date), max("Purchase_Date")).alias("Recency"),
            count(col("Transaction_ID")).alias("Frequency"),
            sum(col("GMV")).alias("Monetary"),
        )
        .withColumn("AOV", round(col("Monetary") / col("Frequency")))
    )
    return df_total


def score_rfm(df_total):
    w_r = Window.orderBy("Recency")
    w_f = Window.orderBy("Frequency")
    w_m = Window.orderBy("Monetary")

    df_scored = (df_total
        .withColumn("R_Score", (lit(6) - ntile(5).over(w_r)).cast("int"))
        .withColumn("F_Score", ntile(5).over(w_f).cast("int"))
        .withColumn("M_Score", ntile(5).over(w_m).cast("int"))
        .withColumn("RFM_Score", concat(col("R_Score"), col("F_Score"), col("M_Score")))
    )
    return df_scored


def assign_segment(df_scored):
    df = df_scored.withColumn("FM_Score", round((col("F_Score") + col("M_Score")) / 2, 0).cast("int"))

    df = df.withColumn("Segment",
        when((col("R_Score") >= 4) & (col("FM_Score") >= 4), "Champions")
        .when((col("R_Score") >= 3) & (col("FM_Score") >= 4), "Loyal Customers")
        .when((col("R_Score") >= 4) & (col("FM_Score").between(2, 3)), "Potential Loyalist")
        .when((col("R_Score") >= 4) & (col("FM_Score") <= 1), "Recent Customers")
        .when((col("R_Score") == 3) & (col("FM_Score") <= 1), "Promising")
        .when((col("R_Score") == 3) & (col("FM_Score").between(2, 3)), "Need Attention")
        .when((col("R_Score") == 2) & (col("FM_Score").between(1, 2)), "About to Sleep")
        .when((col("R_Score") == 2) & (col("FM_Score") >= 3), "At Risk")
        .when((col("R_Score") == 1) & (col("FM_Score") >= 4), "Can't Lose Them")
        .when((col("R_Score") == 1) & (col("FM_Score").between(2, 3)), "Hibernating")
        .otherwise("Lost")
    )
    return df


# ── LOAD ──────────────────────────────────────────────────────────────────────
def select_output(df_segment):
    return df_segment.select(
        "CustomerID", "Recency", "Frequency", "Monetary", "AOV", "RFM_Score", "Segment"
    )


def write_to_mysql(df_output):
    df_output.write.jdbc(
        url=MYSQL_URL,
        table=MYSQL_TABLE,
        mode="append",
        properties=MYSQL_PROPS,
    )
    print("✅ Ghi xong vào MySQL!")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = get_spark()

    # ── Extract ───────────────────────────────────────────────
    print("\nĐọc dữ liệu giao dịch...")
    df_raw = read_transaction(spark)
    raw_count = df_raw.count()
    print(f"       Tổng số dòng: {raw_count:,}")

    data = clean_data(df_raw)
    clean_count = data.count()
    removed = raw_count - clean_count
    print(f"       Sau lọc: {clean_count:,} dòng (loại {removed:,} dòng rác)")

    # ── Transform ─────────────────────────────────────────────
    print("\nTransforming...")
    df_total = compute_rfm(data)
    customer_count = df_total.count()
    print(f"       Số khách hàng: {customer_count:,}")
    df_total.show(5, truncate=False)

    df_scored = score_rfm(df_total)
    df_scored.select("CustomerID", "Recency", "Frequency", "Monetary",
                     "R_Score", "F_Score", "M_Score", "RFM_Score").show(5, truncate=False)

    df_segment = assign_segment(df_scored)
    df_segment.groupBy("Segment").count().orderBy(desc("count")).show(truncate=False)

    df_output = select_output(df_segment)

    # ── Load ──────────────────────────────────────────────────
    print("Ghi kết quả vào MySQL...")
    # write_to_mysql(df_output)

    print("\n" + "=" * 60)
    print(f"✅ HOÀN TẤT — {customer_count:,} khách hàng → {MYSQL_TABLE}")
    print("=" * 60)

    spark.stop()
