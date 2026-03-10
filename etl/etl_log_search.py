import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PATH_T6       = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\log_search\202206*"
PATH_T7       = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\log_search\202207*"
SAVE_PATH_T6  = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\most_search_t6"
SAVE_PATH_T7  = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\most_search_t7"

spark = SparkSession.builder.appName("ETL_LogSearch").getOrCreate()

# ── FUNCTIONS ──────────────────────────────────────────────────────────────────
def read_parquet(path):
    return spark.read.parquet(path)

def most_searched(df):
    result = df.groupBy("user_id","keyword").agg(count("keyword").alias("search_count"))
    window = Window.partitionBy("user_id").orderBy(col("search_count").desc())
    return (result
            .withColumn("rank", row_number().over(window))
            .filter(col("rank") == 1)
            .drop("rank"))

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df_june = read_parquet(PATH_T6)
    df_july = read_parquet(PATH_T7)

    print(f"Total unique keywords T6+T7: {df_june.union(df_july).select(count_distinct('keyword')).collect()[0][0]:,}")

    raw_june = most_searched(df_june).filter(col("keyword").isNotNull())
    raw_july = most_searched(df_july).filter(col("keyword").isNotNull())

    print("=== Top keywords T6 ===")
    raw_june.groupBy("keyword").count().orderBy(col("count").desc()).show(20, truncate=False)

    print("=== Top keywords T7 ===")
    raw_july.groupBy("keyword").count().orderBy(col("count").desc()).show(20, truncate=False)

    raw_june.coalesce(1).write.mode("overwrite").option("header","true").csv(SAVE_PATH_T6)
    raw_july.coalesce(1).write.mode("overwrite").option("header","true").csv(SAVE_PATH_T7)
    print("✅ Saved most_search T6 & T7")
