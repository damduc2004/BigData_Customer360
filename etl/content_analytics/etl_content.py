import os
from datetime import datetime, timedelta
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as sf

# ── CONFIG ─────────────────────────────────────────────────────────────────────
BASE_PATH   = r"C:\Users\Admin\OneDrive\Desktop\DATA\log_content"
MYSQL_HOST  = "localhost"
MYSQL_PORT  = "3306"
MYSQL_USER  = "root"
MYSQL_PASS  = "123456"
MYSQL_DB    = "bigdata"
MYSQL_TABLE = "customer_content_stats"

spark = (SparkSession.builder
         .config("spark.driver.memory", "8g")
         .config("spark.executor.cores", 8)
         .getOrCreate())

# ── FUNCTIONS ──────────────────────────────────────────────────────────────────
def read_data(path):
    return spark.read.json(path)

def select_source(df):
    return df.select("_source.*")

def transform_category(df):
    return (df
        .withColumn("Type",
            when(col("AppName").isin("CHANNEL","DSHD","KPLUS","KPlus"), "Truyền Hình")
            .when(col("AppName").isin("VOD","FIMS_RES","BHD_RES","VOD_RES","FIMS","BHD","DANET"), "Phim Truyện")
            .when(col("AppName") == "RELAX", "Giải Trí")
            .when(col("AppName") == "CHILD", "Thiếu Nhi")
            .when(col("AppName") == "SPORT", "Thể Thao")
            .otherwise("Error"))
        .filter(col("Type") != "Error"))

def total_devices(df):
    return (df.filter(col("Contract") != "0")
              .groupBy("Contract")
              .agg(count_distinct("Mac").alias("Total_Devices"))
              .orderBy(col("Total_Devices").desc()))

def statistic_category(df):
    return (df.filter(col("Contract") != "0")
              .groupBy("Contract","Type")
              .agg(sum("TotalDuration").alias("Total_Duration"))
              .groupBy("Contract")
              .pivot("Type")
              .agg(first("Total_Duration"))
              .fillna(0))

def results(df_devices, df_stats):
    return df_stats.join(df_devices, on="Contract", how="inner")

def ETL_1_DAY(path, process_date):
    print(f"--- Reading {process_date} ---")
    df = select_source(read_data(path))
    df = transform_category(df)
    df_devices = total_devices(df)
    df_stats   = statistic_category(df)
    df_result  = results(df_devices, df_stats)
    return df_result.withColumn("Process_Date", lit(process_date))

def main_task():
    start = datetime.strptime(input("Start date (YYYYMMDD): "), "%Y%m%d").date()
    end   = datetime.strptime(input("End date   (YYYYMMDD): "), "%Y%m%d").date()
    current, df_all = start, None
    while current <= end:
        date_str  = current.strftime("%Y%m%d")
        file_path = os.path.join(BASE_PATH, f"{date_str}.json")
        if os.path.isfile(file_path):
            df_day = ETL_1_DAY(file_path, current.strftime("%Y-%m-%d"))
            df_all = df_day if df_all is None else df_all.unionByName(df_day)
        current += timedelta(days=1)
    return df_all.cache()

def most_watch(df):
    df = df.withColumn("MostWatch", greatest(
        col("`Giải Trí`"), col("`Phim Truyện`"),
        col("`Thể Thao`"), col("`Thiếu Nhi`"), col("`Truyền Hình`")))
    return df.withColumn("MostWatch",
        when(col("MostWatch") == col("`Truyền Hình`"), "Truyền Hình")
        .when(col("MostWatch") == col("`Phim Truyện`"), "Phim Truyện")
        .when(col("MostWatch") == col("`Thể Thao`"),    "Thể Thao")
        .when(col("MostWatch") == col("`Thiếu Nhi`"),   "Thiếu Nhi")
        .when(col("MostWatch") == col("`Giải Trí`"),    "Giải Trí"))

def customer_taste(df):
    return df.withColumn("Taste", concat_ws("-",
        when(col("`Giải Trí`")    > 0, lit("Giải Trí")),
        when(col("`Phim Truyện`") > 0, lit("Phim Truyện")),
        when(col("`Thể Thao`")    > 0, lit("Thể Thao")),
        when(col("`Thiếu Nhi`")   > 0, lit("Thiếu Nhi")),
        when(col("`Truyền Hình`") > 0, lit("Truyền Hình"))))

def find_active(df):
    df_active = (df.groupBy("Contract")
                   .agg(count_distinct("Process_Date").alias("Active_Days"))
                   .withColumn("Active", when(col("Active_Days") > 4, "High").otherwise("Low"))
                   .drop("Active_Days"))
    df_agg = (df.groupBy("Contract")
                .agg(
                    sum("Total_Devices").alias("Total_Devices"),
                    sum("`Giải Trí`").alias("Total_Giai_Tri"),
                    sum("`Phim Truyện`").alias("Total_Phim_Truyen"),
                    sum("`Thể Thao`").alias("Total_The_Thao"),
                    sum("`Thiếu Nhi`").alias("Total_Thieu_Nhi"),
                    sum("`Truyền Hình`").alias("Total_Truyen_Hinh"),
                    first("MostWatch").alias("MostWatch"),
                    first("Taste").alias("Taste")))
    return df_agg.join(df_active, on="Contract", how="left")

def import_to_mysql(df):
    url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    (df.write.format("jdbc")
       .option("url", url)
       .option("driver", "com.mysql.cj.jdbc.Driver")
       .option("dbtable", MYSQL_TABLE)
       .option("user", MYSQL_USER)
       .option("password", MYSQL_PASS)
       .mode("append")
       .save())
    print("✅ Data imported to MySQL successfully")

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = main_task()
    df = most_watch(df)
    df = customer_taste(df)
    df = find_active(df)
    # import_to_mysql(df)
