import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PATH_MAPPING_T6 = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\search_category_t6.csv"
PATH_MAPPING_T7 = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\search_category_t7.csv"
PATH_LOG_T6     = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\log_search\202206*"
PATH_LOG_T7     = r"C:\Users\Admin\OneDrive\Desktop\BigData_Gen15\DATA\log_search\202207*"

MYSQL_HOST  = "localhost"
MYSQL_PORT  = "3306"
MYSQL_USER  = "root"
MYSQL_PASS  = "123456"
MYSQL_DB    = "bigdata"
MYSQL_TABLE = "customer_search_stats"


spark = (SparkSession.builder
         .config("spark.driver.memory", "8g")
         .config("spark.executor.cores", 8)
         .getOrCreate())

# ── FUNCTIONS ──────────────────────────────────────────────────────────────────
def read_csv(path):
    return spark.read.csv(path, header=True, inferSchema=True)

def read_parquet(path):
    return spark.read.parquet(path)

def most_searched_category(df):
    result = df.groupBy("user_id", "keyword").agg(count("keyword").alias("search_count"))
    window = Window.partitionBy("user_id").orderBy(col("search_count").desc())
    return (result
            .withColumn("rank", row_number().over(window))
            .filter(col("rank") == 1)
            .drop("rank"))

def mapping_category(df, mapping_df):
    return (df.join(mapping_df, df.keyword == mapping_df.keyword, "inner")
              .select(df.user_id, df.keyword, mapping_df.Genre, df.Month))

def import_to_mysql(df):
    url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    (df.write
       .format("jdbc")
       .option("url", url)
       .option("driver", "com.mysql.cj.jdbc.Driver")
       .option("dbtable", MYSQL_TABLE)
       .option("user", MYSQL_USER)
       .option("password", MYSQL_PASS)
       .mode("overwrite")       
       .save())
    print("✅ Saved customer_search_stats to MySQL")

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df_mapping_t6 = read_csv(PATH_MAPPING_T6)
    df_mapping_t7 = read_csv(PATH_MAPPING_T7)

    df_t6 = most_searched_category(read_parquet(PATH_LOG_T6)).withColumn("Month", lit(6)).drop("search_count")
    df_t7 = most_searched_category(read_parquet(PATH_LOG_T7)).withColumn("Month", lit(7)).drop("search_count")

    df_clean_t6 = mapping_category(df_t6, df_mapping_t6)
    df_clean_t7 = mapping_category(df_t7, df_mapping_t7)
    df = df_clean_t6.union(df_clean_t7)

    user_most_search = (df.select("user_id", "keyword", "Month")
        .groupBy("user_id").pivot("Month", [6, 7]).agg(first("keyword"))
        .withColumnRenamed("6", "Most_Searched_T6")
        .withColumnRenamed("7", "Most_Searched_T7")
        .filter(col("Most_Searched_T6").isNotNull() & col("Most_Searched_T7").isNotNull()))

    df_mapping_clean = (df_mapping_t6.union(df_mapping_t7)
                        .dropDuplicates(["keyword"])
                        .select("keyword", "Genre"))

    user_most_search = (user_most_search.alias("a")
        .join(df_mapping_clean.alias("b"), col("a.Most_Searched_T6") == col("b.keyword"), "left")
        .select("a.user_id", "a.Most_Searched_T6", col("b.Genre").alias("Genre_T6"), "a.Most_Searched_T7"))

    user_most_search = (user_most_search.alias("a")
        .join(df_mapping_clean.alias("b"), col("a.Most_Searched_T7") == col("b.keyword"), "left")
        .select("a.user_id", "a.Most_Searched_T6", "a.Genre_T6", "a.Most_Searched_T7", col("b.Genre").alias("Genre_T7")))

    result = (user_most_search
        .withColumn("Trending_Type", when(col("Genre_T6") == col("Genre_T7"), "Unchanged").otherwise("Changed"))
        .withColumn("Changing",
            when(col("Trending_Type") == "Changed", concat(col("Genre_T6"), lit(" -> "), col("Genre_T7")))
            .otherwise("No Change"))
        .drop("Trending_Type"))

    result.show(10, truncate=False)
    # import_to_mysql(result)
