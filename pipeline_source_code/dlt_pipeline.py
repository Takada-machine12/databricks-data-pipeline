import dlt
from pyspark.sql.functions import col, current_timestamp, avg, count

# -------------------------------------------------------------------
# 1. Bronze層：Auto Loader による自動増分取り込み
# -------------------------------------------------------------------
@dlt.table(
    comment="Raw diamonds data loaded incrementally via Auto Loader",
    table_properties={"quality": "bronze"}
)
def bronze_diamonds():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/my_ecommerce_pipeline/bronze/checkpoints/raw_data/")
        .select("*", col("_metadata.file_path").alias("input_file_name"), current_timestamp().alias("ingestion_time"))
    )

# -------------------------------------------------------------------
# 2. Silver層：クレンジング + データ品質ルール (Expectations)
# -------------------------------------------------------------------
@dlt.table(
    comment="Cleaned and validated diamonds data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_price", "price > 0")        # 価格が0以下なら自動ドロップ
@dlt.expect_or_drop("valid_carat", "carat > 0")        # カラットが0以下なら自動ドロップ
def silver_diamonds():
    # dlt.readStream で Bronze テーブルの変更をストリーミング読み込み
    return (
        dlt.readStream("bronze_diamonds")
        .filter(col("carat").isNotNull() & col("price").isNotNull())
        .dropDuplicates(["carat", "cut", "color", "clarity", "depth", "table", "price", "x", "y", "z"])
    )

# -------------------------------------------------------------------
# 3. Gold層：ビジネス集計マート
# -------------------------------------------------------------------
@dlt.table(
    comment="Aggregated summary of diamonds by cut and color",
    table_properties={"quality": "gold"}
)
def gold_diamond_summary():
    # dlt.read で Silver テーブルから集計用データを読み取り
    return (
        dlt.read("silver_diamonds")
        .groupBy("cut", "color")
        .agg(
            avg("price").alias("avg_price"),
            avg("carat").alias("avg_carat"),
            count("*").alias("total_count")
        )
    )