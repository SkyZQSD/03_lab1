# Part 2 of the lab (exercises 2 and 4), run separately: with a 2000Mi service the
# Python UDF workers of exercise 2 pushed the full script over the memory limit.
import os, time, timeit
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import re

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
spark = (
    SparkSession.builder.appName("lab-silver-pyspark-part2").master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True,
                              inferSchema=True, samplingRatio=0.01, multiLine=True).cache()
users_bronze.count()
stg_orders = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")

print("\n=== EX2 UDF vs pandas_udf vs regexp_extract ===", flush=True)
ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")

@udf(StringType())
def zip_udf(address):
    if address is None:
        return None
    m = ZIP_RE.search(address)
    return m.group(2) if m else ""

@pandas_udf(StringType())
def zip_pandas_udf(address: pd.Series) -> pd.Series:
    return address.str.extract(r"([A-Z]{2}) ([0-9]{5})$", expand=True)[1].fillna("")

z_native = users_bronze.select(F.regexp_extract("address", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip"))
z_udf = users_bronze.select(zip_udf("address").alias("zip"))
z_pudf = users_bronze.select(zip_pandas_udf("address").alias("zip"))
print("same result udf:", z_native.exceptAll(z_udf).count() == 0,
      "pandas_udf:", z_native.exceptAll(z_pudf).count() == 0, flush=True)
for name, df in [("regexp_extract", z_native), ("python udf", z_udf), ("pandas_udf", z_pudf)]:
    run = lambda: df.write.format("noop").mode("overwrite").save()
    run()
    times = timeit.repeat(run, number=5, repeat=3)
    print(f"{name}: best {min(times)/5*1000:.1f} ms per run (5 runs x 3)", flush=True)
z_udf.explain()
z_pudf.explain()

print("\n=== EX4 Iceberg append + time travel ===", flush=True)
new_order = spark.createDataFrame(
    [("00000000-0000-4000-8000-000000000001", stg_orders.first()["user_id"],
      "2026-10-12 12:00:00", 1, "bread")],
    "order_id string, user_id string, ordered_at string, quantity int, product string",
).withColumn("ordered_at", F.col("ordered_at").cast("timestamp"))
print("count before append:", spark.table("lab.silver.orders").count())
new_order.writeTo("lab.silver.orders").append()
snaps = spark.sql("SELECT snapshot_id, parent_id, operation, committed_at "
                  "FROM lab.silver.orders.snapshots ORDER BY committed_at")
snaps.show(truncate=False)
first_snap = snaps.first()["snapshot_id"]
print("current count:", spark.table("lab.silver.orders").count())
spark.sql(f"SELECT count(*) AS orders_before_append FROM lab.silver.orders VERSION AS OF {first_snap}").show()
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor FROM lab.silver.orders.history").show(truncate=False)
pq = f"s3a://{BUCKET}/silver/spark/orders_timetravel.parquet"
stg_orders.write.mode("overwrite").parquet(pq)
print("parquet copy before append:", spark.read.parquet(pq).count())
new_order.write.mode("append").parquet(pq)
print("parquet copy after append:", spark.read.parquet(pq).count(), "(no previous version can be read)")
spark.stop()
