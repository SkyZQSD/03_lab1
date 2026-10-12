import os, time, timeit, json, urllib.request
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def title(t):
    print("\n=== " + t + " ===", flush=True)


t_start = time.perf_counter()
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[4]")  # service limited to 2000Mi: fewer cores and a smaller heap
    .config("spark.driver.memory", "640m")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
title("SESSION")
print(spark, "version", spark.version, "parallelism", spark.sparkContext.defaultParallelism)
print("startup s", round(time.perf_counter() - t_start, 1))

title("BRONZE")
t_build = time.perf_counter()
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True,
                              inferSchema=True, samplingRatio=0.01, multiLine=True)
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", header=True,
                               inferSchema=True, samplingRatio=0.01, multiLine=True)
users_bronze.printSchema()
orders_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)
print("users_bronze", users_bronze.count(), "orders_bronze", orders_bronze.count())

title("STG_USERS")
users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)
stg_users.select(
    F.sum(F.when(F.col("city").isNull(), 1).otherwise(0)).alias("city_null"),
    F.sum(F.when(F.col("state") == "", 1).otherwise(0)).alias("state_empty"),
    F.sum(F.when(F.col("zip_code") == "", 1).otherwise(0)).alias("zip_empty"),
    F.sum(F.when(F.col("street").isNull(), 1).otherwise(0)).alias("street_null"),
).show()

title("STG_ORDERS")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show(truncate=False)
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

title("SPARK SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql('''
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
''')
stg_orders_sql.show(5, truncate=False)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("asserts OK", stg_orders.count(), stg_orders_sql.count())
stg_orders_df_nodup = (
    orders_typed.withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1).drop("rn")
)
print("sameSemantics(DataFrame, SQL):", stg_orders_df_nodup.sameSemantics(stg_orders_sql))
stg_orders_sql.explain()
stg_users.createOrReplaceTempView("stg_users")
spark.sql('''
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
''').show(10)
print("distinct states:", stg_users.select("state").distinct().count(),
      "| total missing_city:", stg_users.filter(F.col("city").isNull()).count())
stg_users.filter(F.col("city").isNull()).select("street", "state", "zip_code").show(truncate=False)

title("PARQUET")
print("partitions stg_users:", stg_users.rdd.getNumPartitions(),
      "stg_orders:", stg_orders.rdd.getNumPartitions())
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("silver build + parquet write s", round(time.perf_counter() - t_build, 1))
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("parquet asserts OK", users_silver.count(), orders_silver.count())
orders_silver.printSchema()

title("ICEBERG")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)
users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("iceberg asserts OK", users_iceberg.count(), orders_iceberg.count())
spark.sql("DESCRIBE TABLE lab.silver.orders").show(truncate=False)

title("EX1 birthdate_is_valid")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
stg_users_valid = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn("birthdate_is_valid",
                F.coalesce(F.col("birthdate").isNotNull()
                           & (F.col("first_order_at").isNull()
                              | (F.col("birthdate") <= F.col("first_order_at"))), F.lit(False)))
    .drop("first_order_at")
)
stg_orders.createOrReplaceTempView("stg_orders")
stg_users_valid_sql = spark.sql('''
    WITH first_orders AS (
        SELECT user_id, min(ordered_at) AS first_order_at
        FROM stg_orders GROUP BY user_id
    )
    SELECT u.*,
           coalesce(u.birthdate IS NOT NULL
                    AND (f.first_order_at IS NULL OR u.birthdate <= f.first_order_at), false)
               AS birthdate_is_valid
    FROM stg_users u LEFT JOIN first_orders f ON u.user_id = f.user_id
''')
diff = (stg_users_valid.exceptAll(stg_users_valid_sql).count()
        + stg_users_valid_sql.exceptAll(stg_users_valid).count())
print("rows DF", stg_users_valid.count(), "rows SQL", stg_users_valid_sql.count(), "differences", diff)
stg_users_valid.groupBy("birthdate_is_valid").count().show()
(stg_users_valid.filter(~F.col("birthdate_is_valid"))
 .join(first_orders, "user_id").select("username", "birthdate", "first_order_at")
 .orderBy("birthdate").show(5, truncate=False))

title("EX3 cache")
def timed(label, fn):
    t = time.perf_counter(); r = fn(); print(f"{label}: {r} in {(time.perf_counter()-t)*1000:.0f} ms")
timed("users_bronze.count() no cache", users_bronze.count)
timed("orders_bronze.count() no cache", orders_bronze.count)
users_bronze.cache(); orders_bronze.cache()
timed("users_bronze.count() 1st (fills cache)", users_bronze.count)
timed("orders_bronze.count() 1st (fills cache)", orders_bronze.count)
timed("users_bronze.count() cached", users_bronze.count)
timed("orders_bronze.count() cached", orders_bronze.count)
try:
    ui = spark.sparkContext.uiWebUrl
    app = spark.sparkContext.applicationId
    rdds = json.load(urllib.request.urlopen(f"{ui}/api/v1/applications/{app}/storage/rdd"))
    for r in rdds:
        print("Storage:", r["name"][:70], "| partitions", r["numCachedPartitions"],
              "| memory", r["memoryUsed"], "B |", r["storageLevel"])
    stages = json.load(urllib.request.urlopen(f"{ui}/api/v1/applications/{app}/stages"))
    for s in stages[:8]:
        print("Stage", s["stageId"], s["name"][:40], "| input", s["inputBytes"], "B | tasks", s["numTasks"])
except Exception as e:
    print("UI API error", e)

title("EX2 UDF vs pandas_udf vs regexp_extract")
try:
    import re
    import pandas as pd
    from pyspark.sql.functions import udf, pandas_udf
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
          "pandas_udf:", z_native.exceptAll(z_pudf).count() == 0)
    for name, df in [("regexp_extract", z_native), ("python udf", z_udf), ("pandas_udf", z_pudf)]:
        run = lambda: df.write.format("noop").mode("overwrite").save()
        run()
        times = timeit.repeat(run, number=5, repeat=3)
        print(f"{name}: best {min(times)/5*1000:.1f} ms per run (5 runs x 3)")
except Exception as e:
    print("EX2 error:", repr(e)[:300])

title("EX4 Iceberg append + time travel")
new_order = spark.createDataFrame(
    [("00000000-0000-4000-8000-000000000001", stg_orders.first()["user_id"],
      "2026-10-12 12:00:00", 1, "bread")],
    "order_id string, user_id string, ordered_at string, quantity int, product string",
).withColumn("ordered_at", F.col("ordered_at").cast("timestamp"))
new_order.writeTo("lab.silver.orders").append()
snaps = spark.sql("SELECT snapshot_id, parent_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at")
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

print("\ntotal script s", round(time.perf_counter() - t_start, 1))
spark.stop()
