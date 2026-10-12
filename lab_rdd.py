# Lab: Introduction to Spark's RDD API
# Run inside the pyspark shell:  exec(open("lab_rdd.py").read())
# Nothing is stopped or unpersisted at the end: all variables stay available.
import re, time
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

def title(t):
    print("\n" + "=" * 70 + "\n" + t + "\n" + "=" * 70)

# ---------------------------------------------------------------- SparkContext
spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print("Spark", spark.version, "| UI:", sc.uiWebUrl, "| defaultParallelism:", sc.defaultParallelism)

# ---------------------------------------------------- RDD creation & partitions
title("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users_lines partitions :", users_lines.getNumPartitions())
print("orders_lines partitions:", orders_lines.getNumPartitions())
print("sc.defaultMinPartitions:", sc.defaultMinPartitions)
orders_repartitioned = orders_lines.repartition(8)
print("orders_repartitioned   :", orders_repartitioned.getNumPartitions())
orders_coalesced = orders_repartitioned.coalesce(2)
print("orders_coalesced       :", orders_coalesced.getNumPartitions())
print("--- lineage repartition(8) (note the ShuffledRDD):")
print(orders_repartitioned.toDebugString().decode())
print("--- lineage coalesce(2) (no new shuffle, CoalescedRDD):")
print(orders_lines.coalesce(1).toDebugString().decode())

# ---------------------------------------------------- Lazy evaluation & lineage
title("Lazy evaluation and lineage")
header = orders_lines.first()
print("header:", header)
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
sc.setJobDescription("orders_data.count() #1")
print("count #1:", orders_data.count())
sc.setJobDescription("orders_data.count() #2")
print("count #2:", orders_data.count())

# ------------------------------------------------------ Multiline address issue
title("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(line)
sc.setJobDescription("users_lines.count()")
print("users_lines.count():", users_lines.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
sc.setJobDescription("users_records.count()")
print("users_records.count():", users_records.count())
users_df_check = spark.read.csv("users.csv", header=True, multiLine=True, escape='"')
print("DataFrame multiLine=True count:", users_df_check.count())

# --------------------------------------------------- Manual parsing of orders
title("Manual parsing of orders")
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)

# Q: extra comma inside product -> demo on a single bad line
bad = sc.parallelize(["x,y,2020-01-01,3,bread, with butter"]).map(parse_order)
print("transformation built fine (lazy):", bad)
try:
    bad.collect()
except Exception as e:
    print("Fails only at the action ->", type(e).__name__, "|",
          [l for l in str(e).splitlines() if "ValueError" in l][:1])

# ------------------------------------------- Key-value RDDs: quantity per user
title("Key-value RDDs: total quantity per user")
sc.setJobDescription("user_quantity reduceByKey")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print(user_quantity.take(5))

sc.setJobDescription("user_quantity_grouped groupByKey")
user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print(user_quantity_grouped.take(5))
print("same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

# --------------------------------------------------------- Manual deduplication
title("Manual deduplication")
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

sc.setJobDescription("orders_dedup reduceByKey(most_recent)")
orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)
print("orders_rdd.count(), orders_dedup.count():", orders_rdd.count(), orders_dedup.count())

# -------------------------------------------------- Joining orders with users
title("Joining orders with users")
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

sc.setJobDescription("enriched join")
enriched = orders_kv.join(users_kv)
for e in enriched.take(3):
    print(e)
print("enriched.count():", enriched.count(), "| orders_kv.count():", orders_kv.count())

sc.setJobDescription("enriched_left leftOuterJoin")
enriched_left = orders_kv.leftOuterJoin(users_kv)
print("leftOuterJoin count:", enriched_left.count(),
      "| with no user:", enriched_left.filter(lambda kv: kv[1][1] is None).count())

# Co-partitioning demo: same partitioner on both sides -> join without new shuffle
users_kv_p = users_kv.partitionBy(8).cache()
orders_kv_p = orders_kv.partitionBy(8).cache()
enriched_copart = orders_kv_p.join(users_kv_p)
print("--- lineage of a co-partitioned join (no extra shuffle above the partitionBy):")
print(enriched_copart.toDebugString().decode())

# ================================================================== EXERCISES
title("Exercise 1: aggregateByKey -> (order_count, total_quantity), average")
sc.setJobDescription("ex1 aggregateByKey")
user_stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),
        lambda acc, q: (acc[0] + 1, acc[1] + q),        # within a partition
        lambda a, b: (a[0] + b[0], a[1] + b[1]),        # across partitions
    )
)
user_avg_quantity = user_stats.mapValues(lambda cs: cs[1] / cs[0])
for k, v in user_stats.take(5):
    print(k, v)
for k, v in user_avg_quantity.take(5):
    print(k, round(v, 3))
print("totals consistent with reduceByKey:",
      sorted(user_stats.mapValues(lambda cs: cs[1]).collect()) == sorted(user_quantity.collect()))

title("Exercise 2: fake user_id + leftOuterJoin")
fake_order = {"order_id": "00000000-0000-0000-0000-000000000000",
              "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
              "date": "2026-10-12 00:00:00+00:00", "quantity": 1, "product": "ghost"}
orders_rdd_with_fake = orders_rdd.union(sc.parallelize([fake_order]))
sc.setJobDescription("ex2 leftOuterJoin with fake user")
enriched_fake = orders_rdd_with_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
orphans = enriched_fake.filter(lambda kv: kv[1][1] is None)
print("orders without matching user:", orphans.count())
print(orphans.take(1))

title("Exercise 3: same aggregation with the DataFrame API")
orders_df = (spark.read.csv("orders.csv", header=True, inferSchema=True)
             .withColumnRenamed("user_uuid", "user_id"))
user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
user_quantity_df.show(5, truncate=False)
user_quantity_df.explain()
print("--- RDD lineage:")
print(user_quantity.toDebugString().decode())

title("Exercise 4: timings with and without cache")
def timed(label, f):
    t0 = time.perf_counter(); r = f(); dt = time.perf_counter() - t0
    print(f"{label:<45} {dt*1000:8.1f} ms  -> {r}")
    return dt

# cached version (orders_rdd is already cached and materialized above)
sc.setJobDescription("ex4 cached")
timed("CACHED   orders_rdd.count()", orders_rdd.count)
timed("CACHED   user_quantity.count()", user_quantity.count)

# uncached copy, built with the same parsing but without .cache()
orders_rdd_nocache = orders_data.map(parse_order)
user_quantity_nocache = orders_rdd_nocache.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
sc.setJobDescription("ex4 no cache")
timed("NO CACHE orders_rdd_nocache.count()", orders_rdd_nocache.count)
timed("NO CACHE user_quantity_nocache.count()", user_quantity_nocache.count)
print("orders_rdd.is_cached:", orders_rdd.is_cached, "| storage level:", orders_rdd.getStorageLevel())
print("orders_rdd_nocache.is_cached:", orders_rdd_nocache.is_cached)
sc.setJobDescription(None)

title("DONE - session kept alive, Spark UI on http://localhost:4040 (Jobs / Stages / Storage)")
