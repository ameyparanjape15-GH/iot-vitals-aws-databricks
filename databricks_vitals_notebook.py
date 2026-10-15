# Databricks notebook source
# MAGIC %md
# MAGIC # IoT vitals: S3 -> Bronze / Silver / Gold (Databricks Free Edition)
# MAGIC **Learning project with synthetic data - not a medical system.**
# MAGIC
# MAGIC Flow: simulator -> AWS IoT Core -> (rule 1) SNS email alert, (rule 2) Firehose -> S3 JSON files -> **this notebook**.
# MAGIC
# MAGIC | Layer | Table | Purpose |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_vitals` | raw readings exactly as landed, plus source file |
# MAGIC | Silver | `silver_vitals` | typed, de-duplicated, range-checked, with event time |
# MAGIC | Gold | `gold_alerts`, `gold_trend_warnings`, `gold_hourly_summary` | business views |
# MAGIC
# MAGIC Run the cells top to bottom. Run it again after more data arrives (it rebuilds the tables).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Settings
# MAGIC Use a **read-only IAM user** limited to your bucket for the keys below, and delete that user when you finish.
# MAGIC Free Edition has no way to attach an IAM role, so access keys are the simplest route for a short learning exercise.
# MAGIC Never paste keys into notebook code that you save or share.

# COMMAND ----------

dbutils.widgets.text("bucket", "", "S3 bucket (vitals-demo-<account>-<region>)")
dbutils.widgets.text("region", "eu-west-2", "AWS region")
dbutils.widgets.text("aws_access_key_id", "", "Read-only access key id")
dbutils.widgets.text("aws_secret_access_key", "", "Read-only secret key")

BUCKET = dbutils.widgets.get("bucket").strip()
REGION = dbutils.widgets.get("region").strip()

CATALOG, SCHEMA, VOLUME = "workspace", "iot_vitals", "landing"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

# Alert thresholds - identical to the AWS IoT rule so the two can be compared
SPO2_LOW = 85
SYSTOLIC_HIGH = 180

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print("Landing folder:", LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bring the files from S3 into a Unity Catalog Volume
# MAGIC **Plan A** (below): download with boto3. Free Edition only allows outbound traffic to a list of trusted domains, so first check
# MAGIC that S3 is reachable.
# MAGIC
# MAGIC **Plan B** if Plan A reports a network error: on your laptop run
# MAGIC `aws s3 sync s3://<bucket>/bronze/vitals ./vitals` and upload the folder through
# MAGIC *Catalog > workspace > iot_vitals > landing > Upload to this volume*, keeping the
# MAGIC `bronze/vitals/dt=.../` folder structure. Then continue from section 2.

# COMMAND ----------

import os
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

s3 = boto3.client(
    "s3",
    region_name=REGION,
    aws_access_key_id=dbutils.widgets.get("aws_access_key_id").strip(),
    aws_secret_access_key=dbutils.widgets.get("aws_secret_access_key").strip(),
)

try:
    s3.head_bucket(Bucket=BUCKET)
    print("S3 reachable - using Plan A")
    new_files = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="bronze/vitals/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            dest = f"{LANDING}/{key}"
            if os.path.exists(dest):
                continue
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            s3.download_file(BUCKET, key, dest)
            new_files += 1
    print(f"Downloaded {new_files} new file(s) into {LANDING}/bronze/vitals/")
except Exception as e:  # noqa: BLE001
    print("Plan A failed:", type(e).__name__, str(e)[:300])
    print("Use Plan B (upload the files to the Volume), then continue.")

# COMMAND ----------

display(dbutils.fs.ls(f"{LANDING}/bronze/vitals"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze - raw readings, nothing dropped

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

schema = StructType([
    StructField("deviceId", StringType()),
    StructField("ts", LongType()),          # epoch milliseconds from the device
    StructField("spo2", DoubleType()),
    StructField("systolic", LongType()),
    StructField("diastolic", LongType()),
    StructField("heartRate", LongType()),
    StructField("ingestTs", LongType()),    # added by the IoT rule (AWS-side time)
])

bronze = (
    spark.read.schema(schema)
    .option("recursiveFileLookup", "true")
    .json(f"{LANDING}/bronze/vitals")
    .select("*", F.col("_metadata.file_path").alias("source_file"))
)
bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("bronze_vitals"))
print("bronze rows:", spark.table(T("bronze_vitals")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver - clean, typed, de-duplicated
# MAGIC * `event_time` from the device clock, `ingest_time` from AWS, `latency_s` between them.
# MAGIC * IoT delivery is "at least once", so duplicates are possible: drop repeats of (device, ts).
# MAGIC * Physically implausible values are kept but marked `is_valid = false` so a sensor fault is visible, not silently lost.

# COMMAND ----------

silver = (
    spark.table(T("bronze_vitals"))
    .where(F.col("deviceId").isNotNull() & F.col("ts").isNotNull())
    .dropDuplicates(["deviceId", "ts"])
    .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp"))
    .withColumn("ingest_time", (F.col("ingestTs") / 1000).cast("timestamp"))
    .withColumn("latency_s", (F.col("ingestTs") - F.col("ts")) / 1000)
    .withColumn(
        "is_valid",
        F.col("spo2").between(50, 100)
        & F.col("systolic").between(60, 260)
        & F.col("diastolic").between(30, 160)
        & (F.col("systolic") > F.col("diastolic"))
        & F.col("heartRate").between(25, 220),
    )
    .select("deviceId", "event_time", "ingest_time", "latency_s",
            "spo2", "systolic", "diastolic", "heartRate", "is_valid", "source_file")
)
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("silver_vitals"))
print("silver rows:", spark.table(T("silver_vitals")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold
# MAGIC **a) `gold_alerts`** - the same rule AWS applied in real time. Its row count should equal the number of alert emails you received.
# MAGIC
# MAGIC **b) `gold_trend_warnings`** - something the single-reading IoT rule cannot do: a 30-second moving average (6 readings at 5 s).
# MAGIC Catches a slow slide before the hard limit is crossed.
# MAGIC
# MAGIC **c) `gold_hourly_summary`** - per device per hour statistics for a dashboard.

# COMMAND ----------

sv = spark.table(T("silver_vitals")).where("is_valid")

alerts = (
    sv.where((F.col("spo2") < SPO2_LOW) | (F.col("systolic") >= SYSTOLIC_HIGH))
    .withColumn(
        "alert_type",
        F.when((F.col("spo2") < SPO2_LOW) & (F.col("systolic") >= SYSTOLIC_HIGH), "LOW_SPO2_AND_HIGH_BP")
        .when(F.col("spo2") < SPO2_LOW, "LOW_SPO2")
        .otherwise("HIGH_BP"),
    )
)
alerts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_alerts"))

w6 = Window.partitionBy("deviceId").orderBy("event_time").rowsBetween(-5, 0)
trend = (
    sv.withColumn("n6", F.count(F.lit(1)).over(w6))
    .withColumn("spo2_avg_30s", F.avg("spo2").over(w6))
    .withColumn("systolic_avg_30s", F.avg("systolic").over(w6))
    .where((F.col("n6") == 6) & ((F.col("spo2_avg_30s") < 90) | (F.col("systolic_avg_30s") >= 160)))
    .withColumn(
        "warning_type",
        F.when(F.col("spo2_avg_30s") < 90, "SUSTAINED_LOW_SPO2").otherwise("SUSTAINED_HIGH_BP"),
    )
    .select("deviceId", "event_time", "spo2", "systolic", "spo2_avg_30s", "systolic_avg_30s", "warning_type")
)
trend.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_trend_warnings"))

hourly = (
    sv.groupBy("deviceId", F.date_trunc("hour", "event_time").alias("hour"))
    .agg(
        F.count("*").alias("readings"),
        F.round(F.avg("spo2"), 1).alias("avg_spo2"),
        F.min("spo2").alias("min_spo2"),
        F.max("systolic").alias("max_systolic"),
        F.sum(F.when((F.col("spo2") < SPO2_LOW) | (F.col("systolic") >= SYSTOLIC_HIGH), 1).otherwise(0)).alias("alert_readings"),
        F.round(F.avg("latency_s"), 2).alias("avg_latency_s"),
    )
)
hourly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_hourly_summary"))
print("gold tables written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate (do not skip - this is the interview talking point)

# COMMAND ----------

b = spark.table(T("bronze_vitals")).count()
s = spark.table(T("silver_vitals")).count()
a = spark.table(T("gold_alerts")).count()
print(f"bronze={b}  silver={s}  (duplicates removed: {b - s})  alert readings={a}")
print("Compare alert readings with the number of alert emails in your inbox - they should match.")
print("Expected volume: 12 readings per minute per device, 720 per hour.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT alert_type, COUNT(*) AS readings, MIN(event_time) AS first_seen, MAX(event_time) AS last_seen
# MAGIC FROM workspace.iot_vitals.gold_alerts
# MAGIC GROUP BY alert_type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.iot_vitals.gold_hourly_summary ORDER BY hour DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pipeline health: how long does a reading take from device to AWS?
# MAGIC SELECT ROUND(AVG(latency_s), 2) AS avg_s, ROUND(PERCENTILE(latency_s, 0.95), 2) AS p95_s, MAX(latency_s) AS max_s
# MAGIC FROM workspace.iot_vitals.silver_vitals

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clean up
# MAGIC Uncomment to remove the tables and downloaded files, then run `python setup_aws.py teardown` on your laptop and delete the read-only IAM user.

# COMMAND ----------

# for t in ["bronze_vitals", "silver_vitals", "gold_alerts", "gold_trend_warnings", "gold_hourly_summary"]:
#     spark.sql(f"DROP TABLE IF EXISTS {T(t)}")
# dbutils.fs.rm(f"{LANDING}/bronze", recurse=True)
