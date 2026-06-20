from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

SOURCE_PATH = "/opt/spark-data/SMS_REF/source/"         
REFERENCE_PATH = "/opt/spark-data/SMS_REF/reference/"   
CHECKPOINT_BASE = "/opt/spark-data/_checkpoints/"       

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "output"

# تبدیل واحد: داده اصلی DEBIT_AMOUNT_42 بر حسب میلی‌ریال است.
# ۱ تومان = ۱۰ ریال = ۱۰,۰۰۰ میلی‌ریال  -->  تومان = میلی‌ریال / 10000
MILIRIAL_TO_TOMAN_DIVISOR = 10000

# ----------------------------------------------------------------------------
# 1) ساخت SparkSession با تنظیمات MinIO (S3A)
# ----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("RevenueAnalysisStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")  # برای دیتای کوچک/تست؛ روی پروداکشن افزایش بده
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------------
# 2) تعریف schema صریح برای داده سورس (برای استریم، schema باید مشخص باشد)
#    --- این لیست را دقیقاً مطابق هدر واقعی فایل خودت تنظیم کن ---
# ----------------------------------------------------------------------------
source_schema = StructType([
    StructField("ROAMSTATE_519", StringType(), True),
    StructField("CUST_LOCAL_START_DATE_15", StringType(), True),
    StructField("CDR_ID_1", StringType(), True),
    StructField("CDR_SUB_ID_2", StringType(), True),
    StructField("CDR_TYPE_3", StringType(), True),
    StructField("SPLIT_CDR_REASON_4", StringType(), True),
    StructField("RECORD_DATE", StringType(), True),          # بعدا به timestamp تبدیل می‌شود
    StructField("PAYTYPE_515", StringType(), True),
    StructField("DEBIT_AMOUNT_42", DoubleType(), True),
    StructField("SERVICEFLOW_498", StringType(), True),
    StructField("EVENTSOURCE_CATE_17", StringType(), True),
    StructField("USAGE_SERVICE_TYPE_19", StringType(), True),
    StructField("SPECIALNUMBERINDICATOR_534", StringType(), True),
    StructField("BE_ID_30", StringType(), True),
    StructField("CALLEDPARTYIMSI_495", StringType(), True),
    StructField("CALLINGPARTYIMSI_494", StringType(), True),
])

# ----------------------------------------------------------------------------
# 3) خواندن جدول رفرنس (batch - چون کوچک و ثابت است، نیازی به استریم ندارد)
#    فرض: فایل رفرنس دو ستون دارد: PayType (نام) و value (کد عددی)
#    مثال:  PayType   value
#           Prepaid   0
#           Postpaid  1
# ----------------------------------------------------------------------------
reference_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "\t")   
    .csv(REFERENCE_PATH)
)

reference_df = reference_df.withColumnRenamed("PayType", "paytype_name") \
                            .withColumnRenamed("value", "paytype_code")
reference_df = reference_df.withColumn(
    "paytype_code", F.col("paytype_code").cast(StringType())
)
# جدول رفرنس کوچک است؛ broadcast برای join سریع‌تر در هر batch
reference_df = F.broadcast(reference_df)

# ----------------------------------------------------------------------------
# 4) خواندن داده سورس به صورت Structured Streaming
#    تریگر "once": یک‌بار همه فایل‌های موجود را می‌خواند و سپس متوقف می‌شود
# ----------------------------------------------------------------------------
raw_stream_df = (
    spark.readStream
    .option("header", "true")
    .option("sep", ",")         
    .schema(source_schema)
    .csv(SOURCE_PATH)
)

BUCKET_URI = f"s3a://{MINIO_BUCKET}"


# ----------------------------------------------------------------------------
# 5) تابع پردازش هر micro-batch
#    تمام منطق گزارش‌ها اینجا، روی DataFrame استاتیک batch_df انجام می‌شود.
# ----------------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] داده‌ای برای پردازش وجود ندارد.")
        return

    # --- آماده‌سازی پایه ---
    prepared_df = (
        batch_df
        .withColumn("event_time", F.to_timestamp("RECORD_DATE", "yyyy/MM/dd HH:mm:ss"))
        .withColumn("revenue_toman", F.col("DEBIT_AMOUNT_42") / F.lit(MILIRIAL_TO_TOMAN_DIVISOR))
        .withColumn(
            "bucket_15min",
            F.from_unixtime(
                (F.unix_timestamp("event_time") / 900).cast("long") * 900
            ).cast(TimestampType())
        )
        .withColumn("event_date", F.to_date("event_time"))
        .cache()
    )

    # ========================================================================
    # گزارش ۱: درآمد روزانه به تومان
    # ========================================================================
    report1_df = (
        prepared_df
        .groupBy(F.col("event_date").alias("RECORD_DATE"))
        .agg(F.sum("revenue_toman").alias("revenue"))
        .orderBy("RECORD_DATE")
    )

    # ========================================================================
    # گزارش ۲: درآمد با ریزدانگی ۱۵ دقیقه به ازای هر paytype (کد خام)
    # ========================================================================
    report2_df = (
        prepared_df
        .groupBy("bucket_15min", "PAYTYPE_515")
        .agg(F.sum("revenue_toman").alias("revenue"))
        .select(
            F.date_format("bucket_15min", "HH:mm:ss").alias("RECORD_TIME"),
            F.date_format("bucket_15min", "yyyy/MM/dd").alias("RECORD_DATE"),
            F.col("PAYTYPE_515").alias("Pay_type"),
            "revenue"
        )
        .orderBy("RECORD_DATE", "RECORD_TIME", "Pay_type")
    )

    # ========================================================================
    # گزارش ۳: حداکثر و حداقل درآمد (۱۵ دقیقه‌ای) به ازای هر paytype
    # ========================================================================
    report3_df = (
        prepared_df
        .groupBy(
            F.window("event_time", "15 minutes"),
            "PAYTYPE_515"
        )
        .agg(
            F.sum("revenue_toman").alias("total_revenue"),
            F.max("revenue_toman").alias("max_revenue"),
            F.min("revenue_toman").alias("min_revenue")
        )
        .select(
            F.date_format("window.start", "HH:mm:ss").alias("RECORD_TIME"),
            F.date_format("window.start", "yyyy/MM/dd").alias("RECORD_DATE"),
            F.col("PAYTYPE_515").alias("Pay_type"),
            "max_revenue",
            "min_revenue"
        )
    )
    # ========================================================================
    # گزارش ۴: درآمد + تعداد رکورد با ریزدانگی ۱۵ دقیقه به ازای هر paytype
    #    با نام‌گذاری معنی‌دار paytype از طریق join با جدول رفرنس
    # ========================================================================
    report4_raw_df = (
        prepared_df
        .groupBy("bucket_15min", "PAYTYPE_515")
        .agg(
            F.count("*").alias("Record_Count"),
            F.sum("revenue_toman").alias("revenue")
        )
    )

    report4_df = (
        report4_raw_df
        .join(
            reference_df,
            report4_raw_df["PAYTYPE_515"] == reference_df["paytype_code"],
            how="left"
        )
        .select(
            F.date_format("bucket_15min", "HH:mm:ss").alias("RECORD_TIME"),
            F.date_format("bucket_15min", "yyyy/MM/dd").alias("RECORD_DATE"),
            F.coalesce(F.col("paytype_name"), F.col("PAYTYPE_515")).alias("Pay_type"),
            "Record_Count",
            "revenue"
        )
        .orderBy("RECORD_DATE", "RECORD_TIME", "Pay_type")
    )

    # --- نوشتن هر گزارش در MinIO ---
    (report1_df.coalesce(1).write.mode("overwrite")
        .option("header", "true").csv(f"{BUCKET_URI}/report1_daily_revenue"))

    (report2_df.coalesce(1).write.mode("overwrite")
        .option("header", "true").csv(f"{BUCKET_URI}/report2_revenue_15min_paytype"))

    (report3_df.coalesce(1).write.mode("overwrite")
        .option("header", "true").csv(f"{BUCKET_URI}/report3_max_min_revenue_15min_paytype"))

    (report4_df.coalesce(1).write.mode("overwrite")
        .option("header", "true").csv(f"{BUCKET_URI}/report4_revenue_count_15min_paytype_named"))

    prepared_df.unpersist()
    print(f"[batch {batch_id}] هر چهار گزارش با موفقیت نوشته شدند.")


# ----------------------------------------------------------------------------
# 6) اجرای استریم با تریگر once (یک‌بار اجرا و توقف)
# ----------------------------------------------------------------------------
query = (
    raw_stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_BASE + "revenue_analysis")
    .trigger(once=True)   # نسخه‌های جدیدتر Spark: trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print("=" * 60)
print("هر چهار گزارش با موفقیت در MinIO ذخیره شدند.")
print(f"Bucket: {MINIO_BUCKET}")
print("=" * 60)

spark.stop()