import pandas as pd

src = pd.read_csv("/home/claude/spark-minio-project/data/SMS_REF/source/REF_CBS_SMS2.csv")
ref = pd.read_csv("/home/claude/spark-minio-project/data/SMS_REF/reference/paytype_ref.csv", sep="\t")
ref = ref.rename(columns={"PayType": "paytype_name", "value": "paytype_code"})

src["event_time"] = pd.to_datetime(src["RECORD_DATE"], format="%Y/%m/%d %H:%M:%S")
src["revenue_toman"] = src["DEBIT_AMOUNT_42"] / 10000.0

# گرد کردن به ۱۵ دقیقه (floor)
src["bucket_15min"] = src["event_time"].dt.floor("15min")
src["event_date"] = src["event_time"].dt.date

print("=== گزارش ۱: درآمد روزانه ===")
r1 = src.groupby("event_date")["revenue_toman"].sum().reset_index()
print(r1)

print("\n=== گزارش ۲: درآمد ۱۵ دقیقه‌ای به ازای paytype خام ===")
r2 = src.groupby(["bucket_15min", "PAYTYPE_515"])["revenue_toman"].sum().reset_index()
print(r2)

print("\n=== گزارش ۳: max/min درآمد ۱۵دقیقه‌ای به ازای paytype ===")
r3 = r2.groupby("PAYTYPE_515")["revenue_toman"].agg(["max", "min"]).reset_index()
print(r3)

print("\n=== گزارش ۴: تعداد رکورد + درآمد + اسم paytype (join با رفرنس) ===")
r4 = src.groupby(["bucket_15min", "PAYTYPE_515"]).agg(
    Record_Count=("RECORD_DATE", "count"),
    revenue=("revenue_toman", "sum")
).reset_index()
r4 = r4.merge(ref, left_on="PAYTYPE_515", right_on="paytype_code", how="left")
r4 = r4[["bucket_15min", "paytype_name", "Record_Count", "revenue"]]
print(r4)

print("\n--- بررسی دستی نمونه ساعت 12:00:00 2021/06/22 ---")
print("paytype=0 باید: 25000+15000=40000 تومان, 2 رکورد")
print("paytype=1 باید: 15000 تومان, 1 رکورد")
print("(چون 250000000 و 150000000 میلی‌ریال تقسیم بر 10000 = 25000 و 15000 تومان)")
