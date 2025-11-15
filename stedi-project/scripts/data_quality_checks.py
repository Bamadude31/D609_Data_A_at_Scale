from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName("STEDI-Data-Quality").getOrCreate()

print("=" * 80)
print("STEDI DATA QUALITY VALIDATION REPORT")
print("=" * 80)

# Load all tables
customer_landing = spark.read.json("/tmp/stedi-data/landing/customer/")
customer_trusted = spark.read.parquet("/tmp/stedi-data/trusted/customer/")
customer_curated = spark.read.parquet("/tmp/stedi-data/curated/customer/")
ml_curated = spark.read.parquet("/tmp/stedi-data/curated/machine_learning/")

# Test 1: Privacy Filter Validation
print("\n[TEST 1] Privacy Filter Validation")
print("-" * 80)
landing_total = customer_landing.count()
landing_with_consent = customer_landing.filter(col("shareWithResearchAsOfDate").isNotNull()).count()
landing_without_consent = customer_landing.filter(col("shareWithResearchAsOfDate").isNull()).count()
trusted_total = customer_trusted.count()
trusted_with_null = customer_trusted.filter(col("shareWithResearchAsOfDate").isNull()).count()

print(f"Customer Landing Total: {landing_total}")
print(f"  - With Consent: {landing_with_consent}")
print(f"  - Without Consent: {landing_without_consent}")
print(f"\nCustomer Trusted Total: {trusted_total}")
print(f"  - With NULL Consent: {trusted_with_null}")

if trusted_with_null == 0 and trusted_total == landing_with_consent:
    print("✅ PASS: Privacy filter correctly applied")
else:
    print("❌ FAIL: Privacy filter has issues")

# Test 2: Data Lineage
print("\n[TEST 2] Data Lineage Validation")
print("-" * 80)
print(f"Landing → Trusted: {landing_total} → {trusted_total} (filtered {landing_without_consent} unconsented)")
print(f"Trusted → Curated: {trusted_total} → {customer_curated.count()}")
print(f"ML Curated Records: {ml_curated.count()}")

# Test 3: No Null Values in Critical Fields
print("\n[TEST 3] Critical Field Validation")
print("-" * 80)
ml_nulls = ml_curated.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in ['user', 'timeStamp', 'sensorReadingTime', 'serialNumber']
]).collect()[0].asDict()

for field, null_count in ml_nulls.items():
    status = "✅ PASS" if null_count == 0 else "⚠️ WARNING"
    print(f"{status}: {field} has {null_count} null values")

print("\n" + "=" * 80)
print("DATA QUALITY REPORT COMPLETE")
print("=" * 80)

spark.stop()
