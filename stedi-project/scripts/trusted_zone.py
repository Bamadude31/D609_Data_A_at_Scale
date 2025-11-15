from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("STEDI-Trusted-Zone") \
    .getOrCreate()

# Load landing zone data (reuse schemas from Step 3)
customer_landing = spark.read.json("/home/glue_user/stedi-project/data/landing/customer/")
accelerometer_landing = spark.read.json("/home/glue_user/stedi-project/data/landing/accelerometer/")
step_trainer_landing = spark.read.json("/home/glue_user/stedi-project/data/landing/step_trainer/")

# TRANSFORMATION 1: Customer Landing to Trusted
# Filter for customers who agreed to share data for research
customer_trusted = customer_landing.filter(
    col("shareWithResearchAsOfDate").isNotNull()
)

print(f"Customer Trusted Count: {customer_trusted.count()}")

# Save to parquet for better performance
customer_trusted.write.mode("overwrite").parquet("/data/trusted/customer/")

# TRANSFORMATION 2: Accelerometer Landing to Trusted
# Only include accelerometer readings from customers who consented
accelerometer_trusted = accelerometer_landing.join(
    customer_trusted.select("email"),
    accelerometer_landing.user == customer_trusted.email,
    "inner"
).select(
    accelerometer_landing["*"]
)

print(f"Accelerometer Trusted Count: {accelerometer_trusted.count()}")
accelerometer_trusted.write.mode("overwrite").parquet("/data/trusted/accelerometer/")

# TRANSFORMATION 3: Customer Curated (customers with accelerometer data)
# This creates a subset of customers who have both consented AND have accelerometer data
customer_curated = customer_trusted.join(
    accelerometer_landing.select("user").distinct(),
    customer_trusted.email == accelerometer_landing.user,
    "inner"
).select(customer_trusted["*"])

print(f"Customer Curated Count: {customer_curated.count()}")
customer_curated.write.mode("overwrite").parquet("/data/curated/customer/")

# TRANSFORMATION 4: Step Trainer Landing to Trusted
# Join with customer_curated to only include step trainer data from consented customers
# who also have accelerometer data
step_trainer_trusted = step_trainer_landing.join(
    customer_curated.select("serialNumber"),
    step_trainer_landing.serialNumber == customer_curated.serialNumber,
    "inner"
).select(step_trainer_landing["*"])

print(f"Step Trainer Trusted Count: {step_trainer_trusted.count()}")
step_trainer_trusted.write.mode("overwrite").parquet("/data/trusted/step_trainer/")

spark.stop()
