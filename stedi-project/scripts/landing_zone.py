from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("STEDI-Landing-Zone") \
    .getOrCreate()

# Define schemas for each data source
customer_schema = StructType([
    StructField("customerName", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("birthDay", StringType(), True),
    StructField("serialNumber", StringType(), True),
    StructField("registrationDate", LongType(), True),
    StructField("lastUpdateDate", LongType(), True),
    StructField("shareWithResearchAsOfDate", LongType(), True),
    StructField("shareWithPublicAsOfDate", LongType(), True)
])

accelerometer_schema = StructType([
    StructField("user", StringType(), True),
    StructField("timeStamp", LongType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", DoubleType(), True)
])

step_trainer_schema = StructType([
    StructField("sensorReadingTime", LongType(), True),
    StructField("serialNumber", StringType(), True),
    StructField("distanceFromObject", LongType(), True)
])

# Read JSON files into DataFrames
customer_landing = spark.read.json(
    "/home/glue_user/stedi-project/data/landing/customer/",
    schema=customer_schema,
    multiLine=False
)

accelerometer_landing = spark.read.json(
    "/home/glue_user/stedi-project/data/landing/accelerometer/",
    schema=accelerometer_schema,
    multiLine=False
)

step_trainer_landing = spark.read.json(
    "/home/glue_user/stedi-project/data/landing/step_trainer/",
    schema=step_trainer_schema,
    multiLine=False
)

# Create temporary views for SQL querying
customer_landing.createOrReplaceTempView("customer_landing")
accelerometer_landing.createOrReplaceTempView("accelerometer_landing")
step_trainer_landing.createOrReplaceTempView("step_trainer_landing")

# Data quality check: Count records
print(f"Customer Landing Count: {customer_landing.count()}")
print(f"Accelerometer Landing Count: {accelerometer_landing.count()}")
print(f"Step Trainer Landing Count: {step_trainer_landing.count()}")

# Show sample data
print("\n--- Customer Landing Sample ---")
customer_landing.show(5, truncate=False)

print("\n--- Accelerometer Landing Sample ---")
accelerometer_landing.show(5, truncate=False)

print("\n--- Step Trainer Landing Sample ---")
step_trainer_landing.show(5, truncate=False)

spark.stop()
