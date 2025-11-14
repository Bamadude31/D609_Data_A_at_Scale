from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("STEDI-Curated-Zone") \
    .getOrCreate()

# Load trusted zone data
accelerometer_trusted = spark.read.parquet("/data/trusted/accelerometer/")
step_trainer_trusted = spark.read.parquet("/data/trusted/step_trainer/")

# Create the machine learning curated dataset
# Join accelerometer and step trainer data on matching timestamps
machine_learning_curated = step_trainer_trusted.join(
    accelerometer_trusted,
    step_trainer_trusted.sensorReadingTime == accelerometer_trusted.timeStamp,
    "inner"
).select(
    col("user"),
    col("timeStamp"),
    col("sensorReadingTime"),
    col("serialNumber"),
    col("distanceFromObject"),
    col("x"),
    col("y"),
    col("z")
)

print(f"Machine Learning Curated Count: {machine_learning_curated.count()}")

# Show sample of ML-ready data
machine_learning_curated.show(10, truncate=False)

# Save as parquet for ML consumption
machine_learning_curated.write.mode("overwrite").parquet(
    "/data/curated/machine_learning/"
)

# Also save as CSV for easy inspection
machine_learning_curated.write.mode("overwrite") \
    .option("header", "true") \
    .csv("/data/curated/machine_learning_csv/")

print("\n✓ Machine Learning dataset created successfully!")

spark.stop()
