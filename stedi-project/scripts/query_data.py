from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("STEDI-Analytics") \
    .enableHiveSupport() \
    .getOrCreate()

# Load curated data
ml_data = spark.read.parquet("/data/curated/machine_learning/")
ml_data.createOrReplaceTempView("ml_data")

# Example analytical queries
print("=== Query 1: Total sensor readings by user ===")
spark.sql("""
    SELECT user, COUNT(*) as total_readings
    FROM ml_data
    GROUP BY user
    ORDER BY total_readings DESC
""").show()

print("\n=== Query 2: Average accelerometer values ===")
spark.sql("""
    SELECT 
        user,
        AVG(x) as avg_x,
        AVG(y) as avg_y,
        AVG(z) as avg_z,
        AVG(distanceFromObject) as avg_distance
    FROM ml_data
    GROUP BY user
""").show()

print("\n=== Query 3: Data quality check - Null values ===")
spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN x IS NULL THEN 1 ELSE 0 END) as null_x,
        SUM(CASE WHEN y IS NULL THEN 1 ELSE 0 END) as null_y,
        SUM(CASE WHEN z IS NULL THEN 1 ELSE 0 END) as null_z
    FROM ml_data
""").show()

spark.stop()
