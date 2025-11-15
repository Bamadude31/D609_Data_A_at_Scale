from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName("STEDI-Data-Quality").getOrCreate()

def validate_data_quality(df, table_name):
    """Comprehensive data quality checks"""
    print(f"\n{'='*60}")
    print(f"Data Quality Report: {table_name}")
    print('='*60)
    
    total_rows = df.count()
    print(f"Total Rows: {total_rows}")
    
    if total_rows == 0:
        print("❌ ERROR: No data found!")
        return False
    
    # Check null values per column
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    print("\nNull Value Counts:")
    for column, null_count in null_counts.items():
        null_percentage = (null_count / total_rows) * 100
        status = "⚠️" if null_percentage > 10 else "✓"
        print(f"  {status} {column}: {null_count} ({null_percentage:.2f}%)")
    
    return True

# Validate all zones
customer_trusted = spark.read.parquet("/home/glue_user/stedi-project/data/trusted/customer/")
ml_curated = spark.read.parquet("/home/glue_user/stedi-project/data/curated/machine_learning/")

validate_data_quality(customer_trusted, "Customer Trusted")
validate_data_quality(ml_curated, "ML Curated")

spark.stop()
