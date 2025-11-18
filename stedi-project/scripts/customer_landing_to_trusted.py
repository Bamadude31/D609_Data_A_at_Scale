import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame  # Required for converting DF -> DynamicFrame

# Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load customer landing data from Glue Data Catalog
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing"
).toDF()

# CRITICAL PRIVACY FILTER: Only customers who consented
customer_trusted = customer_landing.filter(
    col("shareWithResearchAsOfDate").isNotNull()
)

print(f"Customer Landing Count: {customer_landing.count()}")
print(f"Customer Trusted Count: {customer_trusted.count()}")

# Convert back to DynamicFrame for Glue output
customer_trusted_dyf = DynamicFrame.fromDF(
    customer_trusted,
    glueContext,
    "customer_trusted_dyf"
)

# Write to the Trusted zone in S3 as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-lakehouse-william-aldridge/customer/trusted/"
    },
    format="parquet"
)

job.commit()
