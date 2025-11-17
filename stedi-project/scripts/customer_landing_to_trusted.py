import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

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

# Convert back to DynamicFrame and write to S3 as Parquet
customer_trusted_dyf = DynamicFrame.fromDF(
    customer_trusted, 
    glueContext, 
    "customer_trusted_dyf"
)

glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-lakehouse-william-aldridge/customer/trusted/"
    },
    format="parquet"
)

job.commit()
