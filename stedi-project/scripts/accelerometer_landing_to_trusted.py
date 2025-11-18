import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load customer trusted from Data Catalog
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
).toDF()

# Load accelerometer landing from Data Catalog
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing"
).toDF()

# Only include accelerometer readings from customers who consented
accelerometer_trusted = accelerometer_landing.join(
    customer_trusted.select("email"),
    accelerometer_landing.user == customer_trusted.email,
    "inner"
).select(accelerometer_landing["*"])

print(f"Accelerometer Landing Count: {accelerometer_landing.count()}")
print(f"Accelerometer Trusted Count: {accelerometer_trusted.count()}")

# Convert back to DynamicFrame and write to S3 as Parquet
accelerometer_trusted_dyf = DynamicFrame.fromDF(
    accelerometer_trusted, 
    glueContext, 
    "accelerometer_trusted_dyf"
)

glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-lakehouse-william-aldridge/accelerometer/trusted/"
    },
    format="parquet"
)

job.commit()
