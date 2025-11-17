import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
).toDF()

accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing"
).toDF()

# Join to filter accelerometer data for consented customers only
accelerometer_trusted = accelerometer_landing.join(
    customer_trusted.select("email"),
    accelerometer_landing.user == customer_trusted.email,
    "inner"
).select(accelerometer_landing["*"])

# Write to S3
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
