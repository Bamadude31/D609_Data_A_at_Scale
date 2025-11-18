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

# Load Step Trainer Landing data from Glue Data Catalog
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
).toDF()

# Load Customer Curated from Glue Data Catalog
customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
).toDF()

# Join step trainer data with only customers in curated (have accelerometer data and consent)
step_trainer_trusted = step_trainer_landing.join(
    customer_curated.select("serialNumber"),
    step_trainer_landing.serialNumber == customer_curated.serialNumber,
    "inner"
).select(step_trainer_landing["*"])

print(f"Step Trainer Landing Count: {step_trainer_landing.count()}")
print(f"Step Trainer Trusted Count: {step_trainer_trusted.count()}")

# Convert back to DynamicFrame for Glue Writing
step_trainer_trusted_dyf = DynamicFrame.fromDF(
    step_trainer_trusted,
    glueContext,
    "step_trainer_trusted_dyf"
)

glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-lakehouse-william-aldridge/step_trainer/trusted/"
    },
    format="parquet"
)

job.commit()
