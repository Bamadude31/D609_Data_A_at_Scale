CREATE EXTERNAL TABLE IF NOT EXISTS customer_curated (
    customerName STRING,
    email STRING,
    phone STRING,
    birthDay STRING,
    serialNumber STRING,
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT
)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/customer/curated/'
TBLPROPERTIES ('has_encrypted_data'='false');
