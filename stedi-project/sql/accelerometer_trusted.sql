CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_trusted (
    user STRING,
    timeStamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/accelerometer/trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');
