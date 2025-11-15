CREATE EXTERNAL TABLE IF NOT EXISTS machine_learning_curated (
    user STRING,
    timeStamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE,
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT
)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/machine_learning/curated/'
TBLPROPERTIES ('has_encrypted_data'='false');
