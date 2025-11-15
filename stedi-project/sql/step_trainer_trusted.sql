CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_trusted (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT
)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/step_trainer/trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');
