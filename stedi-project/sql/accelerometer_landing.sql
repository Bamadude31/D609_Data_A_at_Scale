CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
    user STRING,
    timeStamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://your-bucket-name/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
