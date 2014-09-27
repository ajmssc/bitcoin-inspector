




































-- map external tables

CREATE EXTERNAL TABLE btceUSD ( timestamp int, value float, bitcoin_volume float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/bitcoin_historical/btceUSD/';

CREATE EXTERNAL TABLE lybitUSD ( timestamp int, value float, bitcoin_volume float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/bitcoin_historical/lybitUSD/';



CREATE TABLE hive_bitcoin_historical_prices(rowkey STRING, timestamp STRING, value STRING, bitcoin_volume STRING, bitcoin_exchange STRING);

INSERT INTO TABLE hive_bitcoin_historical_prices Select rowkey, timestamp, value, bitcoin_volume, bitcoin_exchange from 
(
    SELECT CONCAT('USD', timestamp) as rowkey, timestamp, value, bitcoin_volume, 'btceUSD' as bitcoin_exchange FROM btceUSD
    UNION ALL
    SELECT CONCAT('USD', timestamp) as rowkey, timestamp, value, bitcoin_volume, 'lybitUSD' as bitcoin_exchange FROM lybitUSD
) Combined ;


insert into table bitcoin_historical_prices select * from hive_bitcoin_historical_prices;












CREATE EXTERNAL TABLE bitcoin_historical_prices(rowkey STRING, timestamp STRING, value STRING, bitcoin_volume STRING, bitcoin_exchange STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,metadata:timestamp,metadata:value,metadata:volume,metadata:exchange')
TBLPROPERTIES ('hbase.table.name' = 'bitcoin_historical_prices');


INSERT INTO TABLE bitcoin_historical_prices Select rowkey, timestamp, value, bitcoin_volume, bitcoin_exchange from 
(
    SELECT CONCAT('USD', timestamp) as rowkey, timestamp, value, bitcoin_volume, 'btceUSD' as bitcoin_exchange FROM btceUSD
    UNION ALL
    SELECT CONCAT('USD', timestamp) as rowkey, timestamp, value, bitcoin_volume, 'lybitUSD' as bitcoin_exchange FROM lybitUSD
) Combined ;


where timestamp > 1365618226 and timestamp < 1370218958