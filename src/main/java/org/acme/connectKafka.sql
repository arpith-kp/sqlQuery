create table tweetsTimelineJson ( created_at  AS PROCTIME(), id BIGINT, text STRING, ticker STRING) WITH
('connector.type' = 'kafka',
'connector.version' = 'universal',
'connector.topic'='tweetsTimelineJson',
'connector.startup-mode'='earliest-offset',
'connector.properties.0.key' = 'zookeeper.connect',
'connector.properties.0.value' = 'localhost:2181',
'connector.properties.1.key' = 'bootstrap.servers',
'connector.properties.1.value' = 'localhost:9092',
'update-mode' = 'append',
'format.type' = 'json',
'format.derive-schema' = 'true');

--CREATE TABLE stock_sink (
--    ticker VARCHAR,
--    tickerCount BIGINT
--) WITH (
--    'connector.type' = 'jdbc',
--    'connector.url' = 'jdbc:mysql://localhost:3306/stocks',
--    'connector.table' = 'stock_sink',
--    'connector.username' = 'root',
--    'connector.password' = 'Password123',
--    'connector.write.flush.max-rows' = '1'
--);


CREATE TABLE stock_sink_elasticsearch (
    ticker VARCHAR,
	tickerCount BIGINT
) WITH (
    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector
    'connector.version' = '6',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本
    'connector.hosts' = 'http://localhost:9200',  -- elasticsearch 地址
    'connector.index' = 'stocks',  -- elasticsearch 索引名，相当于数据库的表名
    'connector.document-type' = 'stock_count', -- elasticsearch 的 type，相当于数据库的库名
    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新
    'format.type' = 'json',  -- 输出数据格式 json
    'format.derive-schema' = 'true',
     'update-mode' = 'upsert'
);

--INSERT INTO stock_sink
--SELECT
--  ticker as ticker,
--  COUNT(*) AS tickerCount
--FROM tweetsTimelineJson
--GROUP BY ticker;

INSERT INTO stock_sink_elasticsearch
SELECT
  ticker as ticker,
  COUNT(*) AS tickerCount
FROM tweetsTimelineJson
GROUP BY  ticker;
