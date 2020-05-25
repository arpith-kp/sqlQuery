CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列
    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列
) WITH (
    'connector.type' = 'kafka',  -- 使用 kafka connector
    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本
    'connector.topic' = 'user_behavior',  -- kafka topic
    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取
    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址
    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址
    'format.type' = 'json',  -- 数据源格式为 json
    'format.derive-schema' = 'true'
);

CREATE TABLE buy_cnt_per_hour (
    hour_of_day BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector
    'connector.version' = '6',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本
    'connector.hosts' = 'http://localhost:9200',  -- elasticsearch 地址
    'connector.index' = 'buy_cnt_per_hour',  -- elasticsearch 索引名，相当于数据库的表名
    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名
    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新
    'format.type' = 'json',  -- 输出数据格式 json,
    'format.derive-schema' = 'true',
    'update-mode' = 'upsert'
);

INSERT INTO buy_cnt_per_hour
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
