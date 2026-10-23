-- Print streaming results when running this file with sql-client.sh -f.
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'clicks-per-minute';

-- Buffer records to reduce aggregation overhead (these do not configure slots).
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';

CREATE TABLE youtube_inbound_stream (
    videoid STRING,
    `timestamp` STRING,
    click_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR click_time AS click_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'youtube-clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-analysis-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    videoid,
    TUMBLE_START(click_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS click_count
FROM youtube_inbound_stream
GROUP BY
    videoid,
    TUMBLE(click_time, INTERVAL '1' MINUTE);
