CREATE TABLE hourly_content_metrics (
    dt STRING NOT NULL,
    `hour` STRING NOT NULL,
    platform_id BIGINT NOT NULL,
    content_id STRING NOT NULL,
    view_count BIGINT,
    impression_count BIGINT,
    joined_impression_count BIGINT,
    joined_impression_position_sum BIGINT,
    navigate_count BIGINT,
    add_to_cart_count BIGINT,
    checkout_count BIGINT,
    purchase_count BIGINT,
    gmv_usd_micros BIGINT
) PARTITIONED BY (dt, `hour`) WITH (
    'connector' = 'filesystem',
    'path' = '{rootPath}etl/hourly_content_metrics',
    'format' = 'parquet',
    'sink.partition-commit.policy.kind' = 'success-file',
    'sink.parallelism' = '{sinkParallelism}'
)
