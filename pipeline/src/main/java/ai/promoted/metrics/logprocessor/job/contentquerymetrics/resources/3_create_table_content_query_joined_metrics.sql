CREATE TABLE {periodLabel}_content_query_joined_metrics (
    dt STRING NOT NULL,
    platform_id BIGINT NOT NULL,
    content_id STRING NOT NULL,
    search_query STRING NOT NULL,
    impression_count BIGINT,
    impression_position_sum BIGINT,
    navigate_count BIGINT,
    add_to_cart_count BIGINT,
    checkout_count BIGINT,
    purchase_count BIGINT,
    gmv_usd_micros BIGINT
) PARTITIONED BY (dt) WITH (
    'connector' = 'filesystem',
    'path' = '{rootPath}etl/{periodLabel}_content_query_joined_metrics',
    'format' = 'parquet',
    'sink.partition-commit.policy.kind' = 'success-file',
    'sink.parallelism' = '{sinkParallelism}'
)