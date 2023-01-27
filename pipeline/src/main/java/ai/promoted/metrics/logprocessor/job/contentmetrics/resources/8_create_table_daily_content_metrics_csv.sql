CREATE TABLE daily_content_metrics_csv (
    dt STRING NOT NULL,
    -- We're writing out csv so a customer can use Stitch data.  Stitch doesn't support pulling fields from Hive partitions.
    `date` STRING NOT NULL,
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
) PARTITIONED BY (dt) WITH (
    'connector' = 'suffixedfilesystem',
    'path' = '{rootPath}etl/daily_content_metrics_csv',
    'format' = 'headered-csv',
    'suffix' = '.csv',
    'sink.partition-commit.policy.kind' = 'success-file',
    'sink.parallelism' = '{sinkParallelism}'
)