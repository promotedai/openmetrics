CREATE TABLE cumulated_content_metrics_file (
    dt STRING NOT NULL,
    `header` ROW<
        `version` STRING NOT NULL,
        `sourceId` STRING NOT NULL,
        -- De-duplicates records by `id`.
        `id` STRING NOT NULL,
        `correlationId` STRING NOT NULL,
        `platformId` BIGINT NOT NULL,
        -- TODO - can this be an enum?
        `messageType` STRING NOT NULL,
        `eventDateTimestamp` TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
    > NOT NULL,
    `body` ROW<
        `contentId` STRING NOT NULL,
        `metrics` ROW(
            `views` BIGINT NOT NULL,
            `impressions` BIGINT NOT NULL,
            `joinedImpressions` BIGINT NOT NULL,
            `joinedImpressionPositionSum` BIGINT NOT NULL,
            `navigates` BIGINT NOT NULL,
            `addToCarts` BIGINT NOT NULL,
            `checkouts` BIGINT NOT NULL,
            `purchases` BIGINT NOT NULL,
            `gmvUsd` BIGINT NOT NULL
        )
    > NOT NULL
) PARTITIONED BY (dt) WITH (
    'connector' = 'filesystem',
    'path' = '{rootPath}etl/cumulated_content_metrics',
    'format' = 'json',
    'sink.partition-commit.policy.kind' = 'success-file',
    'sink.parallelism' = '{sinkParallelism}',
    'json.timestamp-format.standard' = 'ISO-8601'
    -- TODO - json.encode.decimal-as-plain-number
)