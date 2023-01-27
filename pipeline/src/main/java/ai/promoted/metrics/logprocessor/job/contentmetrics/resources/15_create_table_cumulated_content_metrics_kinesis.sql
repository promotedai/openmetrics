CREATE TABLE cumulated_content_metrics_kinesis (
    dt STRING NOT NULL,
    -- contentId needs to be a top-level field for Flink's default Kinesis connector.
    `contentId` STRING NOT NULL,
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
) PARTITIONED BY (contentId) WITH (
    'connector' = 'kinesis',
    'format' = 'json',
    'stream' = '{kinesisStream}',
    'aws.region' = '{region}',
    'json.timestamp-format.standard' = 'ISO-8601',
    'sink.producer.AggregationEnabled' = 'false'
)
