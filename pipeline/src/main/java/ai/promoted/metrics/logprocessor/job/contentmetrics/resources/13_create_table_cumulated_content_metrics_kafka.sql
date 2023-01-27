CREATE TABLE cumulated_content_metrics_kafka (
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
) WITH (
    'connector' = 'kafka',
    'format' = 'json',
    'topic' = '{cumulatedKafkaTopic}',
    'properties.bootstrap.servers' = '{kafkaBootstrapServers}',
    'properties.group.id' = '{cumulatedKafkaGroupId}',
    'scan.startup.mode' = 'earliest-offset',
    -- TODO - redo the key fields
    -- 'key.fields' = 'header.id',
    'json.timestamp-format.standard' = 'ISO-8601',
    -- TODO - sink.transactional-id-prefix
    -- TODO - sink.delivery-guarantee - do we want exactly once?  It's not needed since the sink needs to dedupe.
    'sink.parallelism' = '{sinkParallelism}'
)