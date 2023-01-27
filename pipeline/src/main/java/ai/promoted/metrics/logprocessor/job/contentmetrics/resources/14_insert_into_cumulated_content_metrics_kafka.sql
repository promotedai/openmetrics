INSERT INTO `cumulated_content_metrics_kafka`
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd'),
    (
        '1.0',
        'Metric',
        DATE_FORMAT(window_start, 'yyyy-MM-dd') || '@' || CAST(platform_id AS STRING) || '@' || content_id,
        'TODO',
        `platform_id`,
        '{jobName}',
        `window_end`
    ),
    (
        `content_id`,
        (
            `views`,
            `impressions`,
            `joined_impressions`,
            `joined_impression_position_sum`,
            `navigates`,
            `add_to_carts`,
            `checkouts`,
            `purchases`,
            `gmv_usd`
        )
    )
FROM `cumulated_content_metrics_view`
