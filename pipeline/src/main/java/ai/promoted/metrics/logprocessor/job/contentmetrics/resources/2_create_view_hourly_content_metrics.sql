CREATE TEMPORARY VIEW `hourly_content_metrics_view` AS
SELECT
    DATE_FORMAT(TUMBLE_ROWTIME(rowtime, INTERVAL '1' HOUR), 'yyyy-MM-dd') AS `dt`,
    DATE_FORMAT(TUMBLE_ROWTIME(rowtime, INTERVAL '1' HOUR), 'HH') AS `hour`,
    platform_id,
    content_id,
    SUM(view_count) AS view_count,
    SUM(impression_count) AS impression_count,
    SUM(joined_impression_count) AS joined_impression_count,
    SUM(joined_impression_position_sum) AS joined_impression_position_sum,
    SUM(navigate_count) AS navigate_count,
    SUM(add_to_cart_count) AS add_to_cart_count,
    SUM(checkout_count) AS checkout_count,
    SUM(purchase_count) AS purchase_count,
    SUM(gmv_usd_micros) AS gmv_usd_micros
FROM content_event
GROUP BY
    platform_id,
    content_id,
    TUMBLE(rowtime, INTERVAL '1' HOUR)