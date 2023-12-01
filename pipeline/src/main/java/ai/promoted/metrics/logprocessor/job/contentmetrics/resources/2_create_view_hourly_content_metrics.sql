CREATE TEMPORARY VIEW `hourly_content_metrics_view` AS
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS `dt`,
    DATE_FORMAT(window_start, 'HH') AS `hour`,
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
FROM TABLE(TUMBLE(TABLE content_event, DESCRIPTOR(rowtime), INTERVAL '1' HOUR))
GROUP BY
    platform_id,
    content_id,
    window_start,
    window_end
