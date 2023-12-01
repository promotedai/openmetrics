CREATE TEMPORARY VIEW `{periodLabel}_content_query_joined_metrics_view` AS
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS `dt`,
    platform_id,
    content_id,
    search_query,
    SUM(impression_count) AS impression_count,
    SUM(impression_position_sum) AS impression_position_sum,
    SUM(navigate_count) AS navigate_count,
    SUM(add_to_cart_count) AS add_to_cart_count,
    SUM(checkout_count) AS checkout_count,
    SUM(purchase_count) AS purchase_count,
    SUM(gmv_usd_micros) AS gmv_usd_micros
FROM TABLE(TUMBLE(TABLE content_query_event, DESCRIPTOR(rowtime), INTERVAL '{periodDays}' DAY, INTERVAL '{periodOffsetDays}' DAY))
GROUP BY
    platform_id,
    content_id,
    search_query,
    window_start,
    window_end
