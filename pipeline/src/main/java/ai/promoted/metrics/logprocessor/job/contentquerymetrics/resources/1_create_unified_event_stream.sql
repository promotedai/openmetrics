CREATE TEMPORARY VIEW `content_query_event` AS
SELECT
    rowtime,
    platform_id,
    content_id,
    search_query,
    CAST(1 AS BIGINT) AS impression_count,
    CAST(`position` AS BIGINT) AS impression_position_sum,
    CAST(0 AS BIGINT) AS navigate_count,
    CAST(0 AS BIGINT) AS add_to_cart_count,
    CAST(0 AS BIGINT) AS checkout_count,
    CAST(0 AS BIGINT) AS purchase_count,
    CAST(0 AS BIGINT) AS gmv_usd_micros
FROM joined_impression
WHERE content_id <> ''
UNION ALL
SELECT
    rowtime,
    platform_id,
    content_id,
    search_query,
    CAST(0 AS BIGINT) AS impression_count,
    CAST(0 AS BIGINT) AS impression_position_sum,
    IF(action_type=2, quantity, CAST(0 AS BIGINT)) AS navigate_count,
    IF(action_type=4, quantity, CAST(0 AS BIGINT)) AS add_to_cart_count,
    IF(action_type=8, quantity, CAST(0 AS BIGINT)) AS checkout_count,
    IF(action_type=3, quantity, CAST(0 AS BIGINT)) AS purchase_count,
    IF(action_type=3, quantity * price_usd_micros_per_unit, CAST(0 AS BIGINT)) AS gmv_usd_micros
FROM attributed_action
WHERE content_id <> '' AND model_id = 1
