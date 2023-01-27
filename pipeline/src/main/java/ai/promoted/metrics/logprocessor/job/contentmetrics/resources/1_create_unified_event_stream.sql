CREATE TEMPORARY VIEW `content_event` AS
SELECT
    rowtime,
    platform_id,
    content_id,
    CAST(1 AS BIGINT) AS view_count,
    CAST(0 AS BIGINT) AS impression_count,
    CAST(0 AS BIGINT) AS joined_impression_count,
    CAST(0 AS BIGINT) AS joined_impression_position_sum,
    CAST(0 AS BIGINT) AS navigate_count,
    CAST(0 AS BIGINT) AS add_to_cart_count,
    CAST(0 AS BIGINT) AS checkout_count,
    CAST(0 AS BIGINT) AS purchase_count,
    CAST(0 AS BIGINT) AS gmv_usd_micros
FROM `view`
WHERE content_id <> ''
UNION ALL
SELECT
    rowtime,
    platform_id,
    content_id,
    CAST(0 AS BIGINT) AS view_count,
    CAST(1 AS BIGINT) AS impression_count,
    CAST(0 AS BIGINT) AS joined_impression_count,
    CAST(0 AS BIGINT) AS joined_impression_position_sum,
    CAST(0 AS BIGINT) AS navigate_count,
    CAST(0 AS BIGINT) AS add_to_cart_count,
    CAST(0 AS BIGINT) AS checkout_count,
    CAST(0 AS BIGINT) AS purchase_count,
    CAST(0 AS BIGINT) AS gmv_usd_micros
FROM impression
WHERE content_id <> ''
UNION ALL
SELECT
    rowtime,
    platform_id,
    content_id,
    CAST(0 AS BIGINT) AS view_count,
    CAST(0 AS BIGINT) AS impression_count,
    CAST(1 AS BIGINT) AS joined_impression_count,
    CAST(`position` AS BIGINT) AS joined_impression_position_sum,
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
    CAST(0 AS BIGINT) AS view_count,
    CAST(0 AS BIGINT) AS impression_count,
    CAST(0 AS BIGINT) AS joined_impression_count,
    CAST(0 AS BIGINT) AS joined_impression_position_sum,
    IF(action_type=2, CAST(1 AS BIGINT), CAST(0 AS BIGINT)) AS navigate_count,
    IF(action_type=4, CAST(1 AS BIGINT), CAST(0 AS BIGINT)) AS add_to_cart_count,
    IF(action_type=8, CAST(1 AS BIGINT), CAST(0 AS BIGINT)) AS checkout_count,
    IF(action_type=3, CAST(1 AS BIGINT), CAST(0 AS BIGINT)) AS purchase_count,
    CAST(0 AS BIGINT) AS gmv_usd_micros
FROM action
WHERE content_id <> ''
UNION ALL
SELECT
    rowtime,
    platform_id,
    content_id,
    CAST(0 AS BIGINT) AS view_count,
    CAST(0 AS BIGINT) AS impression_count,
    CAST(0 AS BIGINT) AS joined_impression_count,
    CAST(0 AS BIGINT) AS joined_impression_position_sum,
    IF(action_type=2, quantity, CAST(0 AS BIGINT)) AS navigate_count,
    IF(action_type=4, quantity, CAST(0 AS BIGINT)) AS add_to_cart_count,
    IF(action_type=8, quantity, CAST(0 AS BIGINT)) AS checkout_count,
    IF(action_type=3, quantity, CAST(0 AS BIGINT)) AS purchase_count,
    IF(action_type=3, quantity * price_usd_micros_per_unit, CAST(0 AS BIGINT)) AS gmv_usd_micros
FROM action_cart_content
WHERE content_id <> ''
