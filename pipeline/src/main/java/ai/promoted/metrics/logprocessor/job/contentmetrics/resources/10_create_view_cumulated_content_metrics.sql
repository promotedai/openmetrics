CREATE TEMPORARY VIEW `cumulated_content_metrics_view`
    (
        window_start,
        window_end,
        platform_id,
        content_id,
        `views`,
        impressions,
        joined_impressions,
        joined_impression_position_sum,
        navigates,
        add_to_carts,
        checkouts,
        purchases,
        -- TODO - rename this.
        gmv_usd
    )
AS
SELECT
    window_start,
    window_end,
    platform_id,
    content_id,
    SUM(view_count),
    SUM(impression_count),
    SUM(joined_impression_count),
    SUM(joined_impression_position_sum),
    SUM(navigate_count),
    SUM(add_to_cart_count),
    SUM(checkout_count),
    SUM(purchase_count),
    SUM(gmv_usd_micros)
FROM TABLE(
    CUMULATE(TABLE content_event, DESCRIPTOR(rowtime), {cumulatedWindowStep}, INTERVAL '1' DAY))
GROUP BY
    platform_id,
    content_id,
    window_start,
    window_end
