INSERT INTO `daily_content_metrics_csv`
SELECT dt AS `date`, * FROM `daily_content_metrics_view`
