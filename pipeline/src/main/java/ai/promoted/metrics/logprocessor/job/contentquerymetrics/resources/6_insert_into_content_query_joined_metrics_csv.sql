INSERT INTO `{periodLabel}_content_query_joined_metrics_csv`
SELECT dt AS `date`, * FROM `{periodLabel}_content_query_joined_metrics_view`