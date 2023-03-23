import unittest
from common.table import CommonArgs, Table, get_tables


class TableTest(unittest.TestCase):

    def test_get_tables_fixed_args(self):
        args = CommonArgs()
        args.region = "us-east-1"
        args.db = "ccc_prod_metrics"
        args.table_prefix = "blue_"
        args.only_existing_tables = False
        self.assertEqual(
            get_tables(args),
            [
                Table("blue_action", "ccc_prod_metrics", "us-east-1"),
                Table("blue_auto_view", "ccc_prod_metrics", "us-east-1"),
                Table("blue_cohort_membership", "ccc_prod_metrics", "us-east-1"),
                Table("blue_daily_content_metrics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_daily_content_query_joined_metrics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_delivery_log", "ccc_prod_metrics", "us-east-1"),
                Table("blue_diagnostics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_flat_actioned_response_insertion", "ccc_prod_metrics", "us-east-1"),
                Table("blue_flat_response_insertion", "ccc_prod_metrics", "us-east-1"),
                Table("blue_hourly_content_metrics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_impression", "ccc_prod_metrics", "us-east-1"),
                Table("blue_joined_action", "ccc_prod_metrics", "us-east-1"),
                Table("blue_joined_impression", "ccc_prod_metrics", "us-east-1"),
                Table("blue_log_request", "ccc_prod_metrics", "us-east-1"),
                Table("blue_log_user_user", "ccc_prod_metrics", "us-east-1"),
                Table("blue_user", "ccc_prod_metrics", "us-east-1"),
                Table("blue_view", "ccc_prod_metrics", "us-east-1"),
                Table("blue_weekly_content_query_joined_metrics", "ccc_prod_metrics", "us-east-1"),
            ]
        )

    def test_get_tables_all(self):
        args = CommonArgs()
        args.dev = True
        args.prod = True
        args.metrics_tables = True
        args.metrics_side_tables = True
        args.table_prefix = "blue_"
        args.only_existing_tables = False
        tables = get_tables(args)
        self.assertTrue(len(tables) > 100)
        self.assertTrue(Table("blue_action", "ccc_prod_metrics", "us-east-1") in tables)

    def test_get_tables_invalid_args(self):
        args = CommonArgs()
        args.db = "ccc_prod_metrics"
        args.metrics_side_tables = True
        args.table_prefix = "blue_"
        args.only_existing_tables = False
        self.assertRaises(Exception, get_tables, args)

    def test_get_tables_regional_optional(self):
        args = CommonArgs()
        args.db = "ccc_prod_metrics"
        args.table_prefix = "blue_"
        args.only_existing_tables = False
        self.assertEqual(
            get_tables(args),
            [
                Table("blue_action", "ccc_prod_metrics", "us-east-1"),
                Table("blue_auto_view", "ccc_prod_metrics", "us-east-1"),
                Table("blue_cohort_membership", "ccc_prod_metrics", "us-east-1"),
                Table("blue_daily_content_metrics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_daily_content_query_joined_metrics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_delivery_log", "ccc_prod_metrics", "us-east-1"),
                Table("blue_diagnostics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_flat_actioned_response_insertion", "ccc_prod_metrics", "us-east-1"),
                Table("blue_flat_response_insertion", "ccc_prod_metrics", "us-east-1"),
                Table("blue_hourly_content_metrics", "ccc_prod_metrics", "us-east-1"),
                Table("blue_impression", "ccc_prod_metrics", "us-east-1"),
                Table("blue_joined_action", "ccc_prod_metrics", "us-east-1"),
                Table("blue_joined_impression", "ccc_prod_metrics", "us-east-1"),
                Table("blue_log_request", "ccc_prod_metrics", "us-east-1"),
                Table("blue_log_user_user", "ccc_prod_metrics", "us-east-1"),
                Table("blue_user", "ccc_prod_metrics", "us-east-1"),
                Table("blue_view", "ccc_prod_metrics", "us-east-1"),
                Table("blue_weekly_content_query_joined_metrics", "ccc_prod_metrics", "us-east-1"),
            ]
        )

    def test_get_tables_db_optional(self):
        args = CommonArgs()
        args.region = "us-east-1"
        args.prod = True
        args.metrics_tables = True
        args.metrics_side_tables = True
        args.table_prefix = "blue_"
        args.only_existing_tables = False
        tables = get_tables(args)
        self.assertTrue(Table("blue_action", "ccc_prod_metrics", "us-east-1") in tables)
        self.assertTrue(Table("blue_all_delivery_log_metadata", "ddd_prod_metrics_side", "us-east-1") in tables)

    def test_get_tables_table_expression(self):
        args = CommonArgs()
        args.region = "us-east-1"
        args.table_expression = ".*delivery_log.*"
        args.prod = True
        args.metrics_tables = True
        args.metrics_side_tables = True
        args.table_prefix = "blue_"
        args.only_existing_tables = False
        tables = get_tables(args)
        self.assertFalse(Table("blue_action", "ccc_prod_metrics", "us-east-1") in tables)
        self.assertTrue(Table("blue_all_delivery_log_metadata", "ccc_prod_metrics_side", "us-east-1") in tables)
        self.assertTrue(Table("blue_all_delivery_log_metadata", "ddd_prod_metrics_side", "us-east-1") in tables)
