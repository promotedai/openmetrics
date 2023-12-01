import unittest
from unittest.mock import Mock, patch
from common.table import CommonArgs, Table, get_tables


class TableTest(unittest.TestCase):

    @patch('boto3.client')
    def test_get_tables_empty(self, mock_client):
        mock_get_tables = Mock()
        mock_client.return_value = mock_get_tables
        mock_get_tables.get_tables.return_value = {
            "TableList": [],
        }
        args = CommonArgs()
        args.region = "us-east-1"
        args.db = "mymarket_dev_metrics"
        args.table_expression = ".*delivery_log.*"
        tables = get_tables(args)
        self.assertEqual([], tables)

    @patch('boto3.client')
    def test_get_tables_one_api_call(self, mock_client):
        mock_get_tables = Mock()
        mock_client.return_value = mock_get_tables
        mock_get_tables.get_tables.return_value = {
            "TableList": [
                {
                    "Name": "table1"
                },
                {
                    "Name": "table2"
                }
            ],
            "NextToken": None
        }
        args = CommonArgs()
        args.region = "us-east-1"
        args.db = "mymarket_dev_metrics"
        args.table_expression = ".*delivery_log.*"
        tables = get_tables(args)
        self.assertEqual([
            Table(name='table1', db='mymarket_dev_metrics', region='us-east-1'),
            Table(name='table2', db='mymarket_dev_metrics', region='us-east-1')
        ], tables)

    @patch('boto3.client')
    def test_get_tables_multiple_api_calls(self, mock_client):
        mock_get_tables = Mock()
        mock_client.return_value = mock_get_tables

        # Define multiple return values for sequential calls
        mock_get_tables.get_tables.side_effect = [
            {
                "TableList": [{"Name": "table1"}, {"Name": "table2"}],
                "NextToken": "token1"
            },
            {
                "TableList": [{"Name": "table3"}],
                "NextToken": None
            }
        ]

        args = CommonArgs()
        args.region = "us-east-1"
        args.db = "mymarket_dev_metrics"
        args.table_expression = ".*delivery_log.*"
        tables = get_tables(args)
        self.assertEqual([
            Table(name='table1', db='mymarket_dev_metrics', region='us-east-1'),
            Table(name='table2', db='mymarket_dev_metrics', region='us-east-1'),
            Table(name='table3', db='mymarket_dev_metrics', region='us-east-1')
        ], tables)
