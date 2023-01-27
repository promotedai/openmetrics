import argparse
import boto3


def delete_tables(args):
    """Deletes a list of tables (defined by args.table_expression)."""
    client = boto3.client('glue', region_name=args.region)

    # The max is 1k.
    table_response = client.get_table(DatabaseName=args.db, Name=args.table)
    print(table_response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", type=str, help="The AWS region.  Required.")
    parser.add_argument("--db", type=str, help="The Glue DB.  Required.")
    parser.add_argument(
        "--table",
        type=str,
        help="The name of the table")
    args = parser.parse_args()
    delete_tables(args)
