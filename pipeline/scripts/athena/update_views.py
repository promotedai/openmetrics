import boto3
from common.table import PromptArgs, Table, get_tables
from pyathena import connect
from pyathena.connection import Connection
from .util import expect_empty_result


class UpdateViewArgs(PromptArgs):
    s3_staging_bucket: str = ""  # The s3 staging bucket for Athena
    input_table_prefix: str = ""  # An optional prefix for the input table to remove.  E.g. Allows us to drop "green_".
    output_view_prefix: str = ""  # An optional prefix for the output view.  Used for manual testing.


def upsert_views(args: UpdateViewArgs):
    """Upserts views for tables defined in `args`.  Throws on exception"""
    tables = get_tables(args)

    if len(tables) == 0:
        print("\nNo matching tables.")
        return

    print(f"Views to upsert ({len(tables)}):")
    for table in tables:
        output_view_name = get_output_view_name(table.name, args.input_table_prefix, args.output_view_prefix)
        print(f"- {table.region}.{table.db}.{output_view_name} will `select * from` {table.name}")

    if args.dryrun:
        print("\nNot upserting because `--dryrun` as specified")
        return

    if not args.force:
        user_input = input("\nEnter 'yes' to continue: ")
        if user_input != 'yes':
            print("Not upserting.")
            return

    athena_conns = AthenaConnections(args.s3_staging_bucket)
    for table in tables:
        output_view_name = get_output_view_name(table.name, args.input_table_prefix, args.output_view_prefix)
        athena_conn = athena_conns.get_conn(table.region, table.db)
        upsert_view(athena_conn, table, output_view_name)


class AthenaConnections:
    def __init__(self, s3_staging_bucket):
        self.s3_staging_bucket = s3_staging_bucket
        self.dbregion_to_athena_conn: map[tuple[str, str], Connection] = {}

    def get_conn(self, region: str, db: str) -> Connection:
        region_and_db = (region, db)
        athena_conn = self.dbregion_to_athena_conn.get(region_and_db, None)
        if not athena_conn:
            athena_conn = connect(
                s3_staging_dir=f"s3://{self.s3_staging_bucket}/update-athena/{db}/",
                region_name=region)
            self.dbregion_to_athena_conn[region_and_db] = athena_conn
        return athena_conn


def upsert_view(athena_conn: Connection, table: Table, output_view_name: str):
    """Upserts an Athena View.  Throws on exception."""
    try:
        cursor = athena_conn.cursor()
        cursor.execute(f"CREATE OR REPLACE VIEW {table.db}.{output_view_name} AS SELECT * FROM {table.db}.{table.name}")
        expect_empty_result(cursor)
    except BaseException as e:
        print(f"Failed to update {table.region}.{table.db}.{output_view_name}: {e}")
        # raise
    else:
        print(f"Updated {table.region}.{table.db}.{output_view_name}")


def get_output_view_name(input_table_name, input_table_prefix, output_view_prefix):
    base_table_name = input_table_name.removeprefix(input_table_prefix)
    return output_view_prefix + base_table_name


def get_table_names_from_prefix(region, db, table_prefix):
    """Returns a of table names (list(string)) matching `table_prefix`."""
    return get_table_names_from_regex(region, db, f"^{table_prefix}.*")


def get_table_names_from_regex(region, db, table_expression):
    """Returns a of table names (list(string)) matching `table_expression`."""
    client = boto3.client('glue', region_name=region)
    table_response = client.get_tables(DatabaseName=db, Expression=table_expression, MaxResults=1000)
    return [table['Name'] for table in table_response['TableList']]


if __name__ == "__main__":
    args = UpdateViewArgs().parse_args()
    upsert_views(args)
