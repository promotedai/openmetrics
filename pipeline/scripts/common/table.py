import boto3
from dataclasses import dataclass
import re
from tap import Tap
from typing import List


@dataclass
class Table:
    """Class for keeping track of table identifiers."""
    name: str
    db: str
    region: str

    def __init__(self, name: str, db: str, region: str):
        self.name = name
        self.db = db
        self.region = region


@dataclass
class StaticDB:
    """Class to list of static DBs."""
    name: str
    region: str

    def __init__(self, name: str, region: str):
        self.name = name
        self.region = region

    def is_dev(self):
        return "_dev_metrics" in self.name

    def is_prod(self):
        return "_prod_metrics" in self.name

    def is_side(self):
        return "_side" in self.name


class CommonArgs(Tap):
    region: str = ""  # The AWS region.
    db: str = ""  # The Glue DB.
    table: str = ""  # The name of a single table to select.
    list_from_glue_table_api: bool = False  # Use the Glue get_tables API.  Be careful.  It's buggy.
    dev: bool = False  # Include dev tables.
    prod: bool = False  # Include prod tables.
    metrics_tables: bool = False  # Scope to the main metrics tables.
    metrics_side_tables: bool = False  # Scope to the side metrics tables.
    only_existing_tables: bool = True  # Checks Glue Table API to make sure the table exists.
    table_prefix: str  # Prefix label for metrics tables.  Required since the static tables needs this flag.
    table_expression: str = ""  # The regular expression of tables to delete.


class PromptArgs(CommonArgs):
    dryrun: bool = False  # Whether to run in a silent mode.
    force: bool = False  # Whether to skip the prompt.


def get_tables(args: CommonArgs) -> List[Table]:
    """Gets tables from the args.  Common method to have similar logic across commands."""
    if args.table:
        return [Table(args.table, args.db, args.region)]
    elif args.list_from_glue_table_api:
        return get_tables_names_from_aws(args.region, args.db, args.table_expression)
    else:
        return get_static_tables(args)


def get_static_tables(args: CommonArgs) -> List[Table]:
    if args.db and args.metrics_tables:
        raise Exception("Do not specify both --db and --metrics_table")
    if args.db and args.metrics_side_tables:
        raise Exception("Do not specify both --db and --metrics_side_table")

    # Start from all DBs and filter down.
    dbs = all_dbs()
    if args.region:
        dbs = list(filter(lambda db: db.region == args.region, dbs))
    if args.db:
        dbs = list(filter(lambda db: db.name == args.db, dbs))
    else:
        if not (args.dev or args.prod):
            raise Exception("Requires at least one of --dev or --prod")
        dbs = list(filter(lambda db: (db.is_dev() and args.dev) or (db.is_prod() and args.prod), dbs))
        if not (args.metrics_tables or args.metrics_side_tables):
            raise Exception("Requires at least one of --metrics_tables or --metrics_side_tables")
        dbs = list(filter(
            lambda db: (not db.is_side() and args.metrics_tables) or (db.is_side() and args.metrics_side_tables),
            dbs))

    # If args.db specified, include all tables.
    include_metrics_tables = True if args.db else args.metrics_tables
    include_metrics_side_tables = True if args.db else args.metrics_side_tables
    tables = from_db_to_table(dbs, args.table_prefix, include_metrics_tables, include_metrics_side_tables)

    if args.table_expression:
        tables = list(filter(lambda table: re.search(args.table_expression, table.name), tables))

    if args.only_existing_tables:
        tables = list(filter(table_exists, tables))

    return tables


def from_static_list(table_names: List[str], table_prefix: str, db, region) -> List[Table]:
    return [Table(table_prefix + name, db, region) for name in table_names]


def get_tables_names_from_aws(region, db, table_expression):
    """Returns a list of table names."""
    client = boto3.client('glue', region_name=region)

    if not region:
        raise Exception("Need to specify region")

    if not table_expression:
        raise Exception("Need to specify table_expression")

    # The max is 1k.
    table_response = client.get_tables(DatabaseName=db, Expression=table_expression, MaxResults=1000)
    table_names = [Table(table['Name'], db, region) for table in table_response['TableList']]

    if len(table_names) == 1000:
        raise Exception("Too many tables encountered (>1k).  Limit the number tables using --table-expression")
    return table_names


def table_exists(table: Table) -> bool:
    # TODO - share clients.
    client = boto3.client('glue', region_name=table.region)
    try:
        # Unfortunately, the list table calls are broken.  We need to get specific tables.
        # TODO - we could run this in parallel.
        client.get_table(DatabaseName=table.db, Name=table.name)
    except client.exceptions.EntityNotFoundException:
        print("not found, db=" + str(table.db) + ", name=" + str(table.name))
        return False
    return True


def all_dbs() -> List[StaticDB]:
    return (
        db_and_side("prm_dev_metrics", "us-east-1")
        + db_and_side("ccc_dev_metrics", "us-east-1")
        + db_and_side("bbb_dev_metrics", "us-east-2")
        + db_and_side("ddd_dev_metrics", "us-east-1")
        + db_and_side("eee_dev_metrics", "us-east-1")
        + db_and_side("ccc_prod_metrics", "us-east-1")
        + db_and_side("bbb_prod_metrics", "us-east-2")
        + db_and_side("ddd_prod_metrics", "us-east-1")
        + db_and_side("eee_prod_metrics", "us-east-1")
    )


def from_db_to_table(dbs: List[StaticDB], table_prefix: str, include_main: bool, include_side: bool) -> List[Table]:
    tables: List[Table] = []
    for db in dbs:
        table_names: List[str] = []
        if include_main and not db.is_side():
            table_names = get_metrics_table_names()
        if include_side and db.is_side():
            table_names = get_metrics_side_table_names()
        tables.extend([Table(table_prefix + table_name, db.name, db.region) for table_name in table_names])
    return tables


def db_and_side(db_prefix, region) -> List[StaticDB]:
    return [
        StaticDB(db_prefix, region),
        StaticDB(db_prefix + "_side", region),
    ]


def get_metrics_table_names():
    return [
        "action",
        "auto_view",
        "cohort_membership",
        "daily_content_metrics",
        "delivery_log",
        "diagnostics",
        "flat_response_insertion",
        "flat_user_response_insertion",
        "hourly_content_metrics",
        "impression",
        "joined_action",
        "joined_impression",
        "joined_user_action",
        "joined_user_impression",
        "log_request",
        "log_user_user",
        "user",
        "view",
    ]


def get_metrics_side_table_names():
    return [
        "all_delivery_log_metadata",
        "all_delivery_log_request",
        "all_delivery_log_request_metadata",
        "delivery_log_ids",
        "dropped_delivery_log_is_bot",
        "dropped_impression_actions",
        "dropped_insertion_impressions",
        "dropped_merge_action_details",
        "dropped_merge_impression_details",
        "dropped_redundant_impression",
        "mismatch_error",
        "partial_response_insertion",
        "request_insertion_ids",
        "response_insertion_ids",
        "rhs_tiny_action",
        "tiny_event",
        "validation_error",
    ]
