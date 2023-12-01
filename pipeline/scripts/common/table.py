import boto3
from dataclasses import dataclass
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


class CommonArgs(Tap):
    region: str = ""  # The AWS region.
    db: str = ""  # The Glue DB.
    table_expression: str = ""  # The regular expression of tables to delete.


class PromptArgs(CommonArgs):
    dryrun: bool = False  # Whether to run in a silent mode.
    force: bool = False  # Whether to skip the prompt.


# Put in a safety limit.  This is too many attempts.
max_loops = 101


def get_tables(args: CommonArgs) -> List[Table]:
    """Returns a list of Tables from AWS Glue."""
    client = boto3.client('glue', region_name=args.region)
    if not args.region:
        raise Exception("Need to specify region")
    if not args.table_expression:
        raise Exception("Need to specify table_expression")

    # The max is 1k.
    # Glue Table API has a horrible bug.  Only fetches like 30 candidates in a call.

    result = []
    next_token = None

    i = 0
    while i < max_loops:
        if next_token:
            response = client.get_tables(DatabaseName=args.db, Expression=args.table_expression, MaxResults=1000,
                                         NextToken=next_token)
        else:
            response = client.get_tables(DatabaseName=args.db, Expression=args.table_expression, MaxResults=1000)
        new_tables = [Table(table['Name'], args.db, args.region) for table in response.get('TableList', [])]
        result.extend(new_tables)
        if len(result) == 1000:
            raise Exception("Too many tables encountered (>1k).  Limit the number tables using --table-expression")

        next_token = response.get('NextToken', None)
        if not next_token:
            break
        i += 1

    if i == max_loops:
        raise Exception("Too many (101) get_tables calls.  Likely a bug.")

    return result
