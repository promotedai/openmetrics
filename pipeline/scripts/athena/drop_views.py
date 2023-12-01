from pyathena import connect
from pyathena.connection import Connection
from tap import Tap
from .util import expect_empty_result

# Script is used for dropping test views or deprecated views.


class Args(Tap):
    region: str = ""  # The AWS region.
    s3_staging_bucket: str = ""  # The s3 staging bucket for Athena
    db: str = ""  # The Glue DB.
    view_prefix: str = ""  # The prefix of views to drop.  E.g. `test20200101_`.
    dryrun: bool = False  # Whether to run in a silent mode.
    force: bool = False  # Whether to skip the prompt.


def drop_views(args: Args):
    """Drop views defined in `args`.  Throws on exception"""
    athena_conn = connect(
        s3_staging_dir=f"s3://{args.s3_staging_bucket}/update-athena/{args.db}/",
        region_name=args.region)
    view_names = get_view_names_from_prefix(athena_conn, args.db, args.view_prefix)

    if len(view_names) == 0:
        print("\nNo matching views.")
        return

    if len(view_names) >= 1000:
        print(f"Warning: too many views encountered ({len(view_names)} >= 1000).  This will only drop the "
              "first 1000 views.  Run again to drop the remaining views.")

    print(f"Views to drop ({len(view_names)}):")
    for view_name in view_names:
        print(f"- {args.db}.{view_name}")

    if args.dryrun:
        print("\nNot dropping because `--dryrun` as specified")
        return

    if not args.force:
        user_input = input("\nEnter 'yes' to continue: ")
        if user_input != 'yes':
            print("Not dropping.")
            return
    for view_name in view_names:
        drop_view(athena_conn, args.db, view_name)


def drop_view(athena_conn: Connection, db: str, view_name: str):
    """Drops an Athena View.  Throws on exception."""
    try:
        cursor = athena_conn.cursor()
        cursor.execute(f"DROP VIEW {db}.{view_name}")
        expect_empty_result(cursor)
    except BaseException as e:
        print(f"Failed to drop {db}.{view_name}: {e}")
        raise
    else:
        print(f"Dropped {db}.{view_name}")


def get_view_names_from_prefix(athena_conn: Connection, db: str, view_prefix: str):
    return get_view_names_from_regex(athena_conn, db, f"^{view_prefix}.*")


def get_view_names_from_regex(athena_conn: Connection, db: str, view_expression: str):
    cursor = athena_conn.cursor()
    cursor.execute(f"SHOW VIEWS IN {db} LIKE '{view_expression}'")
    return [row[0] for row in cursor.fetchall()]


if __name__ == "__main__":
    args = Args().parse_args()
    drop_views(args)
