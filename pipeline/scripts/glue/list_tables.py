from common.table import CommonArgs, get_tables


def list_tables(args: CommonArgs):
    """Deletes a list of tables (defined by args.table_expression)."""
    tables = get_tables(args)
    for table in tables:
        print(f"{table.region}.{table.db}.{table.name}")


if __name__ == "__main__":
    args = CommonArgs().parse_args()
    list_tables(args)
