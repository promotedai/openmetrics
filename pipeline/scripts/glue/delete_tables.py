import boto3
from common.table import PromptArgs, get_tables


def delete_tables(args: PromptArgs):
    """Deletes a list of tables."""
    tables = get_tables(args)

    if len(tables) == 0:
        print("\nNo matching tables.")
        return

    print(f"Tables to delete ({len(tables)}):")
    for table in tables:
        print(f"- {table.region}.{table.db}.{table.name}")

    if args.dryrun:
        print("\nNot deleting because `--dryrun` as specified")
        return

    if not args.force:
        user_input = input("\nEnter 'yes' to continue: ")
        if user_input != 'yes':
            print("Not deleting.")
            return

    # Batch up based on [region, db].
    batch_dict = {}
    for table in tables:
        key = (table.region, table.db)
        batch_tables = batch_dict.get(key, [])
        batch_tables.append(table)
        batch_dict[key] = batch_tables

    for key, tables in batch_dict.items():
        region = key[0]
        db = key[1]
        client = boto3.client('glue', region_name=region)
        table_names = [table.name for table in tables]
        delete_response = client.batch_delete_table(
            DatabaseName=db,
            TablesToDelete=table_names
        )
        if len(delete_response['Errors']) != 0:
            raise ValueError(f"Encountered errors when deleting tables: {delete_response}")
        # TODO - figure out if I have to synchronously delete Versions and Partitions.
        print("Deleted:")
        for table_name in table_names:
            print(f"- {region}.{db}.{table_name}")
    print("Deleted finished.  Versions and Partitions will be cleaned up asynchronously.")


if __name__ == "__main__":
    args = PromptArgs().parse_args()
    delete_tables(args)
