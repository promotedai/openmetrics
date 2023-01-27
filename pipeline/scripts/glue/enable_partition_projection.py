import boto3
from common.table import CommonArgs, Table, get_tables

read_only_fields = ['DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 'IsRegisteredWithLakeFormation',
                    'CatalogId', 'VersionId']


def enable_partition_projection(table: Table):
    client = boto3.client('glue', region_name=table.region)
    table_response = client.get_table(DatabaseName=table.db, Name=table.name)
    table_metadata = table_response['Table']
    parameters = table_metadata['Parameters']

    # Clear out read-only fields that we cannot pass back.
    for field_name in read_only_fields:
        table_metadata.pop(field_name, None)

    # Set the partition projection fields.
    parameters['projection.enabled'] = 'true'
    parameters['projection.hour.type'] = 'integer'
    parameters['projection.hour.range'] = '0,23'
    parameters['projection.hour.digits'] = '2'
    parameters['projection.hour.interval'] = '1'
    parameters['projection.dt.type'] = 'date'
    # Start date is a little arbitrary.  We should update this going forward.
    # TODO - maybe make this a required flag?
    parameters['projection.dt.range'] = '2022-04-01,NOW'
    parameters['projection.dt.format'] = 'yyyy-MM-dd'
    parameters['projection.dt.interval'] = '1'
    parameters['projection.dt.interval.unit'] = 'DAYS'

    # Warning: this overwrites all fields.
    try:
        response = client.update_table(DatabaseName=table.db, TableInput=table_metadata)
        if response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) == 200:
            print(f"updated table {table.region}.{table.db}.{table.name}")
        else:
            print(f"failed to update table {table.region}.{table.db}.{table.name}")
            print(response)
    except Exception as e:
        print(f"failed to update table {table.region}.{table.db}.{table.name}")
        raise e


if __name__ == "__main__":
    args = CommonArgs().parse_args()
    tables = get_tables(args)
    for table in tables:
        enable_partition_projection(table)
