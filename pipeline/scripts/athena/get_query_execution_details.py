import boto3
import csv
from tap import Tap

# Columns for the CSV
query_execution_id_key = 'QueryExecutionId'
query_key = 'Query'
submission_date_time_key = 'SubmissionDateTime'
completion_date_time_key = 'CompletionDateTime'
engine_execution_time_in_millis_key = 'EngineExecutionTimeInMillis'
data_scanned_in_bytes_key = 'DataScannedInBytes'
total_execution_time_in_millis_key = 'TotalExecutionTimeInMillis'
query_queue_time_in_millis_key = 'QueryQueueTimeInMillis'
query_planning_time_in_millis_key = 'QueryPlanningTimeInMillis'
service_processing_time_in_millis_key = 'ServiceProcessingTimeInMillis'

fieldnames = [
    query_execution_id_key,
    query_key,
    submission_date_time_key,
    completion_date_time_key,
    engine_execution_time_in_millis_key,
    data_scanned_in_bytes_key,
    total_execution_time_in_millis_key,
    query_queue_time_in_millis_key,
    query_planning_time_in_millis_key,
    service_processing_time_in_millis_key,
]


class GetQueryExecutionDetailsArgs(Tap):
    region: str
    work_group: str
    max_queries: int = 50
    max_batch_size: int = 50
    output_file_name: str = "execution_details.csv"
    max_query_length: int = 1000


def output_csv(args: GetQueryExecutionDetailsArgs):
    """Outputs csv with recent Athena query execution details."""
    client = boto3.client('athena', region_name=args.region)
    next_token: str = None

    with open(args.output_file_name, 'w', newline='') as output_file:
        print(f"started writing execution details to {args.output_file_name}")
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(0, args.max_queries, args.max_batch_size):
            if next_token is None:
                response = client.list_query_executions(
                    MaxResults=args.max_batch_size,
                    WorkGroup=args.work_group,
                )
            else:
                response = client.list_query_executions(
                    NextToken=next_token,
                    MaxResults=args.max_batch_size,
                    WorkGroup=args.work_group,
                )
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            if response_code != 200:
                print(f"Encountered unsupported http response code = {response_code}")
            next_token = response['NextToken']

            execution_response = client.batch_get_query_execution(
                QueryExecutionIds=response['QueryExecutionIds']
            )
            for execution in execution_response['QueryExecutions']:
                status = execution['Status']
                statistics = execution['Statistics']
                writer.writerow({
                    query_execution_id_key: execution[query_execution_id_key],
                    query_key: truncate(execution[query_key], args.max_query_length),
                    submission_date_time_key: status[submission_date_time_key],
                    completion_date_time_key: status[completion_date_time_key],
                    engine_execution_time_in_millis_key: statistics[engine_execution_time_in_millis_key],
                    data_scanned_in_bytes_key: statistics[data_scanned_in_bytes_key],
                    total_execution_time_in_millis_key: statistics[total_execution_time_in_millis_key],
                    query_queue_time_in_millis_key: statistics[query_queue_time_in_millis_key],
                    query_planning_time_in_millis_key: statistics.get(query_planning_time_in_millis_key, 'NA'),
                    service_processing_time_in_millis_key: statistics[service_processing_time_in_millis_key],
                })
        print("finished writing execution details")


def truncate(s: str, max_length: int):
    return (s[:max_length] + '...') if len(s) > max_length else s


if __name__ == "__main__":
    args = GetQueryExecutionDetailsArgs().parse_args()
    output_csv(args)
