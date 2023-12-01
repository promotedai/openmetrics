# Replace S3 tables

This can be used to incrementally replace s3 tables that have a `/dt=YYYY-MM-DD/` partition structure.

To list out debug info for the operations.

```bash
pipenv run python -m s3replace.replace_s3_objects --region us-east-1 --bucket my-dev-event-logs --source_key_prefix 'green/fix/raw/delivery-log' --destination_key_prefix 'green/raw/delivery-log' --start_dt 2022-08-15 --exclusive_end_dt 2022-08-19
```

To apply the operations.

```bash
pipenv run python -m s3replace.replace_s3_objects --region us-east-1 --bucket my-dev-event-logs --source_key_prefix 'green/fix/raw/delivery-log' --destination_key_prefix 'green/raw/delivery-log' --start_dt 2022-08-15 --exclusive_end_dt 2022-08-19 --apply
```

## Running in AWS

1. Create a temporary ECR repository.  E.g. prm-metrics-scripts.
2. Run `make docker_build`.  You might need to [setup docker to push to ECR](https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html#cli-authenticate-registry).
3. Fork an update the args of an existing similar k8s config.
4. `kubectl apply -f` the config.