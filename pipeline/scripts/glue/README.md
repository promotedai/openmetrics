# Glue Table Scripts

## Warning

- All commands are scoped to a single Glue Database.
- Glue returns Views through the `get_tables` call.  Please be careful with your regexes.


## Common across commands

Most of the commands involve (1) listing tables and then (2) doing a command on them.  There are multiple args that can be used to scope down to tables.

Here are some args that can be combined together.

### All tables in a db.

```bash
--db mymarket_prod_metrics --table_expression ".*"
```

### All blue tables.

```bash
--db mymarket_prod_metrics --table_expression "^blue_.*"
```

### A single table
To filter down to a single table

```bash
--region us-east-1 --db mymarket_prod_metrics --table_expression "^blue_action$"
```

## Example commands

## List Tables

```bash
pipenv run python -m glue.list_tables --region us-east-1 --db mymarket_dev_metrics_side --table_expression "^blue_.*"
```

## Get Table Description

```bash
pipenv run python -m glue.get_table --region us-east-1 --db mymarket_dev_metrics_side --table_expression "blue_flat_response_insertion"
```

## Start crawlers

This command takes different args than the other commands because the expression is by crawler names.

```bash
pipenv run python -m glue.start_crawlers --region us-east-1 --crawler_expression "green.*dev.*"
```

## Enable partition projection
Partition projection is a feature of Athena and Glue such that the partitions do not be crawled to be served through Athena.
Glue Crawlers do not have an easy way to enable them so we use the Glue Table API to set the properties on the Tables. 

```bash
pipenv run python -m glue.enable_partition_projection --region us-east-1 --db mymarket_dev_metrics_side --table_expression "^green_.*"
```

## Delete Tables

```bash
pipenv run python -m glue.delete_tables --region us-east-1 --db mymarket_dev_metrics_side --table_expression "^blue_.*"
```

# Testing

Some unit tests.  No automated tests.  You can run the code with `--dryrun`.  This does not attempt to delete.

# Future work

- This script appears to also drop views.  We need to protect against this.