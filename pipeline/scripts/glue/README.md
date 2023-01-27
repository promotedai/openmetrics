# Glue Table Scripts

## Where do we get Glue Table Catalog?

Glue Table API is buggy.  Tables can exist and not return from them.  As a result, we have our own catalog defined in `util.py`.  If you want to use the Glue Table API's catalog, you can use `--list_from_glue_table_api`

## Common across commands

Most of the commands involve (1) listing tables and then (2) doing a command on them.  There are multiple args that can be used to scope down to tables.

Here are some args that can be combined together.

### db

```bash
--db ccc_prod_metrics
```

### table
To filter down to a single table

```bash
--region us-east-1 --db ccc_prod_metrics --table "blue_action"
```

### prod vs dev
This can be used to `_prod_` and `_dev_` tables.  These default to `False`.

```bash
--prod
```

or

```bash
--dev
```

### main metrics tables or side tables
Similar to prod vs dev, these can be used to filter to `_metrics` and `_metrics_side` tables.  These default to False.

```bash
--metrics_tables
```

or

```bash
--metrics_side_tables
```

### table prefix
Should usually be set to `blue_` or `green_`

```bash
--table_prefix "blue_"
```

### table expression
Can be used to run a regex on table names

```bash
--table_expression ".*delivery_log.*"
```

### Example commands

The default commands won't return any tables.  One of the prod/dev filters and one of the main/side filters needs to be specified.

List all tables
```bash
pipenv run python -m glue.list_tables --prod --dev --metrics_tables --metrics_side_tables
```

List just the ccc-dev main tables
```bash
pipenv run python -m glue.list_tables  --db ccc_dev_metrics --metrics_tables
```

## List Tables

```bash
pipenv run python -m glue.list_tables --region us-east-1 --db ccc_dev_metrics_side --table_expression "blue_.*"
```

## Get Table Description

```bash
pipenv run python -m glue.get_table --region us-east-1 --db ccc_dev_metrics_side --table "blue_flat_response_insertion"
```

## Start crawlers

This command takes different args than the other commands.

```bash
pipenv run python -m glue.start_crawlers --region us-east-1 --crawler_expression "green.*dev.*"
```

## Enable partition projection
Partition projection is a feature of Athena and Glue such that the partitions do not be crawled to be served through Athena.
Glue Crawlers do not have an easy way to enable them so we use the Glue Table API to set the properties on the Tables. 

```bash
pipenv run python -m glue.enable_partition_projection --region us-east-1 --db ccc_dev_metrics_side --table_prefix "green_"
```

or

```bash
pipenv run python -m glue.enable_partition_projection --region us-east-1 --db ccc_dev_metrics_side --table_prefix "green_" --table_expression ".*joined_.*"
```

## Delete Tables

```bash
pipenv run python -m glue.delete_tables --region us-east-1 --db ccc_dev_metrics_side --table_prefix "blue_"
```

# Testing

No automated tests.  You can run the code with `--dryrun`.  This does not attempt to delete.

# Future work

- This script appears to also drop views.  We need to protect against this.