# Update Athena

This script updates all of the Athena Views.  This is used to create a common View for a labeled output (like `blue_` or `green_`).

Example: This script creates a `ccc_prod_metrics.action` view that queries `ccc_prod_metrics.blue_action`.  When we want to switch to `green_action`, the script can update the view.

## Updating Athena Views

Supports [../glue/README](../glue/README) list flags.

```
aws-vault exec promotedai -d 12h 
pipenv run python -m athena.update_views --region us-east-1 --db ccc_dev_metrics --table_prefix blue_
```

## Output execution details for recent Athena queries

This command can be used to produce a csv that joins together (1) recent queries and (2) their execution details.  This can be loaded into Google Spreadsheet and manually inspected for outliers.

```bash
pipenv run python -m athena.get_query_execution_details --region us-east-1 --work_group primary --max_queries 100 --max_query_length 2000
```

## Tests

No automated tests.

1. Run the script with `--dry-run`.
2. Run the script manually and verify it succeeds with a different `--output-prefix`.  This creates a different view that shouldn't be used.

```
aws-vault exec promotedai -d 12h 
pipenv run python -m athena.update_views --region us-east-1 --db ccc_dev_metrics --table_prefix blue_ --output_view_prefix test20200101_
```

If the script succeeds, you can drop the tables.

```
pipenv run python -m athena.drop_views --region us-east-1 --db ccc_dev_metrics --view_prefix test20200101_
```

## Future plans

This script implementation is likely temporary.  Expected changes:
- Have a more formal and explicit list of tables and views.  It's easier right now to not have a central definition for that.
- This will migrate into a CICD system.  It'll exist in a workflow that combines with other scripts.  This might change the CLI flag/runner aspect of the scripts.  This might mean merging Python scripts into a single directory so it's easier to reuse code.
