import argparse
from deploy.config import default_config, new_config, to_flags
from deploy.deploy_job import deploy_job, validate_config

# Most of the code is the deploy module so it's easier to write tests.

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # If you update the default values, make sure to update config.new_config.

    parser.add_argument(
        "--namespace",
        type=str,
        help="The k8s namespace.  Required.")

    # TODO - should this stuff be down in main?
    parser.add_argument(
        "--job-name",
        type=str,
        help="The job name.  Required.")

    # TODO - rename stream-job-file to job-file.
    parser.add_argument(
        "--stream-job-file",
        type=str,
        help="The path to the Flink Stream Job Kubernetes config.  Required.")

    parser.add_argument(
        "--new",
        default=default_config.new,
        help="Whether the job is new.  Defaults to False.",
        action=argparse.BooleanOptionalAction)

    parser.add_argument(
        "--create-new-if-stopped",
        default=default_config.create_new_if_stopped,
        help="Create the job if it's stopped.  This is useful to not override manually stopped jobs."
             "  Defaults to False.",
        action=argparse.BooleanOptionalAction)

    parser.add_argument(
        "--deploy",
        default=default_config.deploy,
        help="Whether to deploy the job.  Setting to false will cause jobs to savepoint but not deploy again."
             "  Defaults to True.",
        action=argparse.BooleanOptionalAction)

    parser.add_argument(
        "--start-from-savepoint",
        type=str,
        help="Used to start from a specific savepoint.  Defaults to savepoint.")

    parser.add_argument(
        "--allow-non-restored-state",
        default=default_config.allow_non_restored_state,
        help="Can be set to allow recovering from a savepoint and not using all state.  Default to False.",
        action=argparse.BooleanOptionalAction)

    parser.add_argument(
        "--dry-run",
        default=default_config.dry_run,
        help="Whether to run as a dry run.  Defaults to False.",
        action=argparse.BooleanOptionalAction)

    parser.add_argument(
        "--output-continuation-config-path",
        type=str,
        help="Used to output the continuation config to a spot for later use.  Currently only supports file systems."
             "  Defaults to none (do not write it).")

    # TODO - validate args.

    args = parser.parse_args()

    args = vars(args)
    output_continuation_config_path = args.pop("output_continuation_config_path", None)
    config = new_config(args)

    validate_config(config)

    cont_config = deploy_job(config)

    # TODO - change the exception logic so that we can get cont_configs on failure too.
    if cont_config:
        print("******************************************************************************")
        print("Encountered a continuation config.  Run the script again with the following args")
        print("As config")
        print('  ' + str(cont_config))
        print("As flags")
        print('  ' + to_flags(cont_config))
        if output_continuation_config_path:
            with open(output_continuation_config_path, 'a') as f:
                f.write(to_flags(cont_config) + '\n')
            print('Saved to file: ' + output_continuation_config_path)
        print('')
