from collections import namedtuple


# Config represents the parameters for a single Flink job.
# If you update, update the other functions in this file.
# TODO - rename stream_job_file to k8s_file.
Config = namedtuple('Config', [
    # K8s namespace. String. Required.
    'namespace',
    # Name of the Job. String. Required.
    'job_name',
    # A file path to a K8s config. String. Required.
    'stream_job_file',
    # A SubJobType enum used in the raw log job. String. Optional.
    'sub_job_type',
    # A bool to indicate if a job is new (skips some checks). This is used to make rollouts safer.
    # Boolean. Defaults to False.
    'new',
    # A bool to indicate if a new job should be started if we detect a stopped job with the same name.
    # This is used to make rollouts safer. Boolean. Defaults to False.
    'create_new_if_stopped',
    # Whether to deploy the job. Setting to false allows the job to savepointed for later use.
    # Boolean. Defaults to False.
    'deploy',
    # If set, the script will try to start the job using this field as the savepoint.
    # String. Defaults to None.
    'start_from_savepoint',
    # Can be used in Flink deploys to --allow-non-restored-state. Boolean. Defaults to False.
    'allow_non_restored_state',
    # Whether to run as a dry run. Boolean. Defaults to False.
    'dry_run'],
    defaults=[
        # namespace
        None,
        # job_name
        None,
        # stream_job_file
        None,
        # sub_job_type
        None,
        # new
        False,
        # create_new_if_stopped
        False,
        # deploy
        True,
        # start_from_savepoint
        None,
        # allow_non_restored_state
        False,
        # dry_run
        False
])


# Default values that can be reused with other files.
config_field_types = Config(**{
    'namespace': str,
    'job_name': str,
    'stream_job_file': str,
    'sub_job_type': str,
    'new': bool,
    'create_new_if_stopped': bool,
    'deploy': bool,
    'start_from_savepoint': str,
    'allow_non_restored_state': bool,
    'dry_run': bool,
})


default_config = Config()


def new_config(asdict):
    """
    Creates a new Config with default values.  Mostly used for tests.

    Args:
        asdict: Dictionary of fields.

    Returns:
        A new Config with default values filled in.
    """
    return default_config._replace(**asdict)


def to_flag_field_name(field_name):
    return field_name.replace("_", "-")


def to_flags(config):
    """
    Converts a continuation config to flags so it's easy to run the next steps.
    Continuation configs are used in cases where we should run another deploy step.
    The main use case is if we say --no-deploy and take down a job.  The continuation
    config shows how to start it up again.

    Args:
        config: A continuation config for a follow up run.

    Returns:
        A string of args to use on a command line.
    """
    next_args = []

    default_config_values_dict = default_config._asdict()
    for field_name, value in config._asdict().items():
        if default_config_values_dict[field_name] != value:
            field_type = getattr(config_field_types, field_name)
            flag_field_name = to_flag_field_name(field_name)
            if field_type == str:
                # TODO - validate that the strings don't have invalid chars.
                next_args.append("--%s %s" % (flag_field_name, value))
            elif field_type == bool:
                next_args.append("--%s%s" % ("" if value else "no-", flag_field_name))
            else:
                next_args.append("--unrecogized-type-%s-%s" % (str(field_type), flag_field_name))
    return ' '.join(next_args)
