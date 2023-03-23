import os
from deploy.config import default_config, get_source_namespace
from deploy.deploy_new_job import deploy_new_job
from deploy.deploy_updated_job import create_job_from_savepoint, update_existing_job
from deploy.list_jobs_util import get_job_name_to_latest_jobs, is_stopped
from deploy.unexpected_value_error import UnexpectedValueError

# TODO - detect and handle duplicates.
# TODO - a version of this that writes out the intermediate state to a file.


def validate_config(config):
    validate_stream_job_file_arg(config.stream_job_file)


def validate_stream_job_file_arg(stream_job_file):
    """Validates the command arguments.

    Args:
        stream_job_file: Path to the stream job Kubernetes config.
    """
    if stream_job_file == "":
        raise UnexpectedValueError("stream_job_file needs to be set")
    if not os.path.isfile(stream_job_file):
        raise UnexpectedValueError("stream_job_file needs to be a file")


def deploy_job(config):
    """Deploys a single job type.

    Args:
        config: The config.

    Returns:
        A continuation config.  Can be used to restart the job.
    """
    job_name = config.job_name
    print("\n## deploy_job - %s" % job_name)
    print('\nCommand: deploy_job(%s)' % (str(config)))
    # Refetch all jobs so we can get the latest state.
    job_name_to_latest_job = get_job_name_to_latest_jobs(get_source_namespace(config))
    job = job_name_to_latest_job.get(job_name, None)

    # TODO - what about combining start_from_savepoint with new=False or deploy=True?

    # TODO - if "new=True" is set but the job already exists.  Do not create brand new jobs.

    # not config.deploy handled inside deploy_new_job and update_existing_job.
    s3_savepoint_path = None
    if config.start_from_savepoint is not None:
        # TODO - improve the logging message between stop and deploy to indicate how bad the error is.
        create_job_from_savepoint(None, config)
    elif job is None or (is_stopped(job["state"]) and config.create_new_if_stopped):
        # TODO - output a warning if the job was not successful.

        # Checks are done internally to make it easier to redo.
        deploy_new_job(config)
    else:
        s3_savepoint_path = update_existing_job(job, config)
    return create_continuation_config(config, s3_savepoint_path)


# TODO - do not create a continuation config if the cont config is not needed.
def create_continuation_config(config, s3_savepoint_path):
    continuation_config = config
    if not continuation_config.deploy:
        # The continuation config should deploy from the savepoint.  Remove the deploy=False.
        continuation_config = continuation_config._replace(
            deploy=default_config.deploy, start_from_savepoint=s3_savepoint_path)
    return continuation_config
