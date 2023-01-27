from deploy.list_jobs_util import wait_for_flink_job
from deploy.save_jobs_util import async_stop_job, create_modified_kubernetes_config
from deploy.save_jobs_util import subprocess_kubernetes_delete_then_apply, wait_for_savepoint
from deploy.unexpected_value_error import UnexpectedValueError


def update_existing_job(job, config):
    """Deploys an update to an existing job.  This will stop (and savepoint) the job and deploy the latest version
    using the savepoint.

    Args:
        job: The dict Job from Flink REST API.
        config: The config

    Returns:
        s3_savepoint_path
    """
    job_state = job["state"]
    if job_state != "RUNNING" and job_state != "RESTARTING":
        message = "Unexpected job_state=%s" % job_state
        if job_state == "CANCELED":
            message = message + ". To launch anyways, change config create_new_if_stopped to True."
        # TODO - support the case where jobs are not started, failing, canceling, etc.
        raise UnexpectedValueError(message)

    # TODO - Detect if the job is regularly restarting.  Make sure the job has been running well for at least X seconds.

    job_id = job["jid"]
    s3_savepoint_path = stop_job(config.namespace, job_id, config.dry_run)

    # TODO - improve the logging message between stop and deploy to indicat ehow bad the error is.
    copy_config = config._replace(start_from_savepoint=s3_savepoint_path)
    create_job_from_savepoint(job_id, copy_config)
    return s3_savepoint_path


def stop_job(namespace, job_id, dry_run):
    """Calls subprocess.run to stop a Flink stream job.  This is a separate method so we can easily mock it out.

    Args:
        namespace: the K8s namespace.
        job_id: the Flink job ID.
        dry_run: Whether to run as a dry run.

    Returns:
        The s3 savepoint path.  Returns a fake one for dry run.
    """
    print("Command: stop_job('%s')" % job_id)
    stop_job_request_id = async_stop_job(namespace, job_id, dry_run)
    if dry_run:
        print('dryrun - skipping wait_for_savepoint')
        return 's3://fake-path/for/dry/run'
    else:
        s3_savepoint_path = wait_for_savepoint(namespace, job_id, stop_job_request_id)
        print('Stopped job %s to %s' % (job_id, s3_savepoint_path))
        return s3_savepoint_path


def create_job_from_savepoint(old_job_id, config):
    """Deploy a job from a savepoint.  This involves:
    1. Modifying the k8 config.
    2. Deleting and applying the K8 config.
    3. Waiting for the job to be created.  Waits for old_job_id if not None.

    Args:
        job_id: The job ID.  None means no previous job_id.
        config: The Config.
    """
    print('\nCommand: create_job_from_savepoint("%s", %s)'
          % (old_job_id, config))
    job_name = config.job_name
    if not config.deploy:
        print('Skipping %s deploy since config.deploy is False' % job_name)
        return

    tmp_config_path = create_modified_kubernetes_config(config)

    dry_run = config.dry_run
    subprocess_kubernetes_delete_then_apply(config.namespace, tmp_config_path, dry_run)
    wait_for_flink_job(config.namespace, job_name, old_job_id, dry_run)
