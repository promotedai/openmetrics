from deploy.list_jobs_util import wait_for_flink_job
from deploy.save_jobs_util import create_modified_kubernetes_config, subprocess_kubernetes_delete_then_apply
from deploy.unexpected_value_error import UnexpectedValueError


def deploy_new_job(config):
    """Deploys a newly written job.  Ideally, this only gets called once per job.

    Args:
        config: The config.
    """
    print('\nCommand: deploy_new_job(%s)' % str(config))

    job_name = config.job_name
    if not config.new:
        raise UnexpectedValueError("Missing expected job=%s" % job_name)
    if not config.deploy:
        print('Skipping %s deploy since config.deploy is False' % job_name)
        return

    tmp_config_path = create_modified_kubernetes_config(config)
    dry_run = config.dry_run
    subprocess_kubernetes_delete_then_apply(config.namespace, tmp_config_path, dry_run)
    wait_for_flink_job(config.namespace, job_name, None, dry_run)
