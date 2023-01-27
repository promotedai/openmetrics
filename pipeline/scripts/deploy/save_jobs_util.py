import tempfile
import json
import time
from deploy.list_jobs_util import is_stopped
from deploy.subprocess_run_util import subprocess_run
from deploy.unexpected_value_error import UnexpectedValueError


def subprocess_kubernetes_delete_then_apply(namespace, config_path, dry_run):
    """Calls subprocess.run to delete a Kubernetes job and then calls run again to apply the job.
    This is a separate method so we can easily mock it out.

    Args:
        config_path: Path to a Kubernetes config that creates a Flink stream job.
        dry_run: Whether this is a dry run.
    """
    cmd = "kubectl delete -n %s -f %s; kubectl apply -n %s -f %s" % (namespace, config_path, namespace, config_path)
    if dry_run:
        print("dryrun - cmd=%s" % cmd)
        return
    kubectl_apply = subprocess_run(
        command=cmd,
        # Assume it's not retry for now.
        retry=False)
    if kubectl_apply.returncode != 0:
        raise UnexpectedValueError("Error with kubectl apply=%s" % kubectl_apply.stdout)


def async_stop_job(namespace, job_id, dry_run):
    """Calls Flink REST API to stop a job.
    This is a separate method so we can easily mock it out.

    Args:
        job_id: The Flink job ID.
        dry_run: Whether this is a dry run.

    Returns:
        The request-id for the stop/savepoint operation.  Can return None in dry run mode.
    """
    cmd = "kubectl exec -n %s -i -t flink-jobmanager-0 -- curl -d " \
        "'{\"drain\":false}' -H \"Content-Type: application/json\"" \
        " -X POST http://localhost:8081/jobs/%s/stop" % (namespace, job_id)
    if dry_run:
        print("dry_run - cmd=%s" % cmd)
        return None
    stop_job_result = subprocess_run(cmd)
    if stop_job_result.returncode != 0:
        raise UnexpectedValueError("Error with stop_job(\"%s\")=%s" % (job_id, stop_job_result.stdout))

    stop_job_result_json = json.loads(stop_job_result.stdout)
    request_id = stop_job_result_json.get("request-id", "")
    if request_id:
        return request_id
    else:
        raise UnexpectedValueError("Unrecognized stop response, job_id=%s, response=%s"
                                   % (job_id, stop_job_result.stdout))


def wait_for_savepoint(namespace, job_id, request_id):
    """Waits for a Flink job to have a valid savepoint.  This is a separate method so we can easily mock it out.

    Args:
        job_id: The Flink job ID.
        request_id: The request ID for the stop/savepoint.
    Returns:
        The savepoint.
    """
    print("\nCommand: wait_for_savepoint('%s', '%s', '%s')" % (namespace, job_id, request_id))
    # Max ~60 minutes.  Roughly 1 hour / 15s with some buffer while timeout ramps up.
    for i in range(1, 250):
        result_json = get_savepoint_summary(namespace, job_id, request_id)
        status = result_json.get("status", {}).get("id", "")
        if status == "COMPLETED":
            s3_savepoint_path = result_json.get("operation", {}).get("location", "")
            if s3_savepoint_path:
                return s3_savepoint_path
            else:
                raise UnexpectedValueError("Expected savepoint location, job_id=%s, result_json=%s"
                                           % (job_id, result_json))
        elif is_stopped(status):
            raise UnexpectedValueError("Expected job status when stopping, job_id=%s, status=%s"
                                       % (job_id, status))

        # Only print every few iterations.
        sleep_seconds = get_sleep_seconds_for_i(i)
        # Do not print out for each iteration.
        if i % 5 == 1:
            print("Waiting for savepoint. Sleeping ...")
            print(result_json)
        time.sleep(sleep_seconds)

    raise UnexpectedValueError("Expected job to savepoint within 10 minutes, job_id=%s"
                               % job_id)


def get_sleep_seconds_for_i(i):
    if i <= 5:
        return 1
    elif i <= 10:
        return 5
    else:
        return 15


def get_savepoint_summary(namespace, job_id, request_id):
    """Gets the Flink REST API response for getting a savepoint.
    This is a separate method so we can easily mock it out.

    Args:
        job_id: The Flink job ID.
        request_id: The request ID for the stop/savepoint.

    Returns:
        The savepoint.
    """
    savepoint_details_result = subprocess_run("kubectl exec -n %s -i -t flink-jobmanager-0 -- "
                                              "curl http://localhost:8081/jobs/%s/savepoints/%s"
                                              % (namespace, job_id, request_id))
    if savepoint_details_result.returncode != 0:
        raise UnexpectedValueError("Error with wait_for_savepoint(\"%s\", \"%s\")=%s"
                                   % (namespace, job_id, savepoint_details_result.stdout))
    return json.loads(savepoint_details_result.stdout)


def create_modified_kubernetes_config(config):
    """Takes the Flink stream job Kubernetes config (config.stream_job_file),
    copies it and changes the config to support running from a redeploy.

    Args:
        config: The config for this job.

    Returns:
        The path to the new, temp Kubernetes config.
    """

    stream_job_file = config.stream_job_file
    sub_job_type = config.sub_job_type
    flink_run_args = []
    s3_savepoint_path = config.start_from_savepoint
    if s3_savepoint_path:
        flink_run_args.append('"-s", "%s"' % s3_savepoint_path)
    if config.allow_non_restored_state:
        flink_run_args.append('"--allowNonRestoredState"')

    with open(stream_job_file, "r") as f:
        k8s_file = f.read()
        # TODO - fail the call if we do not find == 1 versions of the strings.
        if flink_run_args:
            k8s_file = k8s_file.replace(
                'command: ["flink", "run", ',
                'command: ["flink", "run", %s, ' % ', '.join(flink_run_args))
        additional_job_args_string = ''
        if sub_job_type:
            additional_job_args_string = '"--subJob=%s",' % sub_job_type
        k8s_file = k8s_file.replace(
            'args: [',
            'args: [%s' % additional_job_args_string)

        fp = tempfile.NamedTemporaryFile(delete=False)
        with open(fp.name, 'w') as f:
            f.write(k8s_file)
        return fp.name
