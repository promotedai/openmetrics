import json
import time
from deploy.subprocess_run_util import subprocess_run
from deploy.unexpected_value_error import UnexpectedValueError


# A set of the currently supported Flink job state.
# We check to make sure state is in this list in order to reduce the chance of bugs.
supported_states = {
    "INITIALIZING",
    "CREATED",
    "RUNNING",
    "FAILING",
    "FAILED",
    "CANCELLING",
    "CANCELED",
    "FINISHED",
    "RESTARTING",
    "SUSPENDED",
    "RECONCILING"
}


def is_successfully_stopped(state):
    """Returns a boolean for if the Flink job state was successfully stopped.
    Dan doesn't think FAILED is a safe failure condition.
    """
    # TODO - SUSPENDED?
    return state == "CANCELED" or state == "FINISHED"


def is_stopped(state):
    """Returns a boolean for if the Flink job state is stopped.
    """
    # TODO - SUSPENDED?
    return state == "FAILED" or state == "CANCELED" or state == "FINISHED"


def get_job_name_to_latest_jobs(namespace):
    """Returns list of the latest Flink jobs (by name) in the Flink REST API JSON format.
    """
    get_jobs = subprocess_run(
        command="kubectl exec -n %s -i -t flink-jobmanager-0 -- curl http://localhost:8081/jobs/overview" % namespace,
        retry=True)

    if get_jobs.returncode != 0:
        raise UnexpectedValueError("Error with get_jobs=%s" % get_jobs.stdout)

    get_jobs_json = json.loads(get_jobs.stdout)
    jobs = get_jobs_json["jobs"]

    # Convert the jobs to a dict.  Keep the latest job.
    job_name_to_latest_job = {}
    for job in jobs:
        job_name = job["name"]
        job_state = job["state"]
        if job_state not in supported_states:
            raise UnexpectedValueError("Encountered unexpected job state=%s" % job_state)

        latest_job = job_name_to_latest_job.get(job_name, None)
        if latest_job is None:
            job_name_to_latest_job[job_name] = job
        else:
            # Pick the latest one except in the case where the later is finished and the current one is not.
            if job["start-time"] > latest_job["start-time"] and \
                    (not is_stopped(job["state"]) or is_stopped(latest_job["state"])):
                job_name_to_latest_job[job_name] = job

    # TODO - filter out FINISHED jobs.

    return job_name_to_latest_job


def wait_for_flink_job(namespace, job_name, old_job_id, dry_run):
    """Waits for a Flink job to be running.

    Args:
        job_name: The job name.
        old_job_id: The ID for the old flink job.  Can be None if no previous job.
        dry_run: Whether this is a dry run.
    """
    new_job_id = None
    print("")
    if dry_run:
        print("dryrun - wait_for_flink_job - skipping")
        return

    for i in range(1, 24):
        job_name_to_latest_job = get_job_name_to_latest_jobs(namespace)
        job = job_name_to_latest_job.get(job_name, {})
        # TODO - check edge conditions.
        tmp_job_id = job.get("jid", None)
        if (tmp_job_id is not None) and (tmp_job_id != old_job_id) and not is_stopped(job["state"]):
            new_job_id = tmp_job_id
            print("Found new job %s" % new_job_id)
            break
        else:
            print("Waiting for new job. Sleeping 5s...")
            time.sleep(5)

    if new_job_id is None:
        raise UnexpectedValueError("Expected job to start within 2 minutes, previous_job_id=%s, job_name=%s"
                                   % (old_job_id, job_name))

    # TODO - wait for the job to be successful for X minutes.
    # Use curl to see the state.
    # kubectl exec -i -t flink-jobmanager-0  -- curl localhost:8081/jobs/19fe13867f49fdff4f9bb69c6ec04910
