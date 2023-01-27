# Deploy to Kubernetes

## Overall push instructions

The overall push process is pretty standard:
1. Deploy to the appropriate dev environments by following the steps in the next section.
2. Look for breaks, errors and exceptions in dev logs.  Generally, verify all jobs are healthy per environment.
3. Make sure the main binary is up-to-date.
4. Verify the appropriate canary parameters are still good.
5. Deploy to the appropriate canary environments.
6. Look for breaks, errors and exceptions in canary logs.  Generally, verify all jobs are healthy per environment.
7. Once the canary jobs are running "at head" (i.e. not backfilling) based on watermarks, validate the canary job correctness by following the [canary validation notebook instructions](https://github.com/promotedai/notebooks/tree/main/metrics)
8. Verify the prod parameters are still good.
9. If green-lit for prod launch, deploy to production environments.
10. Look for breaks, errors and exceptions in prod logs.  Generally, verify all jobs are healthy per environment.

## Deploying the stream jobs incrementally to an AWS environment

### Summary:

- Connect to EKS.
- Run the deploy scripts to do incremental deploys.

### Backfills

When running backfills:
- Join job - Increase `--watermarkIdlenessProcessingTimeTrailingDuration` to a large time (e.g. `PT6H`) to deal with slowdowns during backfill.  Make sure to correct it when deployed.
- Counter job
  - The counter job waits until the start process time to write to Redis (so it's a little safer when backfilling).
  - Be careful using `--wipe`.  Wipe gets run at the start of the counter job.  If Delivery is actively using the Counter Redis, this will impact production. 

# Script to deploy Flink jobs

## Summary

This Python script will update a single Flink job per invocation.  It will savepoint the jobs, then `kubectl delete` and `kubectl apply` a modified config to restart that job.  It will deploy new jobs if the `--new` flag is set.

## Warnings and Debugging

This is a v1 script.  It has very few safe guards.  If an issue is hit, there is a large chance that we will stop running a job in production.  These operations are not done as transactions.  There are no automatic rollbacks.  We would need to switch to blue-green deployments to get safe rollouts.

This script will break.  The script stdouts important debug info and commands for continuing the sub-deploy after fixing issues.  It's hard to build recovery into this script.  We'll let the script fail and the developer needs to fix the state and improve the script.

There are no backups of the state and text logs.  If we lose the execution logs, it's hard to figure out what happened.  You'll need to look at the Flink UI or, possibly, Zookeeper to get the S3 savepoint path.

## File structure

- The main function is in `deploy_flink_job.py.
- The utilities are in `deploy`.  They're broken down by new vs update and list vs save.
- The tests are in `tests`.  Each function mocks out the sub-functions.  Most of the hard logic is done in `subprocess.run` calls which are mocked out.

## Run instructions

### Setup

The script needs the terminal to be configured to connect to a specific environment.

1. [VPN into our aws network](https://github.com/promotedai/workstation#vpn-access).  For this example, we'll update bbb-dev and VPN into bbb-dev.

2. Run the update mfa AWS env variable script.

   ```
   source ~/promotedai/workstation/utils/mfa.sh
   ```

3. Make sure your `KUBECONFIG` and `KUBE_CONFIG_PATH` point to the same k8s config file for the appropriate environment:
   ```
   export KUBECONFIG=~/.kube/bbb-dev;
   export KUBE_CONFIG_PATH=~/.kube/bbb-dev
   ```
   1. `brew install awscli` if you don’t have the aws command line
   2. if you have permission issues, use terraform RBAC controls to set yourself up
   3. ensure you have an aws token by sourcing [workstation/utils/mfa.sh](https://github.com/promotedai/workstation/blob/master/utils/mfa.sh)
   4. `aws eks --region <region code e.g. "us-east-1"> update-kubeconfig --name <cluster name e.g. "bbb-dev">`

4. cd into the deploy directory.

   ```
   cd ~/promotedai/metrics/pipeline/scripts/deploy
   ```

### Deploy

1. Restart the Flink TaskManagers.  We hit an issue (probably perm gen related) when deploying.  Restarting increases the chance of success.

   ```
   kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep taskmanager | while read POD; do
     kubectl delete pod/$POD &
   done
   ```

2. Run the script for each job.

   ```
   RAW_K8S_YAML=~/promotedai/metrics/pipeline/kubernetes/bbb-dev/raw-output-job.yaml
   pipenv run python3 deploy_flink_job.py --job-name "log-user" --stream-job-file $RAW_K8S_YAML --sub-job-type RAW_LOG_USER
   ...
   ```

   Dan has local deploy scripts to deploy to each environment.

## Run tests

```
make test
```

## Deploying the stream job in a way that is not incremental

<details>
  <summary>Do not do this unless you know what you are doing.</summary>

  This is complex because writes to our sinks (S3 and Kafka) are not idempotent.  These steps are vague and likely stale to discourage doing this.  This really only replaces the final deploy to production step.  Please verify canary first.

  1. Stop the current Flink jobs (with a savepoint).  We want a savepoint in case we hit an error starting up the new version.  Follow the instructions in the previous step.
  1. Restart each Flink Task Manager pod.  This increases the chance of success.  
  1. Backup and clear S3 and Kafka data.
      1. Create a new set of Kafka topics or empty the kafka topic.  An easy way of doing this is to delete the kafka_topic using Terraform and recreate it again.  You'll need to make sure `msk_delete_topic_enable=true`.  If you do this, make sure consumers of the Kafka topics continue to work correctly afterwards.
  1. Verify the job parameters are correct.  E.g. `--startFromEarliest`.
  1. Run the deploy script with an additional arg `--new`.
  1. Run the job.  Wait for the data to catch up.
  1. Verify outputs.  If this is done for a canary deploy, the canary checks can be used.  Dan Hill does this by looking at the S3 outputs and comparing against what is expected for the deployed changes.
  1. Fix the downstream consumers.

</details>

## Disaster recovery

If we lose most of the EKS environment (no Flink logs, no Zookeeper logs), then try this:
1. Try to find the jobIds for the latest job in the deploy script logs.  Example: `Found new job d26a1e2e166725f4d5cccce1592aa281`.  Make sure to use the latest jobId, not earlier ones.  Doublecheck the latest jobIds from the deploy script with the jobIds returned from the command in step 2.  If you do not have access to the latest Flink deploy script result use the command in step 2 to find recent metadata files and inspect them.  They contain state information that can be used to diagnose the job.
2. Look for the latest, completed checkpoint (by checkpoint number) using this script `aws s3 ls s3://ccc-prod-metrics-flink-state/checkpoints/ --recursive | grep "meta"`.  Verify that this is a recent timestamp (in case an old jobId was used).
3. Manually edit the deploy script to `start-from-savepoint` using these checkpoints.  The checkpoint path includes the ending `.../chk-123`.
