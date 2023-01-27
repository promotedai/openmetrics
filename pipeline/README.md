# Overview

The "big picture" goals of the Flink pipeline is best illustrated in the [Metrics Solution diagrams](https://drive.google.com/drive/folders/1wtHCBk3hGdr7vbWi2kD42zc4FDnpRrPV) and the `Detailed Diagrams` subfolder.  Some notable diagrams are:
* [System Overview - Presentation](https://drive.google.com/file/d/19mwOiuxUji-T_Zxqcr4J2NDgZXHypmd6/view?usp=sharing) - (intended) general flow of data between the major systems at promoted.ai
* [Metrics Solution](https://drive.google.com/file/d/1IqrMIY-S8hcsOp7bUnTAHhUgBy1SaJre/view?usp=sharing) - illustrates the metrics use cases
* [Detailed Diagrams/Metrics](https://drive.google.com/file/d/1TI9C0e-2B9qwLQDlyQqRuclLRxzko611/view?usp=sharing) - service level diagram of metrics stack components and jobs

There are currently three streaming jobs:
1. Raw Output Job - Save raw log records to S3.
2. Flat Output Job - Join log records and save these flattened (denormalized) log records in Kafka and S3.
3. Counter Job - Count impression and actions over various segments for delivery.
4. Content Metrics Job - Produces ETL tables that contain aggregate metrics by contentId.

More details about these jobs can be found at [src/main/java/ai/promoted/metrics/logprocessor/stream/](src/main/java/ai/promoted/metrics/logprocessor/stream/README.md).

## Why Flink?

We need a real-time data processing system that can scale (>1B log records per day per marketplace) and has fast end-to-end latency (on the order of seconds).  Flink is the leading open source solution for solving this problem.  It's designed for (locally) stateful streaming.  It scales better than current alternatives.

# Setup

## Requirements

* You need to setup Kubernetes (k8s) via minikube for local development.
* Kafka running on k8s.  Please see `//kafka/README.md` for details on how to set Kafka up.
* MinIO is an S3-compatible/replacement service we can run locally via minikube.  We use this so we can run the full stack locally.

## Setup the local development dependencies.

See the [workstation setup docs](https://github.com/promotedai/workstation/blob/master/local-env.md).


## Setup Flink pipeline locally.

You need to start Kafka with the required topics.  The easiest way to do that is running `make local` in `//kafka`.

To start the Flink stack

|**Command**|**Description**|
|-----------|---------------|
|`make local`|Setup Flink pods, MinIO and the streaming job|
|`make clean-local`|Cleans all jobs created in this directory|

If you are iterating on parts of the stack, you can run one of the following commands.  If you are iterating on just a single job (say `foo`), you can run `make local-foo-clean && make local-foo-setup`.

|**Command**|**Description**|
|-----------|---------------|
|`make local-minio-setup`|Setup MinIO|
|`make local-minio-clean`|Tear down MinIO|
|`make local-redis-setup`|Setup Redis|
|`make local-redis-clean`|Tear down Redis|
|`make local-flink-setup`|Setup Flink pods and all jobs|
|`make local-flink-clean`|Tear down Flink and all jobs|
|`make local-fake-log-generator-setup`|Deploy a job that writes fake log records to Kafka.  Writes to Kafka|
|`make local-fake-log-generator-clean`|Deletes the k8s config|
|`make local-fake-content-generator-setup`|Deploy a job that writes fake content records to Content API.|
|`make local-fake-content-generator-clean`|Deletes the k8s config|
|`make local-raw-output-job-setup`|Deploy the raw output jobs|
|`make local-raw-output-job-clean`|Stops the current raw output jobs and deletes the k8s config|
|`make local-flat-output-job-setup`|Deploy the flat output job|
|`make local-flat-output-job-clean`|Stops the current flat output job and deletes the k8s config|
|`make local-counter-job-setup`|Deploy the counter job|
|`make local-counter-job-clean`|Stops the current counter job and deletes the k8s config|

# Development

## Conventions

### Operator uid

We disable autogeneration of Flink uids because they regularly create forwards incompatible releases.  We enforce some conventions in unit tests, but rely on devs to adhere to them.  Defer to the conventions outlined in [UidChecker](https://github.com/promotedai/metrics/blob/develop/pipeline/src/test/java/ai/promoted/metrics/logprocessor/stream/UidChecker.java) if there's disagreement between here and there.

General convetions should be:
1. no spaces or silly casing; just lowercased and hyphens
2. verb -> nouns
3. specific -> general
4. use the uid as the name of the operator (and generally keep related outputs similarly named)

For example, `count-hourly-item-event`, `join-impression-action`, and `timestamp-and-watermark-source-kafka-view` are appropriate.

## Set Up Environment Variables
```
# generally do this whenever you're about to run kubectl commands for local development below
. ~/promotedai/metrics/config/config.sh; export NAMESPACE=$LOCAL_K8S_NAMESPACE; eval $(minikube docker-env)
```

## Sending fake log data

Run `make local-fake-log-generator-setup` to send fake input log records to Kafka.

The default join job and fake data generator outputs ~681 flat-impressions.  Even though there are 700 raw impressions, we drop some due to a combination of using fake data for inferred references, a smaller range of contentId and a setting to reduce redundant impressions.

## Joining in Content data

The Join job joins content from Content API in order to get additional foreign content keys.  These keys are used for attribution when actions have different content types than the insertions.  E.g. insertion for a store but purchase on an item.

### Content joining in production

In production, the Join job will use the Content API.  It'll get the api key from the AWS Secrets Manager.

## Content joining when developing locally

The default local setup does not join in Content details from Content API.  Here are rough steps from Dan for doing this locally:
1. Run [`contentstoreapi`](https://github.com/promotedai/contentstoreapi) locally outside of Minikube.  You'll need to start a standalone MongoDB instance.
2. Run `ngrok http 5150` for `contentstoreapi`.  It's easier to have the Minikube Flink Join job hit the ngrok URL. than using other addresses.
3. Modify the following k8s yamls to use the content store flags: `--contentApiRootUrl` and `--contentApiKey`.
  - `pipeline/kubernetes/local/fake-content-generator.yaml`
  - `pipeline/kubernetes/local/flat-output-job.yaml`
4. Write fake content data to the local Content API.

  ```bash
  make local-fake-content-generator-clean && make local-fake-content-generator-setup
  ```
5. Startup the Join job.

  ```bash
  make local-flat-output-job-clean && make local-flat-output-job-setup
  ```

## Connect to Flink UI
```
# either of the below commands should work, so just pick one
kubectl -n ${NAMESPACE} port-forward flink-jobmanager-0 8081:8081
kubectl -n ${NAMESPACE} port-forward $(kubectl -n ${NAMESPACE} get pods -l "app.kubernetes.io/name=flink,app.kubernetes.io/instance=flink" -o jsonpath="{.items[0].metadata.name}") 8081:8081

```

Then visit http://localhost:8081 to load the Flink UI.

## Connect to MinIO UI.
```
kubectl -n ${NAMESPACE} port-forward $(kubectl -n ${NAMESPACE} get pods -l "release=minio" -o jsonpath="{.items[0].metadata.name}") 9000:9000
```

When you load the UI (http://localhost:9000), you will be asked for access keys.  We use the default keys since we are only using MinIO locally:
* accessKey=`YOURACCESSKEY`
* secretKey=`YOURSECRETKEY`

### MinIO CLI Client
Once you've port forwarded MinIO on port 9000, you can install the `mc` command line utility:

1. `brew install minio-mc`
2. `mc alias set local http://localhost:9000 YOURACCESSKEY YOURSECRETKEY`

Hereafter, you can use `mc` commands to query `local/` to manipulate files via command line.  Remember, you need to have MinIO port-forwarded to have these work.

#### Copy an entire subfolder from minio
```
# here, we're copying everything under etl
mc cp --recursive local/promoted-event-logs/etl .
```

#### Remove all files from the default minio local path
```
mc rm -r --force local/promoted-event-logs
```

## Connect to Redis.
```
kubectl -n ${NAMESPACE} port-forward svc/redis-master 6379
redis-cli -h localhost -p 6379
```

## Java profiling via JMX
1. `brew install visualvm` or whatever floats your JMX boat
2. Edit `kubernetes/local/flink-values.yaml` and uncomment the `flink.params.env.java.opts.taskmanager` value to enable the JMX server within our binaries.  Note: you can do this for canary and production jobs as well if you need to.
3. You can connect to the flink taskmanager at anytime, so it’s suggested to do so before starting up any actual pipeline jobs in case you want to catch the runtime characteristics of startup and/or early execution.
4. Simply port forward the default jmx port (1099):
   ```
   kubectl -n ${NAMESPACE} port-forward flink-taskmanager-0 1099
   ```
5. fire up visualvm and make a local connection to 1099
   1. Add JMX Connection (toolbar or right-click on Local)
   2. Connection: `localhost:1099`
   3. Make sure `Do not require SSL connection` is unchecked
   4. [optional] uncheck “Connect automatically”.  You can manually connect and disconnect by right clicking on the `Application` entry on subsequent runs.


## More Useful Tips

Many of these are just docker-compose helper commands.

### Log fetching
There are a few ways to get logs.  The direct `kubectl logs` might not give the job logs you want.  The job's logs are in a `/opt/flink/log/*.out`.

```
# fetches the job population logs (hint: execution plans are output here if coded to do so)
kubectl -n ${NAMESPACE} logs flink-taskmanager-0
# lists all logs available in the local flink taskmanager
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- ls /opt/flink/log
# grabs the last 100 lines of the log file of a local flink taskmanager
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- tail -n 100 /opt/flink/log/flink--taskexecutor-0-flink-taskmanager-0.log
# grabs the last 100 lines of the stdout of a local flink taskmanager
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- tail -n 100 /opt/flink/log/flink--taskexecutor-0-flink-taskmanager-0.out
```

### Kafka topic inspection
You need to run kafka-testclient first.
```
# dumps the tmp-output topic to stdout
kubectl -n ${NAMESPACE} exec -ti kafka-testclient -- kafka-console-consumer --bootstrap-server kafka:9092 --topic tmp-output --from-beginning
```

### Hit the Flink REST API
Assumes you've exposed the flink jobmanager's 8081 port via [port forwarding](#connect-to-flink-ui).
```
# lists all jobs
curl localhost:8081/jobs
# note: use a job-id fetched from the /jobs get
curl localhost:8081/jobs/<jod-id>
curl "localhost:8081/jobs/<jod-id>/metrics?get=lastCheckpointSize"
```

### Run Flink command line
```
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- flink --help
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- flink list
```

### Stopping a Flink job with a savepoint
```
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- flink stop <job-id>
```

### Cancelling a Flink job without a savepoint
*WARNING:* ungracefully exits!  You should probably use stop instead.
```
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- flink cancel <job-id>
```

### Trigger a savepoint manually
```
kubectl -n ${NAMESPACE} exec -ti flink-taskmanager-0 -- flink savepoint <job-id>
```

### Restart using a savepoint
Assumes you've cancelled the job with a savepoint.

1. Look for the savepoint in minio and note the path.
2. Edit `kubernetes/local/<foo>-job.yaml` and follow the directions about using the `-s <savepoint path>` flag.
3. `make local-<foo>-clean && make local-<foo>-setup`

### Run Flink with more parallelism.
1. Add a `-p "2"` in `kubernetes/local/<foo>-job.yaml`
2. Override or change `taskmanager.replicaCount` in `kubernetes/local/flink-values.yaml` from `1` to `2`.
3. `kubectl -n ${NAMESPACE} delete flink-taskmanager-0`
4. Wait for a new taskmanager to start automatically via helm.
5. Re-create the job: `make local-<foo>-setup`

### Manual throughput testing

Add the following to `kubernetes/local/flink-values.yaml` under `flink.params`.  This will print metrics to the text logs.

```
metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
metrics.reporter.slf4j.interval: 60 SECONDS
```

To filter down the values, here's an example query

```bash
kubectl -n ${NAMESPACE} exec pod/flink-taskmanager-0 -- cat log/flink--taskexecutor-0-flink-taskmanager-0.log  | grep "numRecordsOut" | grep "join-event" | grep "Join insertion to impressions.0" | sed "s/^.*numRecordsOut/numRecordsOut/"
```

This gives records like
```
numRecordsOutPerSecond: 0.0
numRecordsOut: 0
numRecordsOutPerSecond: 0.0
numRecordsOut: 416
numRecordsOutPerSecond: 5.133333333333334
numRecordsOut: 1400
numRecordsOutPerSecond: 18.2
numRecordsOut: 1400
numRecordsOutPerSecond: 0.0
numRecordsOut: 1400
```

# Deploy

Notes are in the [deploy script README](pipeline/scripts/deploy/README.md).

# Testing

Notes are in [tests.md](tests.md).

# Operations

## Lessons learned from backfilling data

The most impactful on backfill throughput:
- Performance of underlying Flink code. Example: `KeyedStream.intervalJoin` slows down exponentially with duplicate event IDs.  Understanding the underlying execution allowed us to actually finish a backfill.

Helps increase backfill throughput:
- Increase memory. Flink memory settings are a little tricky to get right. `memoryProcessSize` does not represent all of the memory that Flink allocates (even though the documentation says it is).  RocksDB can allocate extra memory.
- Reducing checkpoint intervals. Checkpoints are meant to help recovery if there is a disaster.  Checkpoints take time.
- Increase savepoint timeouts.
- Reducing number of running tasks and slots on the task manager.

Did not help:
- Disk size.

# Resources

* [Apache Flink](https://flink.apache.org/)
* [Apache Kafka](https://kafka.apache.org/)
* [Kubernetes](https://kubernetes.io/)
  * [Amazon Elastic Kubernetes Service (EKS)](https://aws.amazon.com/eks/)
  * locally, [Minikube](https://minikube.sigs.k8s.io/) and [Docker](https://www.docker.com/)
* [MinIO](https://min.io/)
