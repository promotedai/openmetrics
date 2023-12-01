# Kafka

Contains scripts and notes for running Kafka locally.

## Setup

|**Command**|**Description**|
|-----------|---------------|
|`make local`|Setup local kafka|
|`make clean-local`|Clean|

## Develop with Kafka

### Running tools in Kubernetes

After running `make local`, the command will output useful commands to run a kafka-client pod.  This makes it easy to run other kafka scripts:
- `kafka-topics.sh --list --bootstrap-server  kafka.yournamespace.svc.cluster.local:9092`
- `kafka-console-consumer.sh --bootstrap-server kafka.yournamespace.svc.cluster.local:9092 --topic metrics.blue.default.cumulated-content-metrics --from-beginning --partition 0`

### Tool - clear topic

There are instructions on the web for clearing a topic by setting retention to 1 second.  This requires a purge to happen though (which might not happen for a while).  The most reliable way I found is the following

If you have already done steps 1-2 previously, you can skip them.

1. Change the kafka topics properties in kafka-*.yaml to support deletion.

   ```
   delete.topic.enable=true
   ```

2. Restart Kafka to pick up the new config/server.properties

3. Run this command

   ```
   kafka-topics.sh --zookeeper localhost:2181 --delete --topic metrics-realtime
   ```

4. Recreate the topic.

## Working in AWS

The steps are pretty complex.  Talk with Dan.  Make sure the MSK cluster has enough CPU and disk.  Rebalancing will consume more of both.

### Setup terminal

Run a container inside the EKS that has MSK access.  This example uses TPT.

1. Connect to AWS VPN.
2. Setup your terminal to contact K8s.

```bash
. ~/promotedai/infra-configs/scripts/metrics/tpt/prod/source-env.sh 
```

3. Run a pod in the EKS cluster.  Make sure to use an image version that matches the MSK Kafka version.

To keep the pod running:

```bash
kubectl --namespace $NAMESPACE run kafka-client --restart='Never' --image docker.io/bitnami/kafka:2.4.1 --command -- sleep infinity
kubectl exec --tty -i kafka-client --namespace $NAMESPACE -- bash
```

Or if you want to run just once:

```bash
kubectl --namespace $NAMESPACE run kafka --rm -i --tty --image bitnami/kafka:2.4.1 -- bash
```

### Setup Kafka scripts

1. Copy [AWS's CA certs](https://github.com/promotedai/metrics/blob/develop/api/main/awscert.go) over to a tmp file in the container.

```bash
echo '
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
' > /tmp/cacert
```

2. Setup Java Keystore files that contains the ssl certs.

```bash
cp /opt/bitnami/java/lib/security/cacerts /tmp/kafka.client.truststore.jks
keytool -keystore /tmp/kafka.client.keystore.jks -import -file /tmp/cacert -alias alias -storepass password -keypass password
```

3. Setup Kafka client properties.  This is needed to communicate with Kafka over SSL.

```bash
echo '                                                             
security.protocol=SSL
ssl.truststore.location=/tmp/kafka.client.truststore.jks
ssl.keystore.location=/tmp/kafka.client.keystore.jks
ssl.keystore.password=password
ssl.key.password=password' > /tmp/client.properties
```

4. Get the MSK Bootstrap Servers and Zookeeper URLs.  The easiest way is to visit the AWS MSK UI and click on `View client information`.

5. Make sure the Kafka script can contact the MSK Cluster.

You'll want to swap out the bootstrap server.

```bash
kafka-topics.sh --list --bootstrap-server b-9.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:9094,b-1.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:9094,b-6.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:9094   --command-config /tmp/client.properties 
```

This should return something like 

```bash
__amazon_msk_canary
__consumer_offsets
metrics.green.default.joined-event
tracking.event.log-request
```

### Rebalance

1. Verify you can ese the current partitions using `describe`.

```bash
kafka-topics.sh --describe --bootstrap-server b-9.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:9094,b-1.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:9094,b-6.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:9094   --command-config /tmp/client.properties --topic tracking.event.log-request
```

2. Generate a proposed reassignment. 

Create an input file for the next command.

```bash
echo '{"topics": [{"topic": "tracking.event.log-request"}], "version": 1}' > /tmp/topics.json
```

```bash
kafka-reassign-partitions.sh --zookeeper z-1.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:2181,z-2.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:2181,z-3.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:2181 --broker-list "1,2,3,4,5,6,7,8,9" --command-config /tmp/client.properties  --topics-to-move-json-file /tmp/topics.json --generate
```

This outputs...

```bash
Current partition replica assignment
{"version":1,"partitions":[...

Proposed partition reassignment configuration
{"version":1,"partitions":[...
```

Either (a) copy the proposed partitions JSON into a file or (b) produce your own proposed partitions.  The proposed output might rebalance all partitions.  If you want to make this more efficient, you can override the partition mapping to just rebalance the parts that you want.

```bash
echo '{"version":1,"partitions":[...' > /tmp/reassignment-file.json
```

4. Run the reassignment command.  This will rebalance asynchronously.  It can take a hours to finish.

```bash
kafka-reassign-partitions.sh --zookeeper z-1.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:2181,z-2.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:2181,z-3.tptprodmetrics.wm1fi5.c23.kafka.us-east-1.amazonaws.com:2181 --command-config /tmp/client.properties  --reassignment-json-file /tmp/reassignment-file.json --execute
```

