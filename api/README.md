# Summary

The Event API takes in user event log record and writes them to Kafka.

# Important

Here are developer notes that could lead to bad bugs if a developer is unfamiliar with the system.

# Develop

Follow instructions in `workstation` repo for how to create a repo.  Setup Kafka using instructions in `kafka/`.

## Run locally once

This runs the event at main/event.json in Kubernetes.  You must set the env variable `NAMESPACE` to match your K8 namespace.

```
NAMESPACE={your K8 namespace} make local-invoke
```
or
```
NAMESPACE={your K8 namespace} make local-invoke event=main/event.json
```

## Run locally (keep running)

Useful when developing services that need Event API running.

This will start a version on the current Kubernetes instance with the `config/local.json` parameters.

```
NAMESPACE={your K8 namespace} make local-keep-running
```

### Send a request - Portforward then curl

    ```
    kubectl port-forward -n metrics-local event-api 9001:9001
    ```
    Then
    ```
    curl -d '{ "body": "{\"userInfo\":{\"userId\": \"1\"}, \"impression\": [{ \"insertionId\": \"b59b77c7-a55a-4f01-91b5-58a3d4e36876\" }]}" }'  http:/localhost:9001/2015-03-31/functions/main/invocations
    ```

### Send a request - Exec on pod

    ```
    kubectl -n ${NAMESPACE} exec -ti event-api -- curl -d '{ "body": "{\"platformId\": \"1\", \"userInfo\":{\"userId\": \"1\"}, \"impression\": [{ \"insertionId\": \"b59b77c7-a55a-4f01-91b5-58a3d4e36876\" }]}" }'  http:/localhost:9001/2015-03-31/functions/main/invocations
    ```

## Bazel Dependencies

In Makefile, build will rerun gomodgen which will re-run gazelle update-repos to auto-import new repositories.  This could be confusing if people are not expecting.

If you want to update the repos, you can run.
```
bazel run //:gazelle -- update-repos <repository>
```

If you want to auto-generate more of the build files, you can use update or fix.
```
bazel run //:gazelle -- update
```

## Run tests.

```
make test
```

or

```
make testwatch
```

Unfortunately, Bazel does not support coverage for go_test.  There are documented work arounds but DHill could not get them to work.

## How to send a Event API request to AWS dev instance.

1) You can click on the "Test" button on the [lambda webpage](https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/event-api-dev-main?tab=configuration).  It sends a sample request to the adserver.  Copy the structure and formatting from another event.

2) You can run curl against the URL.  The json request makes the most sense if you stick the body in a file and then include the file in the curl request.

```
echo '{
  "userInfo": {
    "logUserId": "1",
    "isInternalUser": true
  },
  "impression": [
    {
      "insertionId": "b59b77c7-a55a-4f01-91b5-58a3d4e36876"
    }
  ]
}' > /tmp/impression.json

curl -H "Content-Type: application/json" -H "x-api-key: yourapikey" --data "@/tmp/impression.json" https://depj6tp7ef.execute-api.us-east-1.amazonaws.com/dev/main
```

Or you can run [send_req_and_impr_to_aws_dev.sh](https://github.com/promotedai/serving/blob/master/tools/send_req_and_impr_to_aws_dev.sh) to send multiple ad requests and impressions.

# Deploy

Deploys are controlled by GitHub Actions.  Pushing a commit to `develop` deploys the dev Event API Lambdas.  Pushing a commit to `main` deploys the prod Event API Lambdas.
