Environment varibles need to be updated in these spots:
1) local.json
2) metrics/api/kubernetes/metrics-api-local-deployment.yaml

This is because we have engineering debt with our startup script.  The K8 config is used for one script and the local.json is used for a different script.

TODO - delete local.json and merge into either local-config.yaml or k8 config.
