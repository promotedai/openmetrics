# Configuration variables used by scripts
PROMOTED_DIR="${HOME}/promotedai/"
LOCAL_CONFIG="$PROMOTED_DIR/local-config.yaml"
LOCAL_K8S_NAMESPACE=$(yq ".local.k8s_namespace" $LOCAL_CONFIG)
MINIKUBE_IP=$(yq ".local.minikube.ip" $LOCAL_CONFIG)
LOCAL_KAFKA_FIRST_PORT=$(yq ".local.metrics.kafka.first_port" $LOCAL_CONFIG)
eval $(minikube docker-env)
