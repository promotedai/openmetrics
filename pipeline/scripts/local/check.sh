source "$(dirname "$0")/../../../config/config.sh"

set -x 
function ephemeral_port() {
  # Source - https://unix.stackexchange.com/questions/55913/whats-the-easiest-way-to-find-an-unused-local-port
  local low_bounce=49152
  local range=16384
  while true; do
    candidate=$(($low_bound + ($RANDOM % $range)))
    (echo "" >/dev/tcp/127.0.0.1/${candidate}) >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo $candidate
      break
    fi
  done
}

printf "Verifying if Flink is accessible..."
flink_rest_temp_port="$(ephemeral_port)"
(kubectl port-forward svc/flink-jobmanager-rest -n $LOCAL_K8S_NAMESPACE $flink_rest_temp_port:8081 >/dev/null 2>&1) &
kubectl_pid=$!
sleep 5 # to avoid race

flink_health_check=$(curl --write-out '%{http_code}' --silent --output /dev/null localhost:$flink_rest_temp_port/jobmanager/metrics)

if [[ $flink_health_check == "200" ]]; then
  printf "Success!\n"
else
  printf "Failed. You may want to clean and set up again!\n"
  exit 1
fi
sleep 1
kill $kubectl_pid
