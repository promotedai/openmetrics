#!/usr/bin/env bash
#
# To delete one env data.
#
# DANGEROUS - Be careful running this since it can delete production data.
#
# Expects setup-terminal.sh to already have been run.
#
# To run:
# export LABEL_TO_DELETE=green_or_blue
# ~/promotedai/metrics/pipeline/scripts/redis/flush-counters-redis.sh

set -eux

if [[ -z "${LABEL_TO_DELETE}" ]]; then
  echo "LABEL must be set"
  exit 1
fi

REDIS_URL=$(terraform output --json | jq '.counters_redis_url.value')

REGEX='redis://([^:]*):([0-9]*)/([0-9]*)'
if [[ $REDIS_URL =~ $REGEX ]]; then
  echo "Redis URL Parsed successfully; url=${REDIS_URL}"
else
  echo "Failed to parse redis url=${REDIS_URL}"
  exit 1
fi

# Change readonly addresses to mutable.
URL_DOMAIN="${BASH_REMATCH[1]/-ro/}"
URL_PORT=${BASH_REMATCH[2]}
if [[ "${LABEL_TO_DELETE}" == "green" ]]; then
  DB_NUM=0
elif [[ "${LABEL_TO_DELETE}" == "blue" ]]; then
  DB_NUM=5
else
  echo "Unsupported LABEL_TO_DELETE"
  exit 1
fi


echo "Will execute: redis-cli -h ${URL_DOMAIN} -p ${URL_PORT} -n ${DB_NUM} FLUSHDB ASYNC"
while true; do
    read -r -p "Do you wish to continue? (y/n)" YN
    case $YN in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

echo "Start flushing DB"
redis-cli -h "${URL_DOMAIN}" -p "${URL_PORT}" -n "${DB_NUM}" FLUSHDB ASYNC
echo "Finished flushing DB"
