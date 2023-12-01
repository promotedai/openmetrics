#!/usr/bin/env bash
#
# To delete one env's labeled s3 event logs.
#
# DANGEROUS
#
# Expects setup-terminal.sh to already have been run.
#
# To run:
# export LABEL_TO_DELETE=green_or_blue
# ~/promotedai/infra-configs/scripts/metrics/common/delete-s3-event-logs.sh

set -eux

if [[ -z "${CUSTOMER_ENV}" ]]; then
  echo "CUSTOMER_ENV must be set"
  exit 1
fi

if [[ -z "${LABEL_TO_DELETE}" ]]; then
  echo "LABEL_TO_DELETE must be set"
  exit 1
fi

S3_PATH="s3://${CUSTOMER_ENV}-event-logs/${LABEL_TO_DELETE}"

echo "Will delete objects under ${S3_PATH}"
while true; do
    read -r -p "Do you wish to continue? (y/n)" YN
    case $YN in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

aws s3 rm --recursive "${S3_PATH}"