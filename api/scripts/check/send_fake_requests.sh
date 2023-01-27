#!/bin/bash
# Sends fake requests to an dev Event APIs.
#
# WARNING: This is not currently designed for hitting production Event API.  This will write
# actual events.  We will eventually support this by using a test platformId or userId.
#
# To send a fake event to prm-dev's Event API.
# ./send_fake_requests.sh --eventApi https://m3l376qlnf.execute-api.us-east-1.amazonaws.com/dev/main --apiKey 7Y6IuX9uI670muCQfY6TWaWARpq70WWv3SZqxzkB
#
# TODO - support prod servers.
# TODO - check that the values persisted properly.

EVENT_API=
API_KEY=
NUM_LOG_REQUESTS=1

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -e|--eventApi)
    EVENT_API="$2"
    shift # past argument
    shift # past value
    ;;
    -k|--apiKey)
    API_KEY="$2"
    shift # past argument
    shift # past value
    ;;
    -n|--numLogRequests)
    NUM_LOG_REQUESTS="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ -z ${EVENT_API} ]];
then
  echo "Need to set '--eventApi {url}"
  exit 1
fi

if [[ -z ${API_KEY} ]];
then
  echo "Need to set '-apiKey {key}"
  exit 1
fi

function uuid(){
  if [ -x "$(command -v uuidgen)" ]; then
    uuidgen
    exit 0
  fi
  if [ -x "$(command -v /proc/sys/kernel/random/uuid)" ]; then
    /proc/sys/kernel/random/uuid
    exit 0
  fi
  exit 1
}

for ((i=1;i<=NUM_LOG_REQUESTS;i++));do
  SESSION_ID=$(uuid)
  VIEW_ID=$(uuid)
  REQUEST_ID=$(uuid)
  INSERTION_ID=$(uuid)
  IMPRESSION_ID=$(uuid)

  curl -d "{\"platformId\":\"1\",\"userId\":\"1\",\"user\":[{\"common\":{\"user_id\":\"1\"}}],\"session\":[{\"common\":{\"session_id\":\"${SESSION_ID}\"}}],\"view\":[{\"common\":{\"view_id\":\"${VIEW_ID}\",\"session_id\":\"${SESSION_ID}\"}}],\"request\":[{\"common\":{\"request_id\":\"${REQUEST_ID}\",\"view_id\":\"${VIEW_ID}\"}}],\"insertion\":[{\"common\":{\"insertionId\":\"${INSERTION_ID}\",\"request_id\":\"${REQUEST_ID}\",\"promotedaiContentId\":\"3\",\"contentId\":\"id-1-2\",\"insertionLogFlatPromotion\":{\"flatPromotion\":{\"entityPath\":{\"platformId\":\"1\",\"customerId\":\"1\",\"accountId\":\"2\",\"campaignId\":\"6\",\"promotionId\":\"28\",\"contentId\":\"3\"},\"account\":{\"currencyCode\":\"USD\"},\"promotion\":{\"content\":{\"externalContentId\":\"id-1-2\"},\"bidType\":\"CPC\",\"bidAmount\":3}}}}}],\"impression\":[{\"common\":{\"impressionId\":\"${IMPRESSION_ID}\",\"insertionId\":\"${INSERTION_ID}\"}}]}" -H "x-api-key: ${API_KEY}"  ${EVENT_API}
done
