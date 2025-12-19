#!/bin/sh
set -e

CONNECT_URL=${CONNECT_URL:-http://kafka-connect:8083}
MIRROR_CONFIG_FILE=${MIRROR_CONFIG_FILE:-mirror-connector.json.json}
ELASTIC_CONFIG_FILE=${ELASTIC_CONFIG_FILE:-elastic-sink-connector.json}


echo "Waiting for Kafka Connect at $CONNECT_URL ..."

until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "Waiting..."
  sleep 5
done


echo "Pushing mirror-connector config"

curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data @"$MIRROR_CONFIG_FILE" \
  "$CONNECT_URL/connectors/mirror-connector/config"

echo "Pushing elastic-sink-connector config"

curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data @"$ELASTIC_CONFIG_FILE" \
  "$CONNECT_URL/connectors/elastic-sink-connector/config"

echo "Done."
