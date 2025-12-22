#!/bin/sh
set -e

CONNECT_URL=${CONNECT_URL:-http://kafka-connect:8083}
CONNECT_MIRROR_URL=${CONNECT_MIRROR_URL:-http://kafka-connect-mirror:8083}
MIRROR_CONFIG_FILE=${MIRROR_CONFIG_FILE:-mirror-connector.json}
HDFS_CONFIG_FILE=${HDFS_CONFIG_FILE:-hdfs-sink-connector.json}
ELASTIC_CONFIG_FILE=${ELASTIC_CONFIG_FILE:-elastic-sink-connector.json}


echo "Waiting for Kafka Connect at $CONNECT_URL ..."

until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "Waiting..."
  sleep 5
done


echo "Pushing elastic-sink-connector config"

curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data @"$ELASTIC_CONFIG_FILE" \
  "$CONNECT_URL/connectors/elastic-sink-connector/config"

echo "Waiting for Kafka Connect at $CONNECT_MIRROR_URL ..."

until curl -sf "$CONNECT_MIRROR_URL/connectors" > /dev/null; do
  echo "Waiting..."
  sleep 5
done

echo "Pushing mirror-connector config"

curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data @"$MIRROR_CONFIG_FILE" \
  "$CONNECT_MIRROR_URL/connectors/mirror-connector/config"

echo "Pushing hdfs-sink-connector config"

curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data @"$HDFS_CONFIG_FILE" \
  "$CONNECT_MIRROR_URL/connectors/hdfs-sink-connector/config"

echo "Done."
