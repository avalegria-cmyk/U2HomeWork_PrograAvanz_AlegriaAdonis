#!/bin/sh
set -eu

BASE_URL="${BASE_URL:-http://localhost:3000}"

echo "1) Health"
curl -s "$BASE_URL/api/health"
printf "\n\n"

echo "2) Publish an event"
curl -s -X POST "$BASE_URL/api/events" \
  -H 'content-type: application/json' \
  -d '{"type":"orders.created","payload":{"orderId":"ORD-DEMO-001","amount":99.95}}'
printf "\n\n"

echo "3) Stream history as NDJSON"
curl -s "$BASE_URL/api/events?limit=5"
printf "\n"

echo "4) Stream simulated async data"
curl -s "$BASE_URL/api/simulate?count=5&delayMs=25"
printf "\n"
