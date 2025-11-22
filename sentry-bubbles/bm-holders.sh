#!/bin/bash

CHAIN=${1:-base}
ADDRESS=${2:-0x50f88fe97f72cd3e75b9eb4f747f59bceba80d59}
DATE=${3:-$(date +%Y-%m-%d)}
NOCACHE=${4:-false}
COUNT=${5:-10}

# Build JSON DATA string with input parameters
DATA=$(cat <<EOF
{"chain":"${CHAIN}","address":"${ADDRESS}"}
EOF
)

echo "Parameters:"
echo "  CHAIN: ${CHAIN}"
echo "  ADDRESS: ${ADDRESS}"
echo "  DATE: ${DATE}"
echo "  NOCACHE: ${NOCACHE}"
echo "  COUNT: ${COUNT}"
echo "  DATA: ${DATA}"
echo ""

curl -X POST "https://api.bubblemaps.io/addresses/token-top-holders?count=${COUNT}&date=${DATE}&nocache=${NOCACHE}" \
  -H "X-ApiKey: ${BM_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "${DATA}"

#https://api.bubblemaps.io/addresses/token-top-holders?count=250&date=2025-11-22&nocache=false
