#!/bin/bash

CHAIN=${1:-base}
ADDRESS=${2:-0x50f88fe97f72cd3e75b9eb4f747f59bceba80d59}

curl -X GET "https://api.bubblemaps.io/maps/${CHAIN}/${ADDRESS}?return_nodes=true" \
  -H "X-ApiKey: ${BM_API_KEY}" \
  -H "Content-Type: application/json"
