LIMIT=${LIMIT:-1000}
TYPE=${TYPE:-}
OFFSET=${OFFSET:-0}

# csv
if [ "$TYPE" != "" ]; then
  TYPE="/${TYPE}"
fi

if [ "$OFFSET" != "" ]; then
  OFFSET="&offset=$OFFSET"
fi


curl -H "X-Dune-API-Key:$DUNE_API_KEY" "https://api.dune.com/api/v1/query/6233959/results${TYPE}?limit=${LIMIT}${OFFSET}"
