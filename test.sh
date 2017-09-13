#!/usr/bin/env bash

set -u

AVATICA=$1
SQL=$2

CONNECTION_ID="conn-$(whoami)-$(date +%s)"
MAX_ROW_COUNT=100
NUM_ROWS=2
OFFSET=0

echo "Open connection"
openConnectionReq="{\"request\": \"openConnection\",\"connectionId\": \"${CONNECTION_ID}\"}"
# Example of how to set connection properties with info key
# openConnectionReqWithProperties="{\"request\": \"openConnection\",\"connectionId\": \"${CONNECTION_ID}\",\"info\": {\"user\": \"SCOTT\",\"password\": \"TIGER\"}}"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "$openConnectionReq"
echo

echo "Create statement"
STATEMENTRSP=$(curl -s "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"createStatement\",\"connectionId\": \"${CONNECTION_ID}\"}")
STATEMENTID=$(echo "$STATEMENTRSP" | jq .statementId)
echo

echo "PrepareAndExecuteRequest"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"prepareAndExecute\",\"connectionId\": \"${CONNECTION_ID}\",\"statementId\": $STATEMENTID,\"sql\": \"$SQL\",\"maxRowCount\": ${MAX_ROW_COUNT}, \"maxRowsInFirstFrame\": ${NUM_ROWS}}"
echo

# Loop through all the results
ISDONE=false
while ! $ISDONE; do
  OFFSET=$((OFFSET + NUM_ROWS))
  echo "FetchRequest - Offset=$OFFSET"
  FETCHRSP=$(curl -s "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"fetch\",\"connectionId\": \"${CONNECTION_ID}\",\"statementId\": $STATEMENTID,\"offset\": ${OFFSET},\"fetchMaxRowCount\": ${NUM_ROWS}}")
  echo "$FETCHRSP"
  ISDONE=$(echo "$FETCHRSP" | jq .frame.done)
  echo
done

echo "Close statement"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"closeStatement\",\"connectionId\": \"${CONNECTION_ID}\",\"statementId\": $STATEMENTID}"
echo

echo "Close connection"
curl -i -w "\n" "$AVATICA" -H "Content-Type: application/json" --data "{\"request\": \"closeConnection\",\"connectionId\": \"${CONNECTION_ID}\"}"
echo
