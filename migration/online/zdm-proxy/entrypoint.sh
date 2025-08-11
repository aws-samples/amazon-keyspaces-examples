#!/bin/sh
set -ex

# Resolve Cassandra IP
REGION=${AWS_REGION:-"us-east-1"}
ENPOINT="cassandra.${REGION}.amazonaws.com"
CASSANDRA_PORT=9142

CASSANDRA_IP=$(dig +short "$ENPOINT" | head -n 1)

if [ -z "$CASSANDRA_IP" ]; then
  echo "❌ Failed to resolve $ENPOINT"
  exit 1
fi

echo "✅ Resolved $ENPOINT → $CASSANDRA_IP"

# Start local proxy (e.g. socat)
socat TCP-LISTEN:9142,bind=127.0.0.1,reuseaddr,fork TCP:$CASSANDRA_IP:$CASSANDRA_PORT &

if [ -n "$AWS_NLB_DNS" ]; then
  echo "✅ AWS_NLB_DNS is set: $AWS_NLB_DNS"

  # Read IPs into a space-separated string
  ALL_IPS=$(dig +short "$AWS_NLB_DNS" | tr '\n' ' ')
  
  if [ -n "$ALL_IPS" ]; then
    echo "AWS_NLB_DNS IPs found: $ALL_IPS"

    # Convert space-separated list to comma-separated
    ZDM_PROXY_TOPOLOGY_ADDRESSES=$(echo "$ALL_IPS" | sed 's/ /, /g' | sed 's/, $//')

    export ZDM_PROXY_TOPOLOGY_ADDRESSES

    # Count IPs (words in string)
    IP_COUNT=$(echo "$ALL_IPS" | wc -w)

    # Generate a random number 0 <= n < IP_COUNT
    RAND=$(awk -v max="$IP_COUNT" 'BEGIN { srand(); print int(rand() * max) }')

    # Get selected IP
    SELECTED_IP=$(echo "$ALL_IPS" | awk -v n=$((RAND + 1)) '{ print $n }')

    export ZDM_PROXY_TOPOLOGY_INDEX=$RAND
    export ZDM_PROXY_TOPOLOGY_ADDRESS=$SELECTED_IP

    echo "ZDM_PROXY_TOPOLOGY_INDEX: $ZDM_PROXY_TOPOLOGY_INDEX"
    echo "ZDM_PROXY_TOPOLOGY_ADDRESS: $ZDM_PROXY_TOPOLOGY_ADDRESS"
  else
    echo "AWS_NLB_DNS No IPs returned."
  fi
else
  echo "❌ AWS_NLB_DNS is not set"
fi

# Hand off to ZDM proxy main binary
exec /main "$@"
