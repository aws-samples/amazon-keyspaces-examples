#!/bin/bash
set -eo pipefail
FUNCTION=$(aws cloudformation describe-stack-resource --stack-name KeyspacesLambdaExample --logical-resource-id function --query 'StackResourceDetail.PhysicalResourceId' --output text)
while true; do
  aws lambda invoke --function-name $FUNCTION --payload fileb://event.json /dev/stdout
  echo ""
  sleep 2
done