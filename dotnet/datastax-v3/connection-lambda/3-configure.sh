#!/bin/bash
set -eo pipefail
FUNCTION=$(aws cloudformation describe-stack-resource --stack-name keyspaces-lambda-csharp --logical-resource-id function --query 'StackResourceDetail.PhysicalResourceId' --output text)

aws lambda update-function-configuration --function-name $FUNCTION \
    --environment "Variables={SERVICE_USER_NAME=alice-at-111122223333,SERVICE_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY,CERTIFICATE_PATH=.}"
