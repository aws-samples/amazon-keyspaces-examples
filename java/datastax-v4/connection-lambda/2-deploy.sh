#!/bin/bash
set -eo pipefail
ARTIFACT_BUCKET=$(<bucket-name.txt)
TEMPLATE=template.yml
BUILD_TYPE=gradle

echo "Building with $TEMPLATE..."
./gradlew build
aws cloudformation package --template-file $TEMPLATE --s3-bucket $ARTIFACT_BUCKET --output-template-file out.yml
aws cloudformation deploy --template-file out.yml --stack-name KeyspacesLambdaExample --capabilities CAPABILITY_NAMED_IAM

