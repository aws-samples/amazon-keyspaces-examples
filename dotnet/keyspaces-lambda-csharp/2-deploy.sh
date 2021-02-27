#!/bin/bash
set -eo pipefail
ARTIFACT_BUCKET=$(cat bucket-name.txt)
cd src/keyspaces-lambda-csharp
dotnet lambda package
cd ../../
aws cloudformation package --template-file template.yml --s3-bucket $ARTIFACT_BUCKET --output-template-file out.yml
aws cloudformation deploy --template-file out.yml --stack-name keyspaces-lambda-csharp --capabilities CAPABILITY_NAMED_IAM
