 #!/bin/bash

#Setup stack name is the original stack name used to setup the connector in S3
PARENT_STACK_NAME=${1:-aksglue}
STACK_NAME="${2:-$PARENT_STACK_NAME-inc-export}"
KEYSPACE_NAME=${3:-mykeyspace}
TABLE_NAME=${4:-mytable}
FORMAT=${6:-parquet}
DISTINCT_KEYS=${7:-key,value}
PASTURI=${8:-s3://pasturi}
CURRENTURI=${9:-s3://currenturi}

echo "Parent stack used: ${PARENT_STACK_NAME}"
echo "stack name used: ${STACK_NAME}"
echo "Keyspace used used: ${KEYSPACE_NAME}"
echo "Table used: ${TABLE_NAME}"
echo "S3URI used: ${S3URI}"
echo "Format used: ${FORMAT}"
echo "PASTURI used: ${PASTURI}"
echo "CURRENTURI used: ${CURRENTURI}"
echo "DISTINCT_KEYS used: ${DISTINCT_KEYS}"

if ! command -v aws &> /dev/null; then
    echo "AWS CLI \"aws\" is not installed. aws is required for deploying artifacts to s3. See https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html "
    exit 1
fi

export KEYSPACES_GLUE_BUCKET=$(aws cloudformation describe-stacks --query "Stacks[?StackName==\`$PARENT_STACK_NAME\`][].Outputs[?ExportName==\`KeyspacesBucketNameExport-$PARENT_STACK_NAME\`]".OutputValue --output text)

if [ -z "${KEYSPACES_GLUE_BUCKET}" ]; then
	echo "Parent stack not found. Cloudformation Export not found KeyspacesBucketNameExport-$PARENT_STACK_NAME"
	exit 1
fi

S3URI=${5:-s3://${KEYSPACES_GLUE_BUCKET}/export}

echo "S3URI used: ${S3URI}"

echo "Moving script to bucket ${KEYSPACES_GLUE_BUCKET}"

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key scripts/incremental-export-sample.scala --body incremental-export-sample.scala || exit 1

aws cloudformation create-stack --stack-name ${STACK_NAME} --parameters ParameterKey=ParentStack,ParameterValue=$PARENT_STACK_NAME ParameterKey=KeyspaceName,ParameterValue=$KEYSPACE_NAME ParameterKey=TableName,ParameterValue=$TABLE_NAME ParameterKey=S3URI,ParameterValue=${S3URI} ParameterKey=FORMAT,ParameterValue=$FORMAT ParameterKey=DISTINCTKEYS,ParameterValue=$DISTINCT_KEYS ParameterKey=PASTURI,ParameterValue=$PASTURI ParameterKey=CURRENTURI,ParameterValue=$CURRENTURI  --template-body 'file://glue-job-diff.yaml' || exit 1  #--debug  

echo Waiting for CloudFormation stack to complete ...

aws cloudformation wait stack-create-complete --stack-name ${STACK_NAME}  || exit 1  

