 #!/bin/bash

echo "Positional Arguments: PARENT_STACK_NAME, STACK_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI, FORMAT"
echo ""
echo "PARENT_STACK_NAME: Stack name used for setting up the connector"
echo "STACK_NAME: Stack name used for setting up glue job"
echo "KEYSPACE_NAME: Keyspace to target"
echo "TABLE_NAME: Table to target"
echo "S3URI: S3 URI to store and retrieve data"
echo "FORMAT: format of the export parquet, json, csv"
echo "DISTINCT_KEYS: comma seperated list"
echo "QUERY_FILTER: filter the table to limit the amount of rows."

PARENT_STACK_NAME=${1:-aksglue}
STACK_NAME="${2:-$PARENT_STACK_NAME-bulk-delete}"
KEYSPACE_NAME=${3:-mykeyspace}
TABLE_NAME=${4:-mytable}
FORMAT=${6:-parquet}
DISTINCT_KEYS=${7:-""}
QUERY_FILTER=${8:-""}

echo "Parent stack used: ${PARENT_STACK_NAME}"
echo "Stack name used:   ${STACK_NAME}"
echo "Keyspace used used: ${KEYSPACE_NAME}"
echo "Table used: ${TABLE_NAME}"
echo "Format used: ${FORMAT}"
echo "DISTINCT_KEYS used: ${DISTINCT_KEYS}"
echo "QUERY_FILTER used: ${QUERY_FILTER}"


if ! command -v aws &> /dev/null; then
    echo "AWS CLI \"aws\" is not installed. aws is required for deploying artifacts to s3. See https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html "
    exit 1
fi

export KEYSPACES_GLUE_BUCKET=$(aws cloudformation describe-stacks --query "Stacks[?StackName==\`$PARENT_STACK_NAME\`][].Outputs[?ExportName==\`KeyspacesBucketNameExport-$PARENT_STACK_NAME\`]".OutputValue --output text)

if [ -z "${KEYSPACES_GLUE_BUCKET}" ]; then
	echo "Parent stack not found. Cloudformation Export not found KeyspacesBucketNameExport-$PARENT_STACK_NAME"
	exit 1
fi

S3URI=${5:-s3://${KEYSPACES_GLUE_BUCKET}}

echo "S3URI used: ${S3URI}"

echo "Moving script to bucket ${KEYSPACES_GLUE_BUCKET}"

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key scripts/$PARENT_STACK_NAME-$STACK_NAME-bulk-delete-sample.scala --body bulk-delete-sample.scala || exit 1

aws cloudformation create-stack --stack-name ${STACK_NAME} --parameters ParameterKey=ParentStack,ParameterValue=$PARENT_STACK_NAME ParameterKey=KeyspaceName,ParameterValue=$KEYSPACE_NAME ParameterKey=TableName,ParameterValue=$TABLE_NAME ParameterKey=S3URI,ParameterValue=${S3URI} ParameterKey=FORMAT,ParameterValue=$FORMAT ParameterKey=DistinctKeys,ParameterValue=$DISTINCT_KEYS ParameterKey=QueryFilter,ParameterValue=$QUERY_FILTER --template-body 'file://glue-job-bulk-delete.yaml' || exit 1  #--debug  

echo Waiting for CloudFormation stack to complete ...
aws cloudformation wait stack-create-complete --stack-name ${STACK_NAME}  || exit 1 

aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs" || exit 1 


