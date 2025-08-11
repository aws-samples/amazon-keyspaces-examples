 #!/bin/bash

echo "Positional Arguments: PARENT_STACK_NAME, STACK_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI, FORMAT"
echo ""
echo "PARENT_STACK_NAME: Stack name used for setting up the connector"
echo "STACK_NAME: Stack name used for setting up glue job"
echo "KEYSPACE_NAME: Keyspace to export from"
echo "TABLE_NAME: Table to export from"
echo "TTL_FIELD: Field to modify the TTL for"
echo "TTL_TIME_TO_ADD: Time to add to the TTL"

PARENT_STACK_NAME=${1:-aksglue}
STACK_NAME="${2:-$PARENT_STACK_NAME-export}"
KEYSPACE_NAME=${3:-mykeyspace}
TABLE_NAME=${4:-mytable}
TTL_FIELD=${5:-ttl}
TTL_TIME_TO_ADD=${6:-2592000} # 30 days

echo "Parent stack used: ${PARENT_STACK_NAME}"
echo "Stack name used:   ${STACK_NAME}"
echo "Keyspace used used: ${KEYSPACE_NAME}"
echo "Table used: ${TABLE_NAME}"
echo "TTL Field used: ${TTL_FIELD}"
echo "TTL Time to add: ${TTL_TIME_TO_ADD}"

if ! command -v aws &> /dev/null; then
    echo "AWS CLI \"aws\" is not installed. aws is required for deploying artifacts to s3. See https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html "
    exit 1
fi

export KEYSPACES_GLUE_BUCKET=$(aws cloudformation describe-stacks --query "Stacks[?StackName==\`$PARENT_STACK_NAME\`][].Outputs[?ExportName==\`KeyspacesBucketNameExport-$PARENT_STACK_NAME\`]".OutputValue --output text)

if [ -z "${KEYSPACES_GLUE_BUCKET}" ]; then
	echo "Parent stack not found. Cloudformation Export not found KeyspacesBucketNameExport-$PARENT_STACK_NAME"
	exit 1
fi

echo "Moving script to bucket ${KEYSPACES_GLUE_BUCKET}"

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key scripts/$PARENT_STACK_NAME-$STACK_NAME-export.scala --body export-sample.scala || exit 1

aws cloudformation create-stack --stack-name ${STACK_NAME} --parameters ParameterKey=ParentStack,ParameterValue=$PARENT_STACK_NAME ParameterKey=KeyspaceName,ParameterValue=$KEYSPACE_NAME ParameterKey=TableName,ParameterValue=$TABLE_NAME ParameterKey=TTLField,ParameterValue=$TTL_FIELD ParameterKey=TTLTimeToAdd,ParameterValue=$TTL_TIME_TO_ADD --template-body 'file://glue-job-modify-ttl.yaml' || exit 1  #--debug  

echo Waiting for CloudFormation stack to complete ...
aws cloudformation wait stack-create-complete --stack-name ${STACK_NAME}  || exit 1 

aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs" || exit 1 


