 #!/bin/bash

#Setup stack name is the original stack name used to setup the connector in S3
PARENT_STACK_NAME=${1:-aksglue}
STACK_NAME="${2:-$PARENT_STACK_NAME-count}"
KEYSPACE_NAME=${3:-mykeyspace}
TABLE_NAME=${4:-mytable}

echo "Parent stack used: ${PARENT_STACK_NAME}"
echo "stack name used: ${STACK_NAME}"
echo "Keyspace used used: ${KEYSPACE_NAME}"
echo "Table used: ${TABLE_NAME}"

export KEYSPACES_GLUE_BUCKET=$(aws cloudformation describe-stacks --query "Stacks[?StackName==\`$PARENT_STACK_NAME\`][].Outputs[?ExportName==\`KeyspacesBucketNameExport-$PARENT_STACK_NAME\`]".OutputValue --output text)

echo "Moving script to bucket ${KEYSPACES_GLUE_BUCKET}"

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key scripts/count-example.scala --body count-example.scala || exit 1

aws cloudformation create-stack --stack-name $STACK_NAME --parameters ParameterKey=ParentStack,ParameterValue=$PARENT_STACK_NAME ParameterKey=KeyspaceName,ParameterValue=$KEYSPACE_NAME ParameterKey=TableName,ParameterValue=$TABLE_NAME --template-body 'file://glue-job-count-rows.yaml' #--debug 