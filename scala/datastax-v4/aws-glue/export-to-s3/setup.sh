 #!/bin/bash

#Setup stack name is the original stack name used to setup the connector in S3
PARENT_STACK_NAME=${1:-aksglue}
KEYSPACE_NAME=${3:-mykeyspace}
TABLE_NAME=${4:-mytable}
FORMAT=${5:-parquet}
STACK_NAME="${2:-$PARENT_STACK_NAME-export}"

echo "Parent stack used: ${PARENT_STACK_NAME}"
echo "Export stack used: ${STACK_NAME}"
echo "Keyspace used used: ${KEYSPACE_NAME}"
echo "Table used: ${TABLE_NAME}"
echo "Fornat used: ${FORMAT}"


export KEYSPACES_GLUE_BUCKET=$(aws cloudformation describe-stacks --query "Stacks[?StackName==\`$PARENT_STACK_NAME\`][].Outputs[?ExportName==\`KeyspacesBucketNameExport-$PARENT_STACK_NAME\`]".OutputValue --output text)

echo "Moving export script to bucket ${KEYSPACES_GLUE_BUCKET}"

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key scripts/export-sample.scala --body export-sample.scala || exit 1

aws cloudformation create-stack --stack-name $STACK_NAME --parameters ParameterKey=ParentStack,ParameterValue=$PARENT_STACK_NAME ParameterKey=KeyspaceName,ParameterValue=$KEYSPACE_NAME ParameterKey=TableName,ParameterValue=$TABLE_NAME ParameterKey=FORMAT,ParameterValue=$FORMAT --template-body 'file://glue-job-export-to-s3.yaml' #--debug 