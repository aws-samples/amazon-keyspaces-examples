 #!/bin/bash

echo "Positional Arguments: PARENT_STACK_NAME, STACK_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI, FORMAT"
echo ""
echo "PARENT_STACK_NAME: Stack name used for setting up the connector"
echo "STACK_NAME: Stack name used for setting up glue job"
echo "KEYSPACE_NAME: Keyspace to export from"
echo "TABLE_NAME: Table to export from"
echo "S3URI: S3 URI to export to"
echo "FORMAT: format of the export parquet, json, csv"

PARENT_STACK_NAME=${1:-aksglue}
KEYSPACE_NAME=${2:-mykeyspace}

echo "Parent stack used: ${PARENT_STACK_NAME}"
echo "Keyspace used used: ${KEYSPACE_NAME}"


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

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key sample-data/daily_dataset.csv --body daily_dataset.csv || exit 1

awk -v ks="${KEYSPACE_NAME}" -v s3b="${KEYSPACES_GLUE_BUCKET}" '{ gsub(/800KEYSPACE/,ks); gsub(/800S3BUCKET/,s3b); print}' feature-store-notebook-template.ipynb > feature-store-notebook.ipynb

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key notebooks/feature-store-notebook.ipynb --body feature-store-notebook.ipynb || exit 1



