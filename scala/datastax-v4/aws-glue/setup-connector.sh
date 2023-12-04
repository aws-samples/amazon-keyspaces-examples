 #!/bin/bash

#distribution of downloaded artifacts  
export ARTIFACT_DIR=dist
#Dependency versions. These versions are used to download required jars for AWS Glue jobs
export SPARK_CONNNECTOR_VERSION=3.1.0
export SPARK_EXTENSIONS_VERSION=2.8.0-3.4
export SIGV4_PLUGIN_VERSION=4.0.9
export SCALA_VERSION=2.12

# validate required executables required for this script. 
# if validation fails the script will exit
echo Validating setup ...

if [ $# -gt 3 ]; then
    echo "Invalid number of arguments: STACK_NAME, BUCKET_NAME, ROLE_NAME"
    exit 1
fi

if ! command -v aws &> /dev/null
then
    echo "AWS CLI \"aws\" is not installed. aws is required for deploying artifacts to s3. See https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html "
    exit 1
fi

if ! command -v git &> /dev/null
then
    echo "Git \"git\" is not installed.  git is required for building Retry policy class. See https://git-scm.com/book/en/v2/Getting-Started-Installing-Git "
    exit 1
fi

if ! command -v mvn &> /dev/null
then
    echo "Maven \"mvn\" is not installed. mvn is required for building Retry policy class. See https://maven.apache.org/install.html "
    exit 1
fi

export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ -z "$AWS_ACCOUNT_ID" ]; then   # var is set and is empty
  echo "Could not get the AWS account id through aws cli"
  exit 1
fi

## Validate working directory is clean
if [ -d "$ARTIFACT_DIR" ]; then
  echo "Local directory: \"$ARTIFACT_DIR\" already exist. Please clean up before running again"
  exit 1
fi

#Modifiable Properties. 
## Properties here are derived from Modifiable properties, environment, or service calls
export STACK_NAME="${1:-aksglue}"
export KEYSPACES_GLUE_BUCKET="${2:-amazon-keyspaces-glue-$STACK_NAME-$AWS_ACCOUNT_ID}"
export KEYSPACES_GLUE_SERVICE_ROLE="${3:-amazon-keyspaces-glue-servcie-role-$STACK_NAME}"

export SPARK_CONNNECTOR=spark-cassandra-connector-assembly_2.12-$SPARK_CONNNECTOR_VERSION.jar
export SPARK_EXTENSIONS=spark-extension_2.12-$SPARK_EXTENSIONS_VERSION.jar
export SIGV4_PLUGIN=aws-sigv4-auth-cassandra-java-driver-plugin-$SIGV4_PLUGIN_VERSION-shaded.jar
export KEYSPACES_RETRY_POLICY=amazon-keyspaces-helpers-1.0-SNAPSHOT.jar
export GLUE_SETUP_TEMPLATE=glue-setup-template.yaml

echo "STACK_NAME: ${STACK_NAME}"
echo "Creating bucket with name $KEYSPACES_GLUE_BUCKET"
echo "Creating role with name $KEYSPACES_GLUE_SERVICE_ROLE"

#aws s3 head-bucket help # "mike" || echo " Bucket already exists" && exit 1 
### Create bucket and Glue Service Role 
# Creates a Amazon S3 bucket to store jars for the 
# spark cassandra connector, sigv4-pluggin, spark-extension, 
# and best practice configurations when connecting to Apache Cassandra or Amazon Keyspaces
# Creates an IAM role to read and write to Keyspaces, S3, and Glue 
# spark cassandra connector, sigv4-pluggin, spark-extension, 
# and best practice configurations when connecting to Apache Cassandra or Amazon Keyspaces

### Using Cloudformation create-trable 
echo Creating bucket and IAM service role through AWS CloudFormation ...
aws cloudformation create-stack --stack-name ${STACK_NAME} --parameters ParameterKey=KeyspacesBucketName,ParameterValue=${KEYSPACES_GLUE_BUCKET} ParameterKey=KeyspacesGlueServiceRoleName,ParameterValue=${KEYSPACES_GLUE_SERVICE_ROLE} --template-body 'file://${GLUE_SETUP_TEMPLATE}' --capabilities CAPABILITY_NAMED_IAM  || exit 1

echo Waiting for CloudFormation stack to complete ...
aws cloudformation wait stack-create-complete --stack-name ${STACK_NAME}  || exit 1 

### Download artifacts and upload to s3
# ***  spark cassandra connector for spark cassandra interfacing
# ***  sigv4-pluggin to use glue service roles to connect to Keyspaces 
# ***  spark-extension added libraries to help compare differences in exports

mkdir $ARTIFACT_DIR

echo Downloading $SPARK_CONNNECTOR ... 
curl -L -f -o $ARTIFACT_DIR/$SPARK_CONNNECTOR https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_$SCALA_VERSION/$SPARK_CONNNECTOR_VERSION/$SPARK_CONNNECTOR  || exit 1

echo Downloading $SPARK_EXTENSIONS ... 
curl -L -f -o $ARTIFACT_DIR/$SPARK_EXTENSIONS https://repo1.maven.org/maven2/uk/co/gresearch/spark/spark-extension_$SCALA_VERSION/$SPARK_EXTENSIONS_VERSION/$SPARK_EXTENSIONS || exit 1

echo Downloading $SIGV4_PLUGIN ...
curl -L -f -o $ARTIFACT_DIR/$SIGV4_PLUGIN https://repo1.maven.org/maven2/software/aws/mcs/aws-sigv4-auth-cassandra-java-driver-plugin/$SIGV4_PLUGIN_VERSION/$SIGV4_PLUGIN || exit 1

                                          #https://repo1.maven.org/maven2/software/aws/mcs/aws-sigv4-auth-cassandra-java-driver-plugin/4.0.9/aws-sigv4-auth-cassandra-java-driver-plugin-4.0.9-shaded.jar
aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key jars/$SPARK_CONNNECTOR --body $ARTIFACT_DIR/$SPARK_CONNNECTOR || exit 1

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key jars/$SPARK_EXTENSIONS --body $ARTIFACT_DIR/$SPARK_EXTENSIONS || exit 1

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key jars/$SIGV4_PLUGIN     --body $ARTIFACT_DIR/$SIGV4_PLUGIN     || exit 1

# The Amazon Keyspaces Retry Policy is an alternative to the Retry policy for the Spark Cassandra library. 
# The main difference is the AmazonKeyspacesRetryPolicy is more fault tolerant for Keyspaces SystemErrors and Keyspaces UserErrors 

git clone https://github.com/aws-samples/amazon-keyspaces-java-driver-helpers $ARTIFACT_DIR/amazon-keyspaces-java-driver-helpers

mvn clean package -Dmaven.test.skip=true -q -f $ARTIFACT_DIR/amazon-keyspaces-java-driver-helpers/pom.xml || exit 1

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key jars/$KEYSPACES_RETRY_POLICY  --body $ARTIFACT_DIR/amazon-keyspaces-java-driver-helpers/target/$KEYSPACES_RETRY_POLICY || exit 1

# Upload configuration containing best practices for Apache Cassandra and Amazon Keyspaces
echo Uploading Config ...
aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key conf/cassandra-application.conf --body cassandra-application.conf || exit 1

aws s3api put-object --bucket $KEYSPACES_GLUE_BUCKET --key conf/keyspaces-application.conf --body keyspaces-application.conf || exit 1

