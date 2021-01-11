## Using Amazon Keyspaces with Lambda

This is a sample project showing best practices for connecting to Amazon Keyspaces via AWS Lambda.

## Running tests

### Prerequisites

### Environment variables
Before submitting this job to Lambda it is recommended that you verify it manually on your developer machine.

To connect you need to provide credentials based on the connection method you plan to use.

For connecting via Server-side credentials (SSC) make sure to specify : 
- SSC_USER_NAME
- SSC_PASSWORD 
- SSL_TRUSTSTORE_PASSWORD=<your_password>
 
For connecting via SigV4 and role based authentication
- AWS_REGION
- AWS_SECRET_ACCESS_KEY
- AWS_ACCESS_KEY_ID
- SSL_TRUSTSTORE_PASSWORD=<your_password>

Then execute `example.InvokeTest` by using gradle on the command line.

```sh
$ ./gradlew test
```
## Running an example in Lambda
### Installing the Lambda Function
There are some provided scripts for you to install and deploy Lambda to your existing AWS account.

Before running the scripts you need to load a profile file (or set environment variables) that grant access to AWS to create S3, IAM roles, and to setup Lambda functions.   

```sh
$ ./gradlew build
```

before executing the following. 

To create the S3 bucket used to deploy Lambda code, run the following on the command line.  You only need to run this once. 

```
$ ./1-create-bucket.sh
```

### Deploy and setup Lambda for the first time
next invoke:

```
$ ./2-deploy.sh 
```

This constructs a Lambda function, with the current code in it.  

The example code uses credentials that are meant to be specified either by environment variable or execution role.  After you have run this for the first time, you also have to set the appropriate variables in the AWS Lambda Console.

#### Using SigV4 Credentials
`2-deploy.sh` assigns an execution role for Lambda that automatically uses SigV4
by default.  

#### Using SSC credentials
If you prefer to use Service Specific Credentials, you need to add SSC_PASSWORD, SSC_USER_NAME explictly. 

1.  Go to the AWS Lambda Console.
2.  In the console locate the function with the prefix "KeyspacesLambdaExample-function-"  and select the name.
3.  Under __Environment Variables__ select the edit button.
4.  Fill out the correct SSC_PASSWORD, SSC_USER_NAME.
5.  Set SSL_TRUSTSTORE_PASSWORD to be `amazon` (this is the default for cassandra_truststore.jks)


To use SigV4 if you previously set SSC_PASSWORD or SSC_USER_NAME, you have to remove these variables.  

### Executing the Lambda Function
Once the Lambda function has been deployed, run the following.

```
$ ./3-invoke.sh
```

This continually invokes the Lambda function, until you cancel.
You can read the full and complete logs for each Lambda function in the CloudWatch console.  

1. Go to AWS Cloudwatch console
2. Choose Log groups.
2. Select the log group that starts with `/aws/lambda/KeyspacesLambdaExample-function-`
3. Each invocation produces a separate log.

### To remove Lambda Functions
run the following to remove the stack and users
```
$ ./4-cleanup.sh
```





