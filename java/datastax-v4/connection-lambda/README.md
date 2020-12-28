## Using Amazon Keyspaces with Lambda

This is a sample project showing best practices of connecting to Keyspaces via AWS Lambda.

## Running tests

### Prerequisites

### Environment variables
Before submitting this job to lambda it is a good idea to verify it manually on your developer machine.

To connect you will need to provide credentials based on the connection method you plan to use.

For connecting via Server-side credentials (SSC) make sure to specify : 
- SSC_USER_NAME
- SSC_PASSWORD 
- SSL_TRUSTSTORE_PASSWORD=amazon
 
For connecting via SigV4 and role based authentication
- AWS_REGION
- AWS_SECRET_ACCESS_KEY
- AWS_ACCESS_KEY_ID
- SSL_TRUSTSTORE_PASSWORD=amazon

Then execute `example.InvokeTest` by using gradle on the command line.

```sh
$ ./gradlew test
```
## Running an example in Lambda
### Installing the lambda function
There are some provided scripts for you to install and deploy lambda to your existing AWS account.

Before running these you will need to have a profile file loaded (or environment variables set) that grant access to AWS to create S3, iam roeles, and setup lambda functions.   

```sh
$ ./gradlew build
```

before executing the following. 

To create the S3 bucket used to deploy lambda code, run the following on the command line.  You will only need to execute this once. 

```
$ ./1-create-bucket.sh
```

### Deploy and setup lambda the first time
next invoke:

```
$ ./2-deploy.sh 
```

This will construct a lambda function, with the current code in it.  

The example code is uses credentials that are meant to be specified by environment variable or execution role.  Once this is run the first time, you will need to set the appropriate variables in the AWS Lambda Console.

#### Using SigV4 Credentials
`2-deploy.sh` will assign an execution role for lambda that automatically will use SigV4
by default.  

#### Using SSC credentials
If you would like to use Service Specific Credentials you will need to add SSC_PASSWORD, SSC_USER_NAME explictly. 

1.  Go to the AWS Lambda Console.
2.  In the console there should be a function with the following prefix "KeyspacesLambdaExample-function-" click on the name.
3.  Under __Environment Variables__ Click the edit button.
4.  Fill out the correct SSC_PASSWORD, SSC_USER_NAME.
5.  Set SSL_TRUSTSTORE_PASSWORD to be `amazon` (this is the default for cassandra_truststore.jks)


If you previously set SSC_PASSWORD or SSC_USER_NAME you will need to remove these variables to use SigV4 again.  

### Executing the lambda
Once the lambda has been deployed, run the following to execute the lambda.

```
$ ./3-invoke.sh
```

This will continually invoke the lambda, until you cancel the execution.
You can read the full and complete logs for each lambda in the CloudWatch console.  

1. Go to AWS Cloudwatch console
2. Select Log groups.
2. Click the log group that starts with `/aws/lambda/KeyspacesLambdaExample-function-`
3. Each invocation produces a separate log.

### To remove Lambdas
run the following to remove the stack and users
```
$ ./4-cleanup.sh
```





