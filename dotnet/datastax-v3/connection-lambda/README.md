
## Using Amazon Keyspaces with Lambda (C#/.NET)

This is a sample project showing best practices for connecting to Amazon Keyspaces via AWS Lambda (C#/.NET).

The project source includes function code and supporting resources:

-   ` src/keyspaces-lambda-csharp`  - A C# .NET Core function.
-   `template.yml`  - An AWS CloudFormation template that creates an application.
-   `1-create-bucket.sh`,  `2-deploy.sh`, etc. - Shell scripts that use the AWS CLI to deploy and manage the application.

Use the following instructions to deploy the sample application. For more information on the application's architecture and implementation, see  [Managing Spot Instance Requests](https://docs.aws.amazon.com/lambda/latest/dg/services-ec2-tutorial.html)  in the developer guide.

## Requirements
-   [.NET Core SDK 3.1](https://dotnet.microsoft.com/download/dotnet-core/3.1)
-   [AWS extensions for .NET CLI](https://github.com/aws/aws-extensions-for-dotnet-cli)
-   The Bash shell. For Linux and macOS, this is included by default. In Windows 10, you can install the  [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)  to get a Windows-integrated version of Ubuntu and Bash.
-   [The AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)  v1.17 or newer.

If you use the AWS CLI v2, add the following to your  [configuration file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)  (`~/.aws/config`):

```
cli_binary_format=raw-in-base64-out

```

This setting enables the AWS CLI v2 to load JSON events from a file, matching the v1 behavior.

## Running Tests

### 1. Environment Variables (Developer Machine)

To connect to Amazon Keyspaces using Transport Layer Security (TLS), you need an Amazon digital certificate (present in the project's root directory as `AmazonRootCA1.pem`) and configure the following environment variables: 

- `SERVICE_USER_NAME` and `SERVICE_PASSWORD`: should match the user name and password you obtained when you generated the service-specific credentials by following the steps to  [Generate Service-Specific Credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html#programmatic.credentials.ssc).
- `CERTIFICATE_PATH`: absolute path to the Amazon digital certificate file (`AmazonRootCA1.pem`).

### 2. Run the tests

Then execute the test by using the .NET Core CLI

```sh
$ dotnet test test/keyspaces-lambda-csharp.Tests/
```
## Running the Sample Application
### 0. Prerequisites
There are some provided scripts for you to install, deploy and configure Lambda to your existing AWS account.

Before running the scripts you need to load a profile file (or set environment variables) that grant access to AWS to create S3, IAM roles, and to setup Lambda functions, see [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

### 1. Create S3 bucket
To create the S3 bucket used to deploy Lambda code, run the following on the command line.  You only need to run this once. 

```
$ ./1-create-bucket.sh
```

### 2. Deploy and setup the Lambda function
next invoke:

```
$ ./2-deploy.sh 
```

This constructs a Lambda function, with the current code in it.  

The example code uses credentials that are meant to be specified either by environment variable or execution role.  After you have run this for the first time, you also have to set the appropriate variables through the AWS CLI or in the AWS Lambda Console.

### 3. Configure the Lambda function (Environment Variables)

#### A) Through AWS CLI
To set the environment variables through the AWS CLI, edit the `3-configure.sh` script:

```
aws lambda update-function-configuration --function-name $FUNCTION \
    --environment "Variables={SERVICE_USER_NAME=ServiceUserName,SERVICE_PASSWORD=ServicePassword,CERTIFICATE_PATH=.}"
```
Ensure that the  `ServiceUserName`  and  `ServicePassword`  match the user name and password you obtained when you generated the service-specific credentials by following the steps to  [Generate Service-Specific Credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html#programmatic.credentials.ssc). CERTIFICATE_PATH should be `.` (current working directory).

and then execute the script:
```
$ ./3-configure.sh
```

#### B) Through AWS Lambda Console
1.  Go to the AWS Lambda Console.
2.  In the console locate the function with the prefix `keyspaces-lambda-csharp-function-`  and select the name.
3.  Under __Environment Variables__ select the edit button.
4.  Fill out the correct `SERVICE_USER_NAME` , `SERVICE_PASSWORD` and `CERTIFICATE_PATH`.

Ensure that the  `SERVICE_USER_NAME`  and  `SERVICE_PASSWORD`  match the user name and password you obtained when you generated the service-specific credentials by following the steps to  [Generate Service-Specific Credentials](https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html#programmatic.credentials.ssc). CERTIFICATE_PATH should be `.` (current working directory).

### 4. Executing the Lambda Function
Once the Lambda function has been deployed and configured, run the following:

```
$ ./4-invoke.sh
```
Output:
```
{
    "ExecutedVersion": "$LATEST",
    "StatusCode": 200
}
"200 OK"
```

This continually invokes the Lambda function, until you cancel.
You can read the full and complete logs for each Lambda function in the CloudWatch console.  

1. Go to AWS Cloudwatch console
2. Choose Log groups.
2. Select the log group that starts with `/aws/lambda/keyspaces-lambda-csharp-function-`
3. Each invocation produces a separate log.

### 5. Cleanup
Run the following to remove the stack and other artifacts:
```
$ ./5-cleanup.sh
```