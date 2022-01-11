## Connecting to Amazon Keyspaces with Golang

This sample project shows the use of the GOCQL Golang Driver for Apache Cassandra using SigV4.

### Prerequisites
You should have Golang installed.

You should also setup Amazon Keyspaces with allow access from a specific IAM user/role.  See [Accessing Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/accessing.html) for more.

### Running the sample

This sample uses the GO AWS SDKs which will find credentials based on environment variables, config definitions or a credentials file see [AWS SDK for GO](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) for more information regarding how to configure this.

You can quickly get this to run by explicitly setting the following environment variables...

- AWS_REGION  (for example 'us-east-1)
- AWS_ACCESS_KEY_ID  (ex: 'AKIAIOSFODNN7EXAMPLE')
- AWS_SECRET_ACCESS_KEY (ex: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')


### Install the dependencies 

From the project directory... 
```
$ go mod tidy
```

### Testing
From this project directory...
```
$ go run main.go
```
