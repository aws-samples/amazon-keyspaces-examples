## Connecting to Amazon Keyspaces with NodeJS

This sample project shows the use of the DataStax NodeJS Driver for Apache Cassandra using SigV4.

### Prerequisites

You should have NodeJS and NPM installed. This sample used NodeJS 14.x and NPM 6.x.

You should also setup Amazon Keyspaces with an IAM user. See [Accessing Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/accessing.html) for more.

### Running the Sample

To run this sample, create a .env file in the same directory as this README.
In your .env file, define the following environment variables:
- AWS_REGION (ex: 'us-east-1')
- AWS_ACCESS_KEY_ID (ex: 'AKIAIOSFODNN7EXAMPLE')
- AWS_SECRET_ACCESS_KEY (ex: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

Install the project's dependencies by running `npm install` in this directory.

#### Retry Policies
The nodejs driver will attempt to retry idempotent request transparently to the application. If you are seeing NoHostAvailableException when using Amazon Keyspaces, replacing the default retry policy with the ones provided in this repository will be beneficial.

Implementing a driver retry policy is not a replacement for an application level retry. Users of Apache Cassandra or Amazon Keyspaces should implement an application level retry mechanism for request that satisfy the applications business requirements. Additionally, adding complex logic, sleeps or blocking calls in a Driver retry policy should be used with caution.
###### AmazonKeyspacesRetryPolicy
The Amazon Keyspaces Retry Policy is an alternative to the DefaultRetryPolicy for the Cassandra nodejs driver. The main difference from the DefaultRetryPolicy, is the AmazonKeyspacesRetryPolicy will retry request a configurable number of times. By default, we take a conservative approach of 3 retry attempts. This driver retry policy will not throw a NoHostAvailableException. Instead, this retry policy will pass back the original exception sent back from the service.

The following code shows how to include the AmazonKeyspacesRetryPolicy to existing configuration

const client = new cassandra.Client({
                   contactPoints: ['cassandra.us-east-2.amazonaws.com'],
                   localDataCenter: 'us-east-2',
                   policies: { retry: new retry.AmazonKeyspacesRetryPolicy(Max_retry_attempts) },
                   queryOptions: { isIdempotent: true, consistency: cassandra.types.consistencies.localQuorum },
                   authProvider: auth,
                   sslOptions: sslOptions1,
                   protocolOptions: { port: 9142 }
});

Finally, run `node index.js` to start the sample.