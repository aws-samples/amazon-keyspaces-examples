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

Finally, run `node index.js` to start the sample.