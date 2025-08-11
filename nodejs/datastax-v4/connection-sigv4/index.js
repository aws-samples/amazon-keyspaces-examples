// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

// Variables
const cassandra = require('cassandra-driver');
const fs = require('fs');
const sigV4 = require('aws-sigv4-auth-cassandra-plugin');
// custom retry policy for AmazonKeyspaces to retry on same host
const custom_retry = require('./retry/AmazonKeyspacesRetryPolicy.js');
// Max number of retry attempts for custom retry
const Max_retry_attempts = 10
require('dotenv').config();
const region = process.env.AWS_REGION;
const accessKey = process.env.AWS_ACCESS_KEY_ID;
const secretKey = process.env.AWS_SECRET_ACCESS_KEY;

// Check that environment variables are not undefined
if (!region) {
    console.log("You do not have a region set. Set environment variable AWS_REGION");
    process.exit(1);
}

if (!accessKey) {
    console.log("You do not have an access key set. Set environment variable AWS_ACCESS_KEY_ID");
    process.exit(1);
}

if (!secretKey) {
    console.log("You do not have a secret key set. Set environment variable AWS_SECRET_ACCESS_KEY");
    process.exit(1);
}

const auth = new sigV4.SigV4AuthProvider({
    region: region,
    accessKeyId: accessKey,
    secretAccessKey: secretKey
});

const host = 'cassandra.' + region + '.amazonaws.com'
const sslOptions = {
    ca: [
        fs.readFileSync(__dirname + '/resources/sf-class2-root.crt')
    ],
    host: host,
    rejectUnauthorized: true
};

const client = new cassandra.Client({
    contactPoints: [host],
    localDataCenter: region,
    authProvider: auth,
    sslOptions: sslOptions,
    queryOptions: { isIdempotent: true, consistency: cassandra.types.consistencies.localQuorum },
    policies: { retry: new custom_retry.AmazonKeyspacesRetryPolicy(Max_retry_attempts) },
    protocolOptions: { port: 9142 }
});

const query = 'SELECT * FROM system_schema.keyspaces';

const result = client.execute(query).then(
    result => console.log('Row from Keyspaces %s', result.rows[0])
).catch(
    e => console.log(`${e}`)
);

Promise.allSettled([result]).finally(() => client.shutdown());