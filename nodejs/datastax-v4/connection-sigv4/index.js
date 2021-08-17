// Variables
const cassandra = require('cassandra-driver');
const fs = require('fs');
const sigV4 = require('aws-sigv4-auth-cassandra-plugin');
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
    protocolOptions: { port: 9142 }
});

const query = 'SELECT * FROM system_schema.keyspaces';

client.execute(query).then(
    result => console.log('Row from Keyspaces %s', result.rows[0])
).catch(
    e => console.log(`${e}`)
);