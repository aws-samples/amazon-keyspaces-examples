// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict'

const AWS = require('aws-sdk')
const cassandra = require('cassandra-driver')
const fs = require('fs')
const sigV4 = require('aws-sigv4-auth-cassandra-plugin')
const log = require("loglevel")
const program = require('commander')

program
  .version('1.0.0')
  .requiredOption('-r, --region <region>', 'region-id')
  .requiredOption('-k, --keyspace <keyspace>', 'keyspace-id')
  .requiredOption('-t, --table <table>', 'table-id')
  .parse(process.argv)

const options = program.opts()
const region = options.region.trim()
const keyspace = options.keyspace.trim()
const table = options.table.trim()

log.setDefaultLevel(log.levels.TRACE);

// Function that waits for table status to be ACTIVE
async function tableCreated(keyspace, table) {
  const query = `SELECT status
    FROM system_schema_mcs.tables
    WHERE keyspace_name='${keyspace}' AND table_name='${table}'`
  let status = 'CREATING';
  do {
    const result = await cassandraClient.execute(query)
    const firstRow = result.first()
    if (firstRow) status = firstRow['status']
    log.info(`${keyspace}.${table} status: ${status}`)
    if (status !== 'ACTIVE') await new Promise(r => setTimeout(r, 1000)); // Wait 1 sec and try again
  } while (status!=='ACTIVE')
}

async function runLoadData() {
  try {
    // Create table
    await cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS ${keyspace}.countries(
        id text PRIMARY KEY,
        name text,
        capital text,
        population int,
        area int
      );`
    )

    // Wait for table creation to finish by polling status
    await tableCreated(keyspace, table)

    // Load data
    const insertQuery = `INSERT INTO ${keyspace}.${table} (id, name, capital, population, area)
      VALUES (?, ?, ?, ?, ?)`
    const insertParameters = [
      ["cn","China","Beijing",1398000000,9597000],
      ["in","India","New Delhi",1366000000,3287000],
      ["gb","United Kingdon","London",66650000,242495],
      ["mt","Malta","Valletta",502653,316],
      ["us","United States","Washington DC",328200000,9834000]
    ];
    const results = await cassandra.concurrent.executeConcurrent(
      cassandraClient, insertQuery, insertParameters
    )
    log.info(results)   
  }
  catch (err) {
    log.error(err)
  }
  finally {
    if (cassandraClient)
      await cassandraClient.shutdown()
  }
}

// Set up ECS credentials
const ecsCredentials = new AWS.ECSCredentials({
  httpOptions: { timeout: 3000 }, // 5 second timeout
  maxRetries: 5, // retry 10 times
  retryDelayOptions: { base: 200 } // see AWS.Config for information
})

// Set up provider chain to obtain creds for ECS task
AWS.CredentialProviderChain.defaultProviders = [ ecsCredentials ]
log.info("Chain default providers:" + AWS.CredentialProviderChain.defaultProviders)

// Set up Cassandra client
const auth = new sigV4.SigV4AuthProvider({
  region: region
})
const sslOptions1 = {
  ca: [
    fs.readFileSync('./sf-class2-root.crt', 'utf-8')
  ],      
  host: 'cassandra.' + region + '.amazonaws.com',
  rejectUnauthorized: true
}
const cassandraClient = new cassandra.Client({
  contactPoints: ['cassandra.'+region+'.amazonaws.com'],
  localDataCenter: region,
  authProvider: auth,
  sslOptions: sslOptions1,
  protocolOptions: { port: 9142 },
  queryOptions: {
    consistency: cassandra.types.consistencies.localQuorum
  }
})

// Load the data
runLoadData();