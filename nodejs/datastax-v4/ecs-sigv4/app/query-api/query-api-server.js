// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

'use strict'

const AWS = require('aws-sdk')
const cassandra = require('cassandra-driver')
const fs = require('fs')
const sigV4 = require('aws-sigv4-auth-cassandra-plugin')
const log = require("loglevel")
const program = require('commander')
const express = require('express')

const defaultPort = (typeof process.env.PORT !== 'undefined')
  ? process.env.PORT
  : '80'

program
  .version('1.0.0')
  .option('-p, --port <port>', 'server port', defaultPort)
  .requiredOption('-r, --region <region>', 'region-id')
  .requiredOption('-k, --keyspace <keyspace>', 'keyspace-id')
  .requiredOption('-t, --table <table>', 'table-id')
  .parse(process.argv)

const options = program.opts();
const port = parseInt(options.port)
const region = options.region.trim()
const keyspace = options.keyspace.trim()
const table = options.table.trim()

log.setDefaultLevel(log.levels.TRACE);

function format(o, pretty) {
  return (pretty)
    ? JSON.stringify(o, null, 2) + '\n'
    : JSON.stringify(o);
}

async function executeQuery(query, res, pretty) {
  log.info('Executing query:' + query)
  const result = await cassandraClient.execute(query)
  res.set('Cache-Control', 'no-cache')   // Turn caching off for demo purposes
  res.type('json').send(format(result.rows, pretty))
  log.info("secret " + ecsCredentials.secretAccessKey)
  log.info("expiry: " + ecsCredentials.expireTime)
  log.info("time now: " + new Date())
}

async function runQueryApiServer() {
  try {
    // Check that table exists
    const result = await cassandraClient.execute(
      `SELECT status FROM system_schema_mcs.tables
        WHERE keyspace_name='${keyspace}' AND table_name='${table}'`
    )
    const status = result.first()['status']
    log.info(`${keyspace}.${table} status: ${status}`)
    if (status !=='ACTIVE')
      throw `${keyspace}.${table} is not ACTIVE`

    // Prepare server routing
    const app = express()
    app.use(express.json())
    app.use(express.urlencoded({extended: true}))
    app.get('/countries', (req, res) => {
      const query = `SELECT * FROM ${keyspace}.${table}`;
      let pretty = (typeof req.query.pretty !== 'undefined');
      executeQuery(query, res, pretty)
    })
    app.get('/countries/:id', (req, res) => {
      var id = req.params.id
      const query = `SELECT * FROM ${keyspace}.${table} WHERE id='${id}'`;
      let pretty = (typeof req.query.pretty !== 'undefined');
      executeQuery(query, res, pretty)
    })
    app.get('/ping', (req, res) => {
      res.send("ok")
    })
    log.info('Routing configured')

    // Start server
    const host = '0.0.0.0';
    const server = app.listen(port, host);
    log.info(`Service running on http://${host}:${port}`)

    // Set up SIGTERM processing
    process.on('SIGTERM', () => {
      log.info('SIGTERM signal received.');
      log.info('Closing http server.');
      server.close(() => {
        log.info('Http server closed.');
        cassandraClient.shutdown(() => {
          log.info('Cassandra client closed.');
          process.exit(0);
        })
      })
    })
  }
  catch (err) {
    log.error(err)
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

// Start the server
runQueryApiServer();
