## Using Glue Top N partitions Example
This example provides scala script for calculate the top partitions with the most rows. This is common utility in verifying data skew. You can use this with Amazon Keyspaces or self managed Apache Cassandra. 

## Prerequisites
* Amazon Keyspaces table to perform Top N
* Amazon S3 bucket to store required jars
* Amazon S3 bucket to store job configuration and script
* Amazon S3 bucket to store glue shuffle data

### Performing Top N for rows in a partition
The following example uses the spark-cassandra-connector. With this script you can provide the TOP N for single or multi value partition keys.   

## Update the partitioner for your account
In Apache Cassandra, partitioners control which nodes data is stored on in the cluster. Partitioners create a numeric token using a hashed value of the partition key. Cassandra uses this token to distribute data across nodes.  To use Apache Spark or AWS glue you may need to update the partitioner if set to DefaultPartitioner or RandomPartitioner to Mumur3Partitioner. You can execute this CQL command from the Amazon Keyspaces console [CQL editor](https://console.aws.amazon.com/keyspaces/home#cql-editor)

```
SELECT partitioner FROM system.local;

UPDATE system.local set partitioner='org.apache.cassandra.dht.Murmur3Partitioner' where key='local';
```
For more info see [Working with partitioners](https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html)


## Create IAM ROLE for AWS Glue
Create a new AWS service role named 'GlueKeyspacesRole' with AWS Glue as a trusted entity.

Included is a sample permissions-policy for executing Glue job. You can use managed policies AWSGlueServiceRole, AmazonKeyspacesReadOnlyAccess, read access to S3 bucket containing spack-cassandra-connector jar and configuration.


## Cassandra driver configuration to connect to Amazon Keyspaces
The following configuration for connecting to Amazon Keyspaces with the spark-cassandra connector.

Using the RateLimitingRequestThrottler we can ensure that request do not exceed configured Keyspaces capacity. The G1.X DPU creates one executor per worker. The RateLimitingRequestThrottler in this example is set for 1000 request per second. With this configuration and G.1X DPU you will achieve 1000 request per Glue worker. Adjust the max-requests-per-second accordingly to fit your workload. Increase the number of workers to scale throughput to a table.

```

datastax-java-driver {
  basic.request.consistency = "LOCAL_ONE"
  basic.request.default-idempotence = true
  basic.contact-points = [ "cassandra.us-east-1.amazonaws.com:9142"]
  advanced.reconnect-on-init = true

   basic.load-balancing-policy {
        local-datacenter = "us-east-1"
     }

   advanced.auth-provider = {
       class = PlainTextAuthProvider
       username = "user-at-sample"
       password = "SAMPLE#PASSWORD"
    }

    advanced.throttler = {
      class = RateLimitingRequestThrottler
      max-requests-per-second = 1000
      max-queue-size = 50000
      drain-interval = 1 millisecond
    }

   advanced.ssl-engine-factory {
      class = DefaultSslEngineFactory
      hostname-validation = false
    }

    advanced.connection.pool.local.size = 2
}

```

## Create S3 bucket to store job artifacts
The AWS Glue ETL job will need to access jar dependencies, driver configuration, and scala script.  You can use the same bucket to store backups.
```
aws s3 mb s3://amazon-keyspaces-artifacts
```

## Create S3 bucket for shuffle space
With NoSQL its common to shuffle large sets of data. This can overflow local disk.  With AWS Glue, you can  use Amazon S3 to store Spark shuffle and spill data. This solution disaggregates compute and storage for your Spark jobs, and gives complete elasticity and low-cost shuffle storage, allowing you to run your most shuffle-intensive workloads reliably.

```
aws s3 mb s3://amazon-keyspaces-glue-shuffle
```

## Upload job artifacts to S3
The job will require
* The spark-cassandra-connector to allow reads from Amazon Keyspaces. Amazon Keyspaces recommends version 2.5.2 or above of the spark-cassandra-connector.
* creation of S3 buckets
* cassandra-application.conf containing the cassandra driver configuration for Keyspaces access
* top-partition-example.scala script containing the top-N code.

```
curl -L -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.11/2.5.2/spark-cassandra-connector-assembly_2.11-2.5.2.jar

aws s3api put-object --bucket amazon-keyspaces-artifacts --key jars/spark-cassandra-connector-assembly_2.11-2.5.2.jar --body spark-cassandra-connector-assembly_2.11-2.5.2.jar

aws s3api put-object --bucket amazon-keyspaces-artifacts --key conf/cassandra-application.conf --body cassandra-application.conf

aws s3api put-object --bucket amazon-keyspaces-artifacts --key scripts/top-partition-example.scala --body top-partition-example.scala

```
### Create AWS Glue ETL Job
You can use the following command to create a glue job using the script provided in this example. You can also take the parameters and enter them into the AWS Console.
```
aws glue create-job \
    --name "AmazonKeyspacesTopPartition" \
    --role "GlueKeyspacesRestore" \
    --description "Calculate top partitions with the most Rows in an Amazon Keyspaces table" \
    --glue-version "2.0" \
    --number-of-workers 5 \
    --worker-type "G.1X" \
    --command "Name=glueetl,ScriptLocation=s3://amazon-keyspaces-artifacts/scripts/top-partition-example.scala" \
    --default-arguments '{
        "--job-language":"scala",
        "--AWS_REGION":"us-east-1",
        "--KEYSPACE_NAME":"my_keyspace",
        "--TABLE_NAME":"my_table",
        "--DRIVER_CONF":"cassandra-application.conf",
        "--MAX_RESULTS":"10",
        "--MIN_SIZE":"1",
        "--GROUPBY":"pk1,pk2",
        "--user-jars-first":"true",
        "--extra-jars":"s3://amazon-keyspaces-artifacts/jars/spark-cassandra-connector-assembly_2.11-2.5.2.jar",
        "--extra-files":"s3://amazon-keyspaces-artifacts/conf/cassandra-application.conf",
        "--enable-continuous-cloudwatch-log":"true",
        "--write-shuffle-files-to-s3":"true",
        "--write-shuffle-spills-to-s3":"true",
        "--TempDir":"s3://amazon-keyspaces-glue-shuffle",
        "--class":"GlueApp"
    }'
```
