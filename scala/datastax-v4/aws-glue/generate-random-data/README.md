## Generate random data Glue Example
This example provides scala script for generating random data and importing data to an Amazon Keyspaces table data using AWS Glue. This is useful for prototyping and testing without having an existing dataset. 

## Prerequisites
* Amazon Keyspaces table to import

### Create Table
The following example imports data to Amazon Keyspaces using the spark-cassandra-connector. The script takes two parameters KEYSPACE_NAME, KEYSPACE_TABLE. Start by creating a table.

```
CREATE KEYSPACE IF NOT EXISTS aws WITH REPLICATION = {'class': 'SingleRegionStrategy'}

CREATE TABLE IF NOT EXISTS aws.my_table_example (
	"id" text,
	"create_date" timestamp,
	"data" text,
	"count" bigint,
	PRIMARY KEY("id", "create_date"))
WITH CUSTOM_PROPERTIES = {
    'capacity_mode':{
        'throughput_mode':'PROVISIONED',
        'write_capacity_units':30000,
        'read_capacity_units':30000
    },
	'point_in_time_recovery':{
		'status':'enabled'
	},
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
} AND CLUSTERING ORDER BY("create_date" ASC)
```

[generate-sample.scala](generate-sample.scala)

## Update the partitioner for your account
In Apache Cassandra, partitioners control which nodes data is stored on in the cluster. Partitioners create a numeric token using a hashed value of the partition key. Cassandra uses this token to distribute data across nodes.  To use Apache Spark or AWS glue you may need to update the partitioner if set to DefaultPartitioner or RandomPartitioner to Mumur3Partitioner. You can execute this CQL command from the Amazon Keyspaces console [CQL editor](https://console.aws.amazon.com/keyspaces/home#cql-editor)

```
SELECT partitioner FROM system.local;

UPDATE system.local set partitioner='org.apache.cassandra.dht.Murmur3Partitioner' where key='local';
```
For more info see [Working with partitioners](https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html)


## Create IAM ROLE for AWS Glue
Create a new AWS service role named 'GlueKeyspacesImport' with AWS Glue as a trusted entity.

Included is a sample permissions-policy for executing Glue job. You can use managed policies AWSGlueServiceRole, AmazonKeyspacesFullAccess, read access to S3 bucket containing spack-cassandra-connector jar, and configuration.


## Cassandra driver configuration to connect to Amazon Keyspaces
The following configuration for connecting to Amazon Keyspaces with the spark-cassandra connector.

Using the RateLimitingRequestThrottler we can ensure that request do not exceed configured Keyspaces capacity. The G1.X DPU creates one executor per worker. The RateLimitingRequestThrottler in this example is set for 1000 request per second. With this configuration and G.1X DPU you will achieve 1000 request per Glue worker. Adjust the max-requests-per-second accordingly to fit your workload. Increase the number of workers to scale throughput to a table.

[cassandra-application.conf](cassandra-application.conf)



## Create S3 bucket to store job artifacts
The AWS Glue ETL job will need to access jar dependencies, driver configuration, and scala script.

```shell script
export ARTIFACT_BUCKET=amazon-keyspaces-artifacts-$(aws sts get-caller-identity --query Account --output text)
export SNAPSHOT_BUCKET=amazon-keyspaces-snapshots-$(aws sts get-caller-identity --query Account --output text)
export SHUFFLE_BUCKET=amazon-keyspaces-shuffle-$(aws sts get-caller-identity --query Account --output text)
```

```
aws s3 mb s3://ARTIFACT_BUCKET
```


## Create S3 bucket for Shuffle space
With NoSQL its common to shuffle large sets of data. This can overflow local disk.  With AWS Glue, you can  use Amazon S3 to store Spark shuffle and spill data. This solution disaggregates compute and storage for your Spark jobs, and gives complete elasticity and low-cost shuffle storage, allowing you to run your most shuffle-intensive workloads reliably.

```
aws s3 mb s3://SHUFFLE_BUCKET
```

## Upload job artifacts to S3
The job will require
* The spark-cassandra-connector to allow reads from Amazon Keyspaces. Amazon Keyspaces recommends version 2.5.2 of the spark-cassandra-connector or above.
* application.conf containing the cassandra driver configuration for Keyspaces access
* import-sample.scala script containing the import code.

In the following example you will need to create your own unique bucket names. This can be done by adding your own prefix.

```
curl -L -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.11/2.5.2/spark-cassandra-connector-assembly_2.11-2.5.2.jar

aws s3api put-object --bucket $ARTIFACT_BUCKET --key jars/spark-cassandra-connector-assembly_2.11-2.5.2.jar --body spark-cassandra-connector-assembly_2.11-2.5.2.jar

aws s3api put-object --bucket $ARTIFACT_BUCKET --key conf/cassandra-application.conf --body cassandra-application.conf

aws s3api put-object --bucket $ARTIFACT_BUCKET --key scripts/generate-sample.scala --body generate-sample.scala 

```
### Create AWS Glue ETL Job
You can use the following command to create a glue job using the script provided in this example. You can also take the parameters and enter them into the AWS Console.

```
aws glue create-job \
    --name "AmazonKeyspacesRandomDataImport" \
    --role "GlueKeyspacesImport" \
    --description "Import Random data into Amazon Keyspaces" \
    --glue-version "3.0" \
    --number-of-workers 30 \
    --worker-type "G.1X" \
    --command "Name=glueetl,ScriptLocation=s3://$ARTIFACT_BUCKET/scripts/generate-sample.scala" \
    --default-arguments '{
        "--job-language":"scala",
        "--KEYSPACE_NAME":"aws",
        "--TABLE_NAME":"my_table_example",
        "--S3_URI":"s3://SNAPSHOT_BUCKET/snapshots/random/2023-08-03/",
        "--DRIVER_CONF":"cassandra-application.conf",
        "--user-jars-first":"true",
        "--extra-jars":"s3://$ARTIFACT_BUCKET/jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar",
        "--extra-files":"s3://$ARTIFACT_BUCKET/conf/cassandra-application.conf",
        "--enable-continuous-cloudwatch-log":"true",
        "--write-shuffle-files-to-s3":"true",
        "--write-shuffle-spills-to-s3":"true",
        "--TempDir":"s3://$SHUFFLE_BUCKET",
        "--class":"GlueApp"
    }'
```
