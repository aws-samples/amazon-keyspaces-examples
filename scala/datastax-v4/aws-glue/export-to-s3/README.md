## Using Glue Export Example
This example provides scala script for exporting Amazon Keyspaces table data to S3 using AWS Glue. This allows you to export data from Amazon Keyspaces without setting up a spark cluster.

## Prerequisites
* Amazon Keyspaces table to export
* Amazon S3 bucket to store backups
* Amazon S3 bucket to store job configuration and script

### Export to S3
The following example exports data to S3 using the spark-cassandra-connector. The script takes four parameters KEYSPACE_NAME, KEYSPACE_TABLE, S3_URI for backup files and FORMAT option (parquet, csv, json).  


```
object GlueApp {

  def main(sysArgs: Array[String]) {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    import com.datastax.spark.connector._
    import org.apache.spark.sql.cassandra._
    import sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "S3_URI", "FORMAT").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val backupS3 = args("S3_URI")
    val backupFormat = args("FORMAT")

    val tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, "keyspace" -> keyspaceName))
      .load()

    tableDf.write.format(backupFormat).mode(SaveMode.ErrorIfExists).save(backupS3)

    Job.commit()
  }
}

```


## Create IAM ROLE for AWS Glue
Create a new AWS service role named 'GlueKeyspacesExport' with AWS Glue as a trusted entity.

Included is a sample permissions-policy for executing Glue job. You can use managed policies AWSGlueServiceRole, AmazonKeyspacesReadOnlyAccess, read access to S3 bucket containing spack-cassandra-connector jar, configuration. Write access to S3 bucket containing backups.


## Cassandra driver configuration to connect to Amazon Keyspaces
The following configuration for connecting to Amazon Keyspaces with the spark-cassandra connector.

Using the RateLimitingRequestThrottler we can ensure that request do not exceed configured Keyspaces capacity. The G1.X DPU creates one executor per worker. The RateLimitingRequestThrottler in this example is set for 1000 request per second. With this configuration and G.1X DPU you will achieve 1000 request per Glue worker. Adjust the max-requests-per-second accordingly to fit your workload. Increase the number of workers to scale throughput to a table.

```
datastax-java-driver {
  basic.request.consistency = "LOCAL_QUORUM"
  basic.contact-points = [ "cassandra.us-east-1.amazonaws.com:9142"]

   advanced.reconnect-on-init = true

   basic.load-balancing-policy {
        local-datacenter = "us-east-1"
    }
    advanced.auth-provider = {
       class = PlainTextAuthProvider
       username = "user-at-sample"
       password = "S@MPLE=PASSWORD="
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
    advanced.connection.pool.local.size = 1
}
```

## Create S3 bucket to store job artifacts
The AWS Glue ETL job will need to access jar dependencies, driver configuration, and scala script. You can use the same bucket to store backups.
```
aws s3 mb s3://amazon-keyspaces-backups
```

## Upload job artifacts to S3
The job will require
* The spark-cassandra-connector to allow reads from Amazon Keyspaces. Amazon Keyspaces recommends version 2.5.2 of the spark-cassandra-connector or above. 
* application.conf containing the cassandra driver configuration for Keyspaces access
* export-sample.scala script containing the export code.

```
curl -L -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.11/2.5.2/spark-cassandra-connector-assembly_2.11-2.5.2.jar

aws s3api put-object --bucket amazon-keyspaces-backups --key jars/spark-cassandra-connector-assembly_2.11-2.5.2.jar --body spark-cassandra-connector-assembly_2.11-2.5.2.jar

aws s3api put-object --bucket amazon-keyspaces-backups --key conf/application.conf --body application.conf

aws s3api put-object --bucket amazon-keyspaces-backups --key scripts/export-sample.scala --body export-sample.scala

```
### Create AWS Glue ETL Job
You can use the following command to create a glue job using the script provided in this example. You can also take the parameters and enter them into the AWS Console.
```
aws glue create-job \
    --name "AmazonKeyspacesExport" \
    --role "GlueKeyspacesRestore" \
    --description "Export Amazon Keyspaces table to s3" \
    --glue-version "2.0" \
    --number-of-workers 5 \
    --worker-type "G.1X" \
    --command "Name=glueetl,ScriptLocation=s3://amazon-keyspaces-backups/scripts/export-sample.scala" \
    --default-arguments '{
        "--job-language":"scala",
        "--FORMAT":"parquet",
        "--KEYSPACE_NAME":"my_keyspace",
        "--TABLE_NAME":"my_table",
        "--S3_URI":"s3://amazon-keyspaces-backups/snap-shots/",
        "--extra-jars":"s3://amazon-keyspaces-backups/jars/spark-cassandra-connector-assembly_2.11-2.5.2.jar",
        "--extra-files":"s3://amazon-keyspaces-backups/conf/application.conf",
        "--conf":"spark.cassandra.connection.config.profile.path=application.conf",
        "--class":"GlueApp"
    }'
```
