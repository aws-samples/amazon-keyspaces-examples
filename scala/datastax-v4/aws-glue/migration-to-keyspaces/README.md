# Migration to Amazon Keyspaces without a hassle
This example provides scala scripts for migrating the Cassandra workload to Amazon Keyspaces (for Apache Cassandra) using AWS Glue. 
This allows you to migrate data from the Cassandra cluster to Amazon Keyspaces without setting up and provisioning a spark cluster.

## Prerequisites
* Cassandra source table
* Amazon Keyspaces's target table to replicate the workload 
* Amazon S3 bucket to store intermediate parquet files with incremental data changes
* Amazon S3 bucket to store job configuration and scripts

## Getting started 
### Create a target keyspace and table in Amazon Keyspaces Console

`CREATE KEYSPACE target_keyspace WITH replication = {'class': 'SingleRegionStrategy'}`

`CREATE TABLE target_keyspace.target_table (
    userid uuid,
    level text,
    gameid int,
    description text,
    nickname text,
    zip text,
    email text,
    updatetime text,
PRIMARY KEY (userid, level, gameid)
) WITH default_time_to_live = 0 AND CUSTOM_PROPERTIES = 
    {'capacity_mode':{ 'throughput_mode':'PROVISIONED',
                       'write_capacity_units':76388,
                       'read_capacity_units':3612 }} 
  AND CLUSTERING ORDER BY (level ASC, gameid ASC)`

After the table was created consider to switch the table to on-demand mode to avoid unnecessary charges. 

The following script will update the throughput mode.

`ALTER TABLE target_keyspace.target_table WITH CUSTOM_PROPERTIES = {
    'capacity_mode':{ 'throughput_mode':'PAY_PER_REQUEST'}
}`

### Export the Cassandra workload to S3 in parquet format
The following example offloads data to S3 using the spark-cassandra-connector. 
The script takes four parameters KEYSPACE_NAME, KEYSPACE_TABLE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, and S3_URI_NEW_CHANGE.

```scala
object GlueApp {
  def main(sysArgs: Array[String]) {
      
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import com.datastax.spark.connector._
    import org.apache.spark.sql.cassandra._
    import sparkSession.implicits._
    
    // @params: [JOB_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI_FULL_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_NEW_CHANGE]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "S3_URI_FULL_CHANGE", "S3_URI_CURRENT_CHANGE", "S3_URI_NEW_CHANGE").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val fullDataset = args("S3_URI_FULL_CHANGE")
    val incrementalCurrentDataset = args("S3_URI_CURRENT_CHANGE")
    val incrementalNewDataset = args("S3_URI_NEW_CHANGE")
    def checkS3(path:String):Boolean = {
      FileSystem.get(URI.create(path), spark.hadoopConfiguration).exists(new Path(path))
    }

    val dfSourceAsIs = spark.cassandraTable(keyspaceName, tableName)
                            .select("userid","level","gameid","description","nickname","zip","email","updatetime", WriteTime("email") as "writeTime")
    
    // Cast to Spark datatypes, for example, all UDTs to String
    val dfSourceWithCastDataTypes = dfSourceAsIs.keyBy(row => (row.getString("userid"), 
                                                               row.getString("level"), 
                                                               row.getInt("gameid"), 
                                                               row.getString("description"), 
                                                               row.getString("nickname"), 
                                                               row.getString("zip"), 
                                                               row.getString("email"), 
                                                               row.getString("updatetime"), 
                                                               row.getStringOption("writeTime")))
                                                .map(x => x._1)
                                                .toDF("userid","level","gameid","description","nickname","zip","email","updatetime","writeTime")
    
    // Persist full dataset in parquet format to S3
    dfSourceWithCastDataTypes.drop("writeTime")
                             .write
                             .mode(SaveMode.Overwrite)
                             .parquet(fullDataset)
    
    // Persist primarykeys and column writetimes to S3
    if(checkS3(incrementalCurrentDataset) && checkS3(incrementalNewDataset))
    {
        val shortDataframeT0 = sparkSession.read.parquet(incrementalCurrentDataset)
        val shortDataframeT1 = sparkSession.read.parquet(incrementalNewDataset)
        shortDataframeT1.select("userid","level","gameid","writeTime")
                        .write.mode(SaveMode.Overwrite).parquet(incrementalCurrentDataset)
        dfSourceWithCastDataTypes.select("userid","level","gameid","writeTime")
                                  .write.mode(SaveMode.Overwrite).parquet(incrementalNewDataset)
    }

    if(checkS3(incrementalCurrentDataset) && !checkS3(incrementalNewDataset))
    {
        dfSourceWithCastDataTypes.select("userid","level","gameid","writeTime")
                                 .write.mode(SaveMode.Overwrite).parquet(incrementalNewDataset)
    }
          
    Job.commit()
  }
}
```

### Import the incremental workload to Amazon Keyspaces
The following example reads the incremental parquet files from S3. 
The script takes four parameters KEYSPACE_NAME, KEYSPACE_TABLE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, and S3_URI_NEW_CHANGE.

```scala
object S3ToKeyspaces {
  
  def main(sysArgs: Array[String]) {
      
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import com.datastax.spark.connector._
    import org.apache.spark.sql.cassandra._
    import sparkSession.implicits._
    
    // @params: [JOB_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI_FULL_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_NEW_CHANGE]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "S3_URI_FULL_CHANGE", "S3_URI_CURRENT_CHANGE", "S3_URI_NEW_CHANGE").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    def checkS3(path:String):Boolean = {
      FileSystem.get(URI.create(path), spark.hadoopConfiguration).exists(new Path(path))
    }

    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val fullDataset = args("S3_URI_FULL_CHANGE")
    val incrementalCurrentDataset = args("S3_URI_CURRENT_CHANGE")
    val incrementalNewDataset = args("S3_URI_NEW_CHANGE")
    val fullDf = sparkSession.read.parquet(fullDataset)
    
    if(checkS3(incrementalCurrentDataset) && !checkS3(incrementalNewDataset))
    {
        fullDf.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()
        
    }
    
    if(checkS3(incrementalCurrentDataset) && checkS3(incrementalNewDataset))
    {
        val shortDataframeT1 = sparkSession.read.parquet(incrementalNewDataset)
        val shortDataframeT0 = sparkSession.read.parquet(incrementalCurrentDataset)
        
        val inserts = shortDataframeT1.as("T1").join(shortDataframeT0.as("T0"), 
                                                $"T1.userid" === $"T0.userid" && 
                                                $"T1.level" === $"T0.level" && 
                                                $"T1.gameid" === $"T0.gameid", "leftanti")
        val finalInserts = inserts.as("INSERTED").join(fullDf.as("ORIGINAL"), 
                                                $"INSERTED.userid" === $"ORIGINAL.userid"  && 
                                                $"INSERTED.level" === $"ORIGINAL.level" && 
                                                $"INSERTED.gameid" === $"ORIGINAL.gameid" , "inner").selectExpr("ORIGINAL.*").drop("writeTime")
        finalInserts.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()
        
        val updates = shortDataframeT0.as("T0").join(shortDataframeT1.as("T1"), 
                                                $"T1.userid" === $"T0.userid" && 
                                                $"T1.level" === $"T0.level" && 
                                                $"T1.gameid" === $"T0.gameid", "inner")
                                               .filter($"T1.writeTime">$"T0.writetime").select($"T1.userid",$"T1.name",$"T1.endpointid", $"T1.writeTime")
        val finalUpdates = updates.as("UPDATED").join(fullDf.as("ORIGINAL"), 
                                                $"UPDATED.userid" === $"ORIGINAL.userid" && 
                                                $"UPDATED.level" === $"ORIGINAL.level" && 
                                                $"UPDATED.gameid" === $"ORIGINAL.gameid", "inner").selectExpr("ORIGINAL.*").drop("writeTime")
        finalUpdates.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()
        
        val finalDeletes = shortDataframeT0.as("T0").join(shortDataframeT1.as("T1"), 
                                                     $"T1.userid" === $"T0.userid" && 
                                                     $"T1.level" === $"T0.level" && 
                                                     $"T1.gameid" === $"T0.gameid", "leftanti").drop("writeTime")

        finalDeletes.rdd.foreach(d=> 
        {
            val userid = d.get(0).toString
            val level = d.get(1).toString
            val endpointid = d.get(2).toString
            val whereClause = f"$userid%s and '$level%s' and $endpointid%s"
            spark.cassandraTable(keyspaceName, tableName).where(whereClause).deleteFromCassandra(keyspaceName, tableName)
        })
    }      
    Job.commit()
  }
}

```
## Create a network connection for AWS Glue
Create a new Glue connection to connect to the Cassandra cluster.
 
```shell script
aws glue create-connection --connection-input '{ 
         "Name":"conn-cassandra-custom",
         "Description":"Connection to Cassandra cluster",
         "ConnectionType":"NETWORK",
         "ConnectionProperties":{
             "JDBC_ENFORCE_SSL": "false"
         },
         "PhysicalConnectionRequirements":{
             "SubnetId":"subnet-ee1111d1EXAMPLE",
             "SecurityGroupIdList":["sg-0f0f0f000000f000fSAMPLE"],
             "AvailabilityZone":"us-east-1e"}
 }' --region us-east-1 --endpoint https://glue.us-east-1.amazonaws.com
```

## Cassandra driver configuration to connect to Cassandra
The following configuration for connecting to Cassandra with the spark-cassandra connector.
```yaml
datastax-java-driver {
  basic.request.consistency = "LOCAL_QUORUM"
  basic.contact-points = ["127.0.0.1:9042"]
   advanced.reconnect-on-init = true
   basic.load-balancing-policy {
        local-datacenter = "datacenter1"
    }
    advanced.auth-provider = {
       class = PlainTextAuthProvider
       username = "user-at-sample"
       password = "S@MPLE=PASSWORD="
    }
}
```

## Cassandra driver configuration to connect to Amazon Keyspaces
The following configuration for connecting to Amazon Keyspaces with the spark-cassandra connector.
Using the RateLimitingRequestThrottler we can ensure that request do not exceed configured Keyspaces capacity. The G1.X DPU creates one executor per worker. The RateLimitingRequestThrottler in this example is set for 1000 request per second. 
With this configuration and G.1X DPU you will achieve 1000 request per Glue worker. Adjust the max-requests-per-second accordingly to fit your workload. Increase the number of workers to scale throughput to a table.

```yaml
datastax-java-driver {
  basic.request.consistency = "LOCAL_QUORUM"
  basic.contact-points = ["cassandra.us-east-1.amazonaws.com:9142"]
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

```shell script
MIGRATION_BUCKET=aws-migration-$(dbus-uuidgen)
aws s3 mb s3://$MIGRATION_BUCKET
```

## Upload job artifacts to S3
The job will require:
* the spark-cassandra-connector to allow reads from Amazon Keyspaces
* CassandraConnector.conf KeyspacesConnector.conf containing the cassandra driver configuration for Cassandra and Keyspaces access
* CassandraToS3 and S3toKeyspaces scripts containing the migration code

```shell script
curl -L -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.2.0/spark-cassandra-connector-assembly_2.12-3.2.0.jar
aws s3api put-object --bucket $MIGRATION_BUCKET --key jars/spark-cassandra-connector-assembly_2.12-3.2.0.jar --body spark-cassandra-connector-assembly_2.12-3.2.0.jar
aws s3api put-object --bucket $MIGRATION_BUCKET --key conf/KeyspaacesConnector.conf --body KeyspacesConnector.conf
aws s3api put-object --bucket $MIGRATION_BUCKET --key conf/CassandraConnector.conf --body CassandraConnector.conf
aws s3api put-object --bucket $MIGRATION_BUCKET --key scripts/CassandraToS3.scala --body CassandraToS3.scala
aws s3api put-object --bucket $MIGRATION_BUCKET --key scripts/S3toKeyspaces.scala --body S3toKeyspaces.scala
```

## Create IAM ROLE for AWS Glue
Create a new AWS service role named 'GlueKeyspacesMigration' with AWS Glue as a trusted entity.

Included is a sample permissions-policy for executing Glue job. Read access to S3 bucket containing spack-cassandra-connector jar, configuration files for Amazon Keyspaces and the Cassandra. 
Read and write access to S3 bucket containing intermediate data.
```shell script
sed 's/amazon-keyspaces-migration-bucket/'$MIGRATION_BUCKET'/g' permissions-policy-template.json > permissions-policy.json
```

### Create AWS Glue ETL Job to read data from Cassandra
You can use the following command to create a glue job using the script provided in this example. You can also take the parameters and enter them into the AWS Console.
```shell script
aws glue create-job \
    --name "CassandraToS3" \
    --role "GlueKeyspacesMigration" \
    --description "Offload data from the Cassandra to S3" \
    --glue-version "3.0" \
    --number-of-workers 2 \
    --worker-type "G.1X" \
    --connections "conn-cassandra-custom" \
    --command "Name=glueetl,ScriptLocation=s3://$MIGRATION_BUCKET/scripts/CassandraToS3.scala" \
    --max-retries 0 \
    --default-arguments '{
        "--job-language":"scala",
        "--KEYSPACE_NAME":"source_keyspace",
        "--TABLE_NAME":"source_table",
        "--S3_URI_FULL_CHANGE":"s3://$MIGRATION_BUCKET/full-dataset/",
        "--S3_URI_CURRENT_CHANGE":"s3://$MIGRATION_BUCKET/incremental-dataset/current/",
        "--S3_URI_NEW_CHANGE":"s3://$MIGRATION_BUCKET/incremental-dataset/new/",
        "--extra-jars":"s3://$MIGRATION_BUCKET/jars/spark-cassandra-connector-assembly_2.12-3.2.0.jar",
        "--extra-files":"s3://$MIGRATION_BUCKET/conf/CassandraConnector.conf",
        "--conf":"spark.cassandra.connection.config.profile.path=CassandraConnector.conf",
        "--class":"GlueApp"
    }'
```

### Create AWS Glue ETL Job to write incremental to Amazon Keyspaces
You can use the following command to create a glue job using the script provided in this example. You can also take the parameters and enter them into the AWS Console.
```shell script
aws glue create-job \
    --name "S3toKeyspaces" \
    --role "GlueKeyspacesMigration" \
    --description "Push data to Amazon Keyspaces" \
    --glue-version "3.0" \
    --number-of-workers 2 \
    --worker-type "G.1X" \
    --command "Name=glueetl,ScriptLocation=s3://amazon-keyspaces-backups/scripts/S3toKeyspaces.scala" \
    --default-arguments '{
        "--job-language":"scala",
        "--KEYSPACE_NAME":"target_keyspace",
        "--TABLE_NAME":"target_table",
        "--S3_URI_FULL_CHANGE":"s3://$MIGRATION_BUCKET/full-dataset/",
        "--S3_URI_CURRENT_CHANGE":"s3://$MIGRATION_BUCKET/incremental-dataset/current/",
        "--S3_URI_NEW_CHANGE":"s3://$MIGRATION_BUCKET/incremental-dataset/new/",
        "--extra-jars":"s3://$MIGRATION_BUCKET/jars/spark-cassandra-connector-assembly_2.12-3.2.0.jar",
        "--extra-files":"s3://$MIGRATION_BUCKET/conf/KeyspacesConnector.conf",
        "--conf":"spark.cassandra.connection.config.profile.path=KeyspacesConnector.conf",
        "--class":"GlueApp"
    }'
```