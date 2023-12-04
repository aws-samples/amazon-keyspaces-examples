## Using Glue Import Example
This example provides scala script for importing data to Amazon Keyspaces from S3 using AWS Glue. 

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Import to S3
The following script sets up AWS Glue job to import a Keyspaces table from S3. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* IMPORT_STACK_NAME is the stack name used to create import glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to import. 
* S3URI the S3 uri where the data to import is located.  
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena.

```shell
./setup-import.sh SETUP_STACK_NAME IMPORT_STACK_NAME KEYSPACE_TABLE TABLE_NAME S3URI FORMAT

```

## Performance tuning

When importing data with Glue there are few variables to take into consideration. First is time duration you need the Glue job to complete such as 1 hour, 2 hours, or 20 minutes.  Take this time period and determine the rate rows per second to finish the data load in that time. For instance, if you have 10 million rows to insert, and one hour to finish, you will need to execute at least ~2,778 rows per second.   Lastly, what resources from AWS Glue and Amazon Keyspaces are required to acheive this throughput. 

### Using Glue with Keyspaces Provisioned Mode 
With provisioned capacity mode, you specify the number of reads and writes per second that are required for your application. This helps you manage your Amazon Keyspaces usage to stay at or below a defined request rate to optimize price and maintain predictability. Achieving 10,000 write operations per second may result in many UserErrors if the Keyspaces table is currently provisioned for three hundred Write Units per second. To ensure a succesfull Glue job completion, you will need to ensure the Keyspaces table has the appropriate provisioned rate. You can manually configure provisioned throughput through the Amazon Keyspaces console, or through the aws cli. To achieve 10,000 writes units per second you would want to provision 10,000 write unites per second or more for the table. In our example above of 2,778 rows per second, we will need to calculate the row size to estimate the number of capacity units consumed per row. For every one KB of data, Keyspaces consumes 1 WCU. If the row is 4.5KB, it will require 5 Write Capacity Units. At 2,778 rows per second with 5 Write Capacity Units per row. The Keyspaces table will need to provide at least 13,890 Write Capacity Units per second. At target utilization of 90% will require 15,433 write capacity units per second. 

The command is using the aws cli to update capacity for the keyspaces table mykeyspace.mytable to 15,500 write request units. 

```shell
aws keyspaces update-table --keyspace-name mykeyspace --table-name mytable --capacity-specification "throughputMode=PROVISIONED,readCapacityUnits=10,writeCapacityUnits=15,500"
```


### Using Glue with Keyspaces Ondemand Mode 
When you choose on-demand mode, Amazon Keyspaces can scale the throughput capacity for your table up to any previously reached traffic level instantly, and then back down when application traffic decreases. If a workload’s traffic level hits a new peak, the service adapts rapidly to increase throughput capacity for your table. For new tables, Keyspaces does have have history of your workload and starts with the capcity of 4,000 Write units per second. If you plan to create a new table and perform 10,000 writes per second from Glue, we suggest creating the table in provisioned mode with 12,000 Write Units per second, then switching to ondemand capacity mode to ondemand mode. This tells Keyspaces to expect a higher initial capacity as a starting point. 


### Configuring Glue for the Spark Cassandra Connector 
When you create a Glue job, you define the number of workers and worker type used during execution time. Glue will designate one worker as the spark driver, a central coordinator which communicates with all the Workers.  For example, when deploying a Glue job with three workers, one worker will be the driver and the other workers will host the executors which process task. For each executer, an instance of the spark cassandra connector will be deployed and establish a cassandra session.  

Each Spark Cassandra connector concurrent writer threads will result in roughly 1000 1KB writes operations per second for G1 Worker. To achieve 3,000 writes per second, you can set spark.cassandra.output.concurrent.writes to 1 and deploy 3 G1 Workers(4 includin the driver) or you can set spark.cassandra.output.concurrent.writes to 3 and use 1 G1 Worker. As you increase spark.cassandra.output.concurrent.writes you may recieve more or less write operations per second. A good upper bound for the setting spark.cassandra.output.concurrent.writes is 10 for G1 workers. Its recommended to Start with spark.cassandra.output.concurrent.writes = 1 and 3 (1 driver 2 workers) Workers to get a benchmark of throughput for the data per worker.  

The below configuration is set in the spark conf of the Glue job script. 
```scala

val conf = new SparkConf()
      .setAll(
       Seq(("spark.cassandra.output.concurrent.writes", "1"),
        ...
        ...

```


```yaml
 NumberOfWorkers: 2
```



