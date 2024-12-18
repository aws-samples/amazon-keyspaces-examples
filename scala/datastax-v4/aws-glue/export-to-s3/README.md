## Using Glue Export Example
This example provides scala script for exporting Amazon Keyspaces table data to S3 using AWS Glue. This allows you to export data from Amazon Keyspaces without setting up a spark cluster.

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Export to S3
The following script sets up AWS Glue job to export a Keyspaces table to S3. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* EXPORT_STACK_NAME is the stack name used to create export glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to export.
* S3URI is the S3 uri where the data will be exported. By default it will use the s3 bucket from the parent stack. 
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena. default is parquet. 

```shell
./setup-export.sh SETUP_STACK_NAME EXPORT_STACK_NAME KEYSPACE_TABLE TABLE_NAME 

```

Optionally, you may also provide the S3URI, FORMAT. Later you can change parameters on job run.

```shell
./setup-export.sh SETUP_STACK_NAME EXPORT_STACK_NAME KEYSPACE_TABLE TABLE_NAME S3URI FORMAT

```


By default the export will copy data to S3 bucket specified in the parent stack in the format. You can override the S3 bucket at run time.  

```shell
    \--- S3_BUCKET
            \------- jars
            \------- conf
            \------- scripts
            \------- spark-logs
            \------- export
                \----- keyspace_name
                    \----- table_name
                       \----- snapshot
                           \----- year=2025
                               \----- month=01
                                  \----- day=02
                                      \----- hour=09
                                          \----- minute=22
                                              \--- YOUR DATA HERE

``` 

Running the job can be done through the AWS CLI. In the following example the command is running the job created in the previous step, but overrides the number of glue workers, worker type, and script arguments such as the table name. You can override any of the glue job parameters at run time. 

```shell
aws-glue % aws glue start-job-run --job-name AmazonKeyspacesExportToS3-shuffletest-shuffletest-export1 --number-of-workers 8 --worker-type G.2X --arguments '{"--TABLE_NAME":"transactions"}'
```

