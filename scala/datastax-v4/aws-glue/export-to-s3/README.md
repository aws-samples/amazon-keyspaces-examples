## Using Glue Export Example
This example provides scala script for exporting Amazon Keyspaces table data to S3 using AWS Glue. This allows you to export data from Amazon Keyspaces without setting up a spark cluster.

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Export to S3
The following script sets up AWS Glue job to export a Keyspaces table to S3. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* EXPORT_STACK_NAME is the stack name used to create export glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to export.
* S3URI is the S3 uri where the data will be exported.  
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena.

```shell
./setup-export.sh SETUP_STACK_NAME EXPORT_STACK_NAME KEYSPACE_TABLE TABLE_NAME S3URI FORMAT

```

By default the export will copy data to S3 bucket specified in the parent stack in the format ```/export/keyspace/table/``` 
