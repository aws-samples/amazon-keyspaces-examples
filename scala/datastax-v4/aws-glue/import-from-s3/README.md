## Using Glue Import Example
This example provides scala script for importing data to Amazon Keyspaces from S3 using AWS Glue. 

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Import to S3
The following script sets up AWS Glue job to import a Keyspaces table from S3. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* IMPORT_STACK_NAME is the stack name used to create import glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to import. 
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena.

```shell
./setup SETUP_STACK_NAME IMPORT_STACK_NAME KEYSPACE_TABLE TABLE_NAME FORMAT

```

By default the import will copy data from S3 bucket specified in the parent stack in the format ```/export/keyspace/table/snapshot/timestamp``` . 
