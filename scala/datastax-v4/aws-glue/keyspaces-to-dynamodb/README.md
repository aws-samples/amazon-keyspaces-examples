## Using Glue for migration example
This example provides scala script for directly copying an Amazon Keyspaces table to DynamoDB. This allows you to migrate data to DynamoDB without setting up a spark cluster.

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup migration from Keyspaces to DynamoDB
The following script sets up AWS Glue job to directly copy the Keyspaces table to DynamoDB. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* MIGRATION_STACK_NAME is the stack name used to create migration glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to migrate.
* DYNAMODB_TABLE_NAME is the name of the DynamoDB table the data should be written to.
* DYNAMODB_WRITE_UNITS is the target write throughput for the job. This setting is agnostic to the capacity mode (provisioned on on-demand) of your DynamoDB table.

```shell
./setup-migration.sh SETUP_STACK_NAME MIGRATION_STACK_NAME KEYSPACE_TABLE TABLE_NAME DYNAMODB_TABLE_NAME DYNAMODB_WRITE_UNITS

```

By default this script will copy all rows and all columns into DynamoDB. You can choose any arbitrary throughput, but you must increase the requested number of Glue workers through trial and error to achieve your desired throughput in DynamoDB.