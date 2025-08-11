## Using Glue Modify TTL Example
This example provides scala script for modifying TTL across many or all rows in a table.

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Export to S3
The following script sets up AWS Glue job to modify ttl on a table. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* TLL_STACK_NAME is the stack name used to create glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to modify.
* TTL_FIELD the field used as existing TTL value
* TTL_TIME_TO_ADD the amount of time to add to the existing ttl value. 

```shell
./setup-modify-ttl.sh SETUP_STACK_NAME TLL_STACK_NAME KEYSPACE_TABLE TABLE_NAME TTL_FIELD TTL_TIME_TO_ADD 

```

### Running the script from the CLI

Running the job can be done through the AWS CLI. In the following example the command is running the job created in the previous step, but overrides the number of glue workers, worker type, and script arguments such as the table name. You can override any of the glue job parameters at run time and the default arguments. 

```shell
aws glue start-job-run --job-name AmazonKeyspacesModifyTTL-aksglue-aksglue-export --number-of-workers 8 --worker-type G.2X --arguments '{"--TABLE_NAME":"keyvalue"}'
```

Full list of aws cli arguments [start-job-run arguments](https://docs.aws.amazon.com/cli/latest/reference/glue/start-job-run.html)

### List of arguments

| argument          | defenition                                      | default |
| :---------------- | :---------------------------------------------- | ----: |
| --KEYSPACE_NAME   |   Name of the keyspace of the table to export   | none  |
| --TABLE_NAME      |   Name of the table to export                   | none  |
| --TTL_FIELD       |   Name of the field to use ttl value            | none  |
| --TTL_TIME_TO_ADD |   Amount of time to modify the existig ttl      | node  |


