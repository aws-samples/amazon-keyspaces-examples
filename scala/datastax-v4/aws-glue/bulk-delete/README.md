## Glue Bulk Delete Example
This example provides scala script for bulk delete for data in Amazon Keyspaces using AWS Glue. This allows you to bulk delete or truncate a table from Amazon Keyspaces without setting up a spark cluster.

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Bulk delete
The following script sets up AWS Glue job to bulk delete from a Keyspaces table. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* DELETE_STACK_NAME is the stack name used to create glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to bulk delete from.
* S3URI is the S3 uri where the deleted records will be stored. By default it will use the s3 bucket from the parent stack. 
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena. default is parquet. 
* DISTINCT_KEYS comma seperated list of keys to perform deletes. If left blank, deletes will be made by primary key. If only some keys are specified, the job will perform range deletes. Range deletes in keyspaces can delete up to 1000 rows.
* QUERY_FILTER like query to apply a filter condition to delete statement. Leave blank to delete every row and truncate the table.

```shell
./setup-bulkd-delete.sh SETUP_STACK_NAME DELETE_STACK_NAME KEYSPACE_TABLE TABLE_NAME S3URI FORMAT DISTINCT_KEYS QUERY_FILTER 

```

 The job will copy data to S3 bucket if provided. You can override or remove the S3 bucket at run time.  The structure below is appended to the s3 bucket provided and is the final location of the data thats deleted. 

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

### Running the bulk job from the CLI

Running the job can be done through the AWS CLI. In the following example the command is running the job created in the previous step, but overrides the number of glue workers, worker type, and script arguments such as the table name. You can override any of the glue job parameters at run time and the default arguments. 

```shell
aws-glue % aws glue start-job-run --job-name AmazonKeyspacesBulkDelete-aksglue-aksglue-bulk-delete --number-of-workers 8 --worker-type G.2X --arguments '{"--TABLE_NAME":"transactions"}'
```

Full list of aws cli arguments [start-job-run arguments](https://docs.aws.amazon.com/cli/latest/reference/glue/start-job-run.html)

### List of script arguments

| argument          | defenition                                      | default             | required   |
| :---------------- | :---------------------------------------------- | :------------------ | :------    |
| --KEYSPACE_NAME   |   Name of the keyspace of the table to delete   | provided at setup        | Y |
| --TABLE_NAME      |   Name of the table to delete                   | provided at setup        | Y |
| --S3_URI          |  S3 URI where the root of the bulk delete data will be located. The folder structure is added dynamically in the scala script       | The default location is the s3 bucked provided when setting up the parent stack or the bulk-delete stack | parent stack s3 bucket | N |
| --FORMAT          |  THe format of the export. Its recommended to use parquet. You could alternativley use json or other types supported by spark s3 libraries | parquet | N |
| --DRIVER_CONF     |  the file containing the driver configuration.  | By default the parent stack sets up a config for Cassandra and config for keyspaces. You can add as many additional configurations as you like by dropping them in the same location in s3. | keyspaces-application.conf | Y |
| DISTINCT_KEYS     | comma seperated list of keys. If left blank the script will use the full primary key located in the system schema. If you provide a portion of the primary key a range delete will be used. Range delete in keyspaces can delete up to 1000 rows. | full primary key | N |
| QUERY_FILTER   | SQL like query to apply a filter condition to delete statement | no query filter | N |


### Scheduled Trigger (Cron) 
If you are building a frequent delete workload such as custom ttl or moving data to cold storage, you can setup a cron job using glue scheduled trigger.  Here is a simple AWS CLI command to create a Glue Trigger that runs your bulk delete Glue job once per week (every Monday at 12:00 UTC). The following script is used to delete rows which event data has passed current date epoch

```shell
  aws glue create-trigger \
  --name KeyspacesBulkDeleteWeeklyTrigger \
  --type SCHEDULED \
  --schedule "cron(0 12 ? * MON *)" \
  --start-on-creation \
  --actions '[{
     "JobName": "AmazonKeyspacesBulkDelete-bulk-delete",
     "WorkerType": "G.2X",
     "NumberOfWorkers": 8,
     "Arguments": {
       "--table_name": "transactions",
       "--keyspace_name": "aws",
       "--QUERY_FILTER": "event_date < '2024-05-17'"
     }
  }]'
  ```
