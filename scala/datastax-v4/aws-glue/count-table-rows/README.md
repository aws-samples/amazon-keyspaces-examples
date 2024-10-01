## Using Glue Count and Distinct Count Example
This example provides scala script for counting number of rows in Amazon Keyspaces table data using AWS Glue. 

## Prerequisites
* Setup Spark Cassandra connector using provided [setup script](../)

### Setup Count rows
The following script sets up AWS Glue job to count rows for a Keyspaces table. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* COUNT_STACK_NAME is the stack name used to create count rows glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to count. 


```shell
./setup SETUP_STACK_NAME COUNT_STACK_NAME KEYSPACE_TABLE TABLE_NAME

```

By default the result of the count will be logged in Cloudwatch logs. You should see similar output to the following. 

```
    Total number of rows: 5191983
```

### Setup Distinct Count
The following script sets up AWS Glue job to count rows for a Keyspaces table. The script takes the following parameters 
* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* COUNT_STACK_NAME is the stack name used to create count rows glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to count. 
* DISTINCT_KEYS comma seperated list of keys to perform distinct count. Leave blank to count every row. 

```shell
./setup SETUP_STACK_NAME COUNT_STACK_NAME KEYSPACE_TABLE TABLE_NAME DISTINCT_KEYS

```

By default the result of the distinct count will be logged in Cloudwatch logs. You should see similar output to the following. 

```
    Total number of distinct rows: 41983
```
