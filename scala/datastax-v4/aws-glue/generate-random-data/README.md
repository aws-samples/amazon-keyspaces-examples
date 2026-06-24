## Generate Random Data

This example provides a Scala script for generating random data and importing it to an Amazon Keyspaces table using AWS Glue. Useful for prototyping and testing without an existing dataset.

### Prerequisites

* Run `./keyspaces-glue bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs
* Create the target table first

### Create Table

```cql
CREATE KEYSPACE IF NOT EXISTS aws WITH REPLICATION = {'class': 'SingleRegionStrategy'}

CREATE TABLE IF NOT EXISTS aws.my_table_example (
    "id" text,
    "create_date" timestamp,
    "data" text,
    "count" bigint,
    PRIMARY KEY("id", "create_date"))
WITH CUSTOM_PROPERTIES = {
    'capacity_mode':{
        'throughput_mode':'PROVISIONED',
        'write_capacity_units':30000,
        'read_capacity_units':30000
    },
    'point_in_time_recovery':{
        'status':'enabled'
    },
    'encryption_specification':{
        'encryption_type':'AWS_OWNED_KMS_KEY'
    }
} AND CLUSTERING ORDER BY("create_date" ASC)
```

### Running Generate

```bash
./keyspaces-glue generate \
  --keyspace aws \
  --table my_table_example
```

Override workers for faster generation:
```bash
./keyspaces-glue generate \
  --keyspace aws \
  --table my_table_example \
  --workers 10
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the target table | mykeyspace |
| --TABLE_NAME | Table to generate data into | mytable |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Notes

- The script generates 1,000,000 rows by default with random string keys and numeric values
- Data is written directly to Keyspaces (not to S3)
- Ensure your table has sufficient write capacity provisioned before running
