## Modify Time to Live (TTL)

This example provides a Scala script for modifying TTL values across many or all rows in a table using AWS Glue.

### Prerequisites

* Run `./keyspaces-bulk-cli bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs
* TTL must be enabled on the Keyspaces table

### Running Modify TTL

Add 30 days to existing TTL:
```bash
./keyspaces-bulk-cli modify-ttl \
  --keyspace mykeyspace \
  --table mytable \
  --ttl-field value \
  --ttl-time-to-add 2592000
```

Subtract 1 year from TTL:
```bash
./keyspaces-bulk-cli modify-ttl \
  --keyspace mykeyspace \
  --table mytable \
  --ttl-field value \
  --ttl-time-to-add -31536000
```

Modify TTL only for a subset of rows:
```bash
./keyspaces-bulk-cli modify-ttl \
  --keyspace mykeyspace \
  --table mytable \
  --ttl-field value \
  --ttl-time-to-add 2592000 \
  --where-clause "status == 'active'"
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the table | mykeyspace |
| --TABLE_NAME | Table to modify | mytable |
| --TTL_FIELD | Column name used for existing TTL value | - |
| --TTL_TIME_TO_ADD | Seconds to add to existing TTL (negative to subtract) | - |
| --WHERE_CLAUSE | Optional filter condition | (none) |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### How It Works

The script reads each row's current TTL using the Spark Cassandra connector's `ttl()` function, calculates the new TTL by adding the specified time delta, then updates the row using a Lightweight Transaction (LWT) to avoid conflicts with concurrent writes.
