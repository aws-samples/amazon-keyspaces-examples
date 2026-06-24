## Top N Partitions

This example provides a Scala script for finding the partitions with the most rows. This is a common utility for verifying data skew. Works with Amazon Keyspaces or self-managed Apache Cassandra.

### Prerequisites

* Run `./keyspaces-glue bootstrap` from the [parent directory](../) to set up infrastructure and deploy all Glue jobs

### Running Top Partitions

```bash
./keyspaces-glue top-partitions \
  --keyspace mykeyspace \
  --table mytable \
  --group-by "user_id" \
  --max-results 10 \
  --min-size 100
```

With a where clause to analyze a subset:
```bash
./keyspaces-glue top-partitions \
  --keyspace mykeyspace \
  --table mytable \
  --group-by "user_id" \
  --max-results 20 \
  --min-size 50 \
  --where-clause "created_date >= '2025-01-01'"
```

Multi-column partition key:
```bash
./keyspaces-glue top-partitions \
  --keyspace mykeyspace \
  --table mytable \
  --group-by "tenant_id,region" \
  --max-results 10 \
  --min-size 1000
```

### Script Arguments

| Argument | Description | Default |
| :--- | :--- | :--- |
| --KEYSPACE_NAME | Keyspace containing the table | mykeyspace |
| --TABLE_NAME | Table to analyze | mytable |
| --GROUPBY | Comma-separated columns to group by | - |
| --MAX_RESULTS | Number of top partitions to return | 10 |
| --MIN_SIZE | Minimum row count to include in results | 1 |
| --WHERE_CLAUSE | Optional filter condition | (none) |
| --DRIVER_CONF | Driver configuration file | keyspaces-application.conf |

### Viewing Results

Results are logged to CloudWatch. Retrieve them with:

```bash
./keyspaces-glue logs top-partitions --log-type error
```

For a specific run:
```bash
./keyspaces-glue logs top-partitions --run-id jr_abc123 --log-type error
```

Example output:
```
user-1999,1999
user-7999,1999
user-5999,1999
user-3999,1999
user-9999,1998
```
